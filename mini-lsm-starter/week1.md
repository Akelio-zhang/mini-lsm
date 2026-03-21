# Week 1: Storage Format & Engine Skeleton

本文记录 Week 1 各阶段的 LSM 核心原理、数据结构设计与实现细节。

---

## 整体架构

Week 1 构建了 LSM-Tree 的基础骨架，自底向上分为四个存储层：

```
写入路径：  put/delete
              ↓
         MemTable (当前可写)
              ↓ freeze
         imm_memtable[] (不可变，最新在前)
              ↓ flush
         L0 SSTs (未排序，最新在前)

读取路径（优先级从高到低）：
  MemTable → imm_memtables → L0 SSTs
```

**关键并发模型**：`LsmStorageState` 通过 copy-on-modify 更新：写操作持有 `state_lock (Mutex)`，克隆当前状态修改后原子替换 `Arc<RwLock<Arc<State>>>` 内的指针；读操作只需 RwLock 读锁，不会长时间阻塞写入。

---

## Day 1 — MemTable

### LSM 原理

LSM-Tree 的核心思想是**将随机写转化为顺序写**。所有写入首先进入内存中的 MemTable，积累到一定大小后冻结为 immutable memtable，再由后台线程顺序写入磁盘（SST 文件）。

MemTable 使用 **SkipList** 作为底层数据结构（`crossbeam_skiplist::SkipMap`），原因：
- O(log n) 的读写性能
- 天然有序，flush 时可直接按序输出
- 支持并发读写（lock-free）

**Tombstone（删除标记）**：LSM 中删除操作不会立即删除数据，而是写入一条值为空字节串的记录。读取时遇到空值即认为该 key 已删除。这避免了在多层数据中搜索并真正删除记录的开销，实际删除发生在后续的 Compaction 过程中。

### 数据结构

```
MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,  // key → value（删除时 value = ""）
    wal: Option<Wal>,                  // Week 2.6 引入
    id: usize,                         // SST ID（flush 时使用）
    approximate_size: AtomicUsize,     // 估算大小，用于触发 freeze
}
```

### 实现要点

**`put`**：写入 SkipMap 并累加 `approximate_size`。当 `approximate_size >= target_sst_size` 时触发 `try_freeze`，在持有 `state_lock` 后二次检查大小（double-checked locking），避免多线程重复 freeze。

**`scan`**：使用 `ouroboros` crate 实现自引用结构 `MemTableIterator`——SkipMap 的范围迭代器借用了 SkipMap 本身的生命周期，需要将二者封装在同一结构体中。迭代器初始化时立即 advance 到第一个条目，`is_valid()` 通过检查当前 key 是否为空判断。

**`force_freeze_memtable`**：创建新 MemTable，将旧 MemTable 推入 `imm_memtables` 队列头部（index 0 = 最新），完成原子状态替换。

---

## Day 2 — Merge Iterator

### LSM 原理

LSM-Tree 的**读放大问题**：一个 key 可能同时存在于 MemTable、多个 immutable memtable 和多个 SST 中。扫描时需要合并这些有序序列，取相同 key 中最新版本（优先级最高的来源）的值。

这是经典的 **K 路归并（K-way Merge）** 问题，用最小堆实现 O(n log k) 的合并效率。

### MergeIterator

```
MergeIterator<I> {
    iters: BinaryHeap<HeapWrapper<I>>,  // 堆中存放非当前的迭代器
    current: Option<HeapWrapper<I>>,    // 当前最小 key 的迭代器
}

HeapWrapper(idx, Box<I>)
// 排序规则：key 升序，相同 key 时 idx 小的优先（idx 小 = 更新的来源）
```

**`next()` 的关键逻辑**：
1. 跳过堆中所有与 current key 相同的条目（去重，保留最高优先级版本）
2. 推进 current 迭代器
3. 将 current 重新压入堆，弹出新的最小值

### TwoMergeIterator

用于合并两类不同类型的迭代器（如 `MergeIterator<MemTableIterator>` 和 `MergeIterator<SsTableIterator>`），始终优先选择 A（内存侧）。当 A 和 B 的 key 相同时，推进 B 跳过重复。

### LsmIterator + FusedIterator

**LsmIterator** 在合并结果之上过滤 tombstone（空值），并持有 `end_bound` 实现上界截断——SST 迭代器在创建时只设定下界（seek），上界必须在顶层过滤，否则会返回超出范围的条目。

**FusedIterator** 保证迭代器出错后始终返回错误，不会在 `is_valid() == false` 后意外继续迭代。

---

## Day 3 — Block

### LSM 原理

**Block** 是 SST 文件的最小读取和缓存单元（默认 4KB）。将大文件切分为固定大小的 Block 有两个好处：
1. **缓存粒度**：Block Cache 以 Block 为单位缓存，避免读取整个 SST 文件
2. **索引加速**：每个 Block 记录首尾 key，可用二分查找快速定位目标 Block

### Block 编码格式

```
┌──────────────────────────────────────────────┐
│  Entry 0: overlap(2B) rest_len(2B) rest val  │
│  Entry 1: overlap(2B) rest_len(2B) rest val  │  ← data section
│  ...                                          │
├──────────────────────────────────────────────┤
│  offset_0(2B)  offset_1(2B)  ...             │  ← offsets section
│  num_elements(2B)                             │
└──────────────────────────────────────────────┘
```

**前缀压缩（Prefix Key Encoding，Day 7 引入）**：每条 entry 存储相对于该 Block 第一个 key 的公共前缀长度（`overlap_len`）和剩余部分（`rest`），而非完整 key。对于具有相同前缀的 key（如 `user_0001`, `user_0002`），可显著减少存储空间，允许每个 Block 容纳更多条目。

### BlockIterator

迭代器维护 `first_key` 字段，解码时：`full_key = first_key[..overlap] + rest`。

`seek_to_key` 在引入前缀压缩后改为线性扫描（从第一个条目开始），因为前缀压缩使得无法在任意 offset 处独立解码 key（需要依赖 first_key）。对于块内通常只有几十到几百个条目的情况，线性扫描的开销可以接受。

---

## Day 4 — Sorted String Table (SST)

### LSM 原理

**SST（Sorted String Table）** 是 LSM-Tree 的磁盘存储格式。SST 内所有 key 有序排列，由若干 Block 组成。SST 是**不可变的**——一旦写入磁盘就不再修改，只会在 Compaction 时被合并生成新 SST 并删除旧 SST。

### SST 文件格式（Week 1 Day 7 版本）

```
┌─────────────────────────────────────────────────┐
│  Block 0 data                                   │
│  Block 1 data                                   │  ← data section
│  ...                                            │
├─────────────────────────────────────────────────┤
│  BlockMeta[]: offset(4B) fk_len(2B) fk lk      │  ← meta section
├─────────────────────────────────────────────────┤
│  Bloom filter data                              │  ← bloom section
├─────────────────────────────────────────────────┤
│  meta_offset (4B)                               │
│  bloom_offset (4B)                              │  ← trailer (8B)
└─────────────────────────────────────────────────┘
```

**BlockMeta**：记录每个 Block 的 `offset`、`first_key`、`last_key`，存储在 SST 尾部。Open 时全部加载进内存，用于：
- `find_block_idx(key)`：二分查找最后一个 `first_key ≤ key` 的 Block
- Range filter：判断查询范围是否与 SST 有交集

### Block Cache

使用 `moka::sync::Cache<(sst_id, block_idx), Arc<Block>>` 实现 LRU 缓存。`read_block_cached` 先查缓存，未命中时从文件读取并插入缓存，避免重复 I/O。

### SsTableIterator

持有 `blk_idx`（当前 Block 索引）和 `blk_iter`（当前 Block 的迭代器）。`next()` 在当前 Block 迭代器耗尽时自动推进到下一个 Block。`seek_to_key` 通过 `find_block_idx` 定位到候选 Block 后再在 Block 内 seek。

---

## Day 5 — Read Path

### LSM 原理

完整的读路径需要按优先级顺序搜索所有层级：
1. MemTable（最新）
2. Immutable MemTables（从新到旧）
3. L0 SSTs（从新到旧，可能有 key 重叠）
4. L1+ 层级（Week 2 实现，每层内 key 不重叠）

**点查（get）**：找到第一个包含目标 key 的位置即可返回，无需扫描所有层。对 L0 SSTs 的查询：利用 `first_key/last_key` 跳过不可能包含目标 key 的 SST，再用 `SsTableIterator::seek_to_key` 定位。

**范围扫描（scan）**：

```
FusedIterator<LsmIterator>
  └── LsmIterator (tombstone filter + upper bound check)
        └── TwoMergeIterator
              ├── MergeIterator<MemTableIterator>  (memtable + imm_memtables)
              └── MergeIterator<SsTableIterator>   (L0 SSTs)
```

### SST Range Filter

在 `scan` 中构建 L0 SST 迭代器之前，通过比较 SST 的 `[first_key, last_key]` 与查询范围 `[lower, upper]` 来过滤不相关的 SST：

```
跳过 SST 的条件（二者满足其一即可跳过）：
- upper = Included(k): sst.first_key > k
- upper = Excluded(k): sst.first_key >= k   ← 注意严格大于等于
- lower = Included(k): sst.last_key < k
- lower = Excluded(k): sst.last_key <= k    ← 注意严格小于等于
```

这使得 `num_active_iterators()` 随查询范围缩小而减少，是性能优化的基础。

---

## Day 6 — Write Path

### LSM 原理

写路径的核心是 **MemTable → L0 SST 的 flush 流程**：

```
put/delete
  → MemTable.put()
  → [size >= target] → try_freeze → force_freeze_memtable
                                     (imm_memtables.insert(0, old_mem))
  → 后台 flush 线程 (每 50ms 检查)
  → [imm_memtables.len() >= num_memtable_limit] → force_flush_next_imm_memtable
```

**flush 策略**：`imm_memtables` 是有序队列，index 0 最新、末尾最旧。flush 总是取末尾（最旧）的 memtable 写入磁盘，保证数据顺序性。新生成的 L0 SST 插入 `l0_sstables` 的 index 0（最新在前），与 memtable 的优先级顺序一致。

### `force_flush_next_imm_memtable`

```
1. 持有 state_lock（防止并发 flush）
2. 取 imm_memtables.last()（最旧的 imm memtable）
3. 遍历 SkipMap，逐 entry 写入 SsTableBuilder
4. build() → 写入 {id}.sst 文件
5. 原子更新状态：imm_memtables.pop() + l0_sstables.insert(0, sst_id)
6. sync_dir() → fsync 目录（保证文件元数据持久化）
```

### num_active_iterators

通过递归统计实现，用于监控查询的实际 I/O 开销：

```
FusedIterator → LsmIterator → TwoMergeIterator
                                ├── MergeIterator (heap中所有迭代器数量之和)
                                └── MergeIterator
```

---

## Day 7 — SST Optimizations

### Bloom Filter

**Bloom Filter** 是一种概率型数据结构，用于快速判断一个元素**肯定不在**集合中（可能误判为"存在"，但不会误判为"不存在"）。

在 LSM 中，Bloom Filter 附加在每个 SST 文件上，`get` 操作在访问 SST 之前先查 Bloom Filter，如果 Filter 报告 key 不存在，则跳过整个 SST 的读取，大幅减少 I/O。

**实现**（LevelDB/RocksDB 算法）：
```
k 个哈希函数（通过旋转实现）：
  h0 = h
  delta = h.rotate_left(15)
  h1 = h0 + delta, h2 = h1 + delta, ...

Build: 对每个 key hash，设置 k 个 bit
May_contain: 检查 k 个 bit 是否全部为 1
```

误判率 ≈ 1% 时的 bits_per_key ≈ 10 bits。

**文件格式变更**：原 trailer 为 4B（meta_offset），Day 7 扩展为 8B（meta_offset + bloom_offset）。需同时更新 `SsTableBuilder::build` 和 `SsTable::open`。

### 前缀 Key 压缩

在 Block 内，相邻 key 通常共享较长的公共前缀。Day 7 改用相对于**块内第一个 key** 的前缀压缩编码：

```
原始编码：| key_len(2B) | key(N B) | val_len(2B) | val |
压缩编码：| overlap(2B) | rest_len(2B) | rest(M B) | val_len(2B) | val |
          其中 N = overlap + M，M < N
```

对于 `key_0000000005`, `key_0000000010`, `key_0000000015`（前缀相同），压缩后每个 entry 节省约 10 字节，使同等 block_size 下能存放更多 entry，block 数量减少约 30%。

100 个键的测试中块数量从 ~36 降至 ≤ 25。

---

## Week 1 各模块文件对照

| 模块 | 文件 | 职责 |
|------|------|------|
| Block | `src/block.rs`, `src/block/builder.rs`, `src/block/iterator.rs` | 最小 I/O 单元，前缀压缩编码/解码 |
| SST | `src/table.rs`, `src/table/builder.rs`, `src/table/iterator.rs`, `src/table/bloom.rs` | 磁盘文件格式、Block 缓存、Bloom Filter |
| MemTable | `src/mem_table.rs` | 内存写缓冲，SkipList，自引用迭代器 |
| Iterators | `src/iterators/merge_iterator.rs`, `src/iterators/two_merge_iterator.rs` | K 路归并，双路归并 |
| LSM Iterator | `src/lsm_iterator.rs` | tombstone 过滤，上界截断，FusedIterator |
| Storage | `src/lsm_storage.rs` | 状态管理，读写路径，flush 触发 |
| Compact | `src/compact.rs` | flush 后台线程（Week 2 实现 compaction） |

---

## Week 1 测试覆盖

```
Day 1: test_task1_memtable_get/overwrite, test_task2~4_storage_integration  (6 tests)
Day 2: test_task1_memtable_iter, test_task2_merge_*, test_task3_fused, task4_integration  (8 tests)
Day 3: test_block_build_*, test_block_encode/decode/iterator/seek_key  (9 tests)
Day 4: test_sst_build_*, test_sst_decode/iterator/seek_key  (6 tests)
Day 5: test_task1_merge_1~5, test_task2_storage_scan, test_task3_storage_get  (7 tests)
Day 6: test_task1_storage_scan/get, test_task2_auto_flush, test_task3_sst_filter  (4 tests)
Day 7: test_task1_bloom_filter, test_task2_sst_decode, test_task3_block_key_compression  (3 tests)

合计：43 tests，全部通过 ✓
```
