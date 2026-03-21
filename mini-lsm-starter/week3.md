# Week 3: MVCC（多版本并发控制）

本文记录 Week 3 各阶段的核心原理、数据结构设计与实现细节。

---

## 总体架构

Week 3 在 Week 2 的基础上为 LSM-Tree 引入 **MVCC（Multi-Version Concurrency Control）**，通过在 key 中追加时间戳，实现快照读、事务隔离和 GC 压实。主要变更分为三大块：

1. **Key 时间戳化**：key 从纯字节变为 `(key_bytes, ts)`，排序语义改为 `key_bytes` 升序、`ts` 降序（最新版本排前）
2. **事务 API**：`new_txn()` 返回 `Arc<Transaction>`，支持局部写缓冲、快照读和可序列化冲突检测（OCC）
3. **GC 压实**：Compaction 按 watermark 清理过期版本，并支持前缀压实过滤器

```
写入路径（有 ts）：
  put/delete → write_batch_inner → memtable.put(KeySlice{key, ts}, value)
                                   ts = latest_commit_ts + 1（原子递增）

读取路径（快照读）：
  get_with_ts(key, read_ts) → LsmIterator{read_ts}
  scan_with_ts(lower, upper, read_ts) → LsmIterator{read_ts}
  → LsmIterator::move_to_key() 跳过 ts > read_ts 的版本和 tombstone

事务路径：
  new_txn() → Transaction{read_ts = latest_commit_ts, local_storage}
  txn.put/delete → local_storage（SkipMap<Bytes, Bytes>）
  txn.get/scan → local_storage 优先，fallback 到 get_with_ts/scan_with_ts
  txn.commit() → write_batch_inner（可选：OCC 冲突检测）
```

---

## Day 3.1：Key 时间戳重构

### Key 数据结构

`Key<T>` 从单字段 `(T,)` 扩展为双字段 `(T, u64)`，ts 为第二分量：

```rust
pub struct Key<T: AsRef<[u8]>>(T, u64);

// 重要常量
pub const TS_DEFAULT: u64 = 0;      // 无 MVCC 时使用
pub const TS_MAX: u64 = u64::MAX;   // 用于比较上界
pub const TS_RANGE_BEGIN: u64 = u64::MAX;  // seek 时找最新版本
pub const TS_RANGE_END: u64 = 0;           // 范围扫描上界
```

**排序语义**（`PartialOrd`/`Ord`）：
- `key_bytes` 升序（字典序）
- 相同 `key_bytes` 时，`ts` 降序（即 `Reverse(ts)`，最新版本排前）

这保证了对 `(key, TS_RANGE_BEGIN)` 的 seek 等价于"找该 key 的最新版本"。

### Block 编码变更

每条 entry 在 key 字节之后追加 8 字节 ts：

```
原始（Week 1/2）:
  | overlap(2B) | rest_len(2B) | rest_bytes | val_len(2B) | val |

Week 3:
  | overlap(2B) | rest_len(2B) | rest_bytes | ts(8B) | val_len(2B) | val |
```

- `compute_overlap()`：仅比较 `key_ref()`（不含 ts），overlap 只针对 key 字节
- `add()` 中大小估算：加 `sizeof::<u64>()` 表示 ts 字节
- `seek_to_key()` 改回**二分查找**：由于 ts 独立存储，不参与前缀压缩，二分查找再次可行

### SST 元数据变更

`BlockMeta` 中的 `first_key`/`last_key` 包含 ts，编码格式在末尾追加 `max_ts`：

```
BlockMeta 序列化:
  [per block: offset(4B) fk_len(2B) fk_bytes ts(8B) lk_len(2B) lk_bytes ts(8B)]
  ...
  max_ts(8B)   ← 整个 SST 中最大的 ts，用于恢复时重建 last_commit_ts
  checksum(4B)
```

`SsTable` 新增 `max_ts: u64` 字段；`SsTableBuilder` 构建时追踪 `max_ts = max(all key ts)`。

### MemTable 类型变更

`SkipMap<Bytes, Bytes>` → `SkipMap<KeyBytes, Bytes>`：

- key 类型从裸字节升级为带 ts 的 `KeyBytes`
- `get(KeySlice)` / `scan(Bound<KeySlice>, Bound<KeySlice>)` 的入参类型随之变化
- 引入 `map_key_bound_plus_ts(lower, upper, ts)` 辅助函数：
  - `Included(x)` → `Included(KeySlice{x, TS_RANGE_BEGIN})`（从最新版本开始）
  - `Excluded(x)` → `Excluded(KeySlice{x, TS_RANGE_END})`（排除该 key 所有版本）
  - 上界同理，`Included(x)` → `Included(KeySlice{x, TS_RANGE_END})`

### WAL 格式变更（批量写）

WAL 从逐条记录改为批量记录格式，同时引入 ts：

```
每个 batch:
  | batch_size(4B) | [key_len(2B) key ts(8B) val_len(2B) val]... | crc32(4B) |
```

- `put_batch(data: &[(KeySlice, &[u8])])`: 一次性写入整批，crc32 覆盖整个 body
- `recover()`: 读 batch_size → 解析每个 entry（含 ts） → 验证 crc32 → 插入 SkipMap

---

## Day 3.2：SST 分裂边界（Key-Boundary Splitting）

### 问题

Week 2 的 Compaction 在 `estimated_size >= target_sst_size` 时立即分裂 SST。引入多版本后，同一个 user key 可能有多个 `(key, ts)` 版本，若在版本之间分裂，后续 `SstConcatIterator` 无法保证"同一 user key 的所有版本在同一 SST 中"，破坏查询正确性。

### 解决方案

`compact_generate_sst_from_iter` 引入 `last_key: Vec<u8>` 追踪上一个条目的 user key。分裂条件改为：

```rust
let should_split = b.estimated_size() >= self.options.target_sst_size
    && (!iter.is_valid() || iter.key().key_ref() != last_key.as_slice());
```

即：只在**下一个条目属于不同 user key**（或迭代器已耗尽）时才分裂，确保同一 key 的所有版本落入同一 SST。

---

## Day 3.3：快照读（Snapshot Reads）

### LsmIterator MVCC 改造

新增两个字段：

```rust
pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    read_ts: u64,    // 快照时间戳
    prev_key: Vec<u8>,  // 上一个已emit的user key（跳过同key旧版本）
}
```

`move_to_key()` 核心循环：

```
loop:
  1. 跳过 ts > read_ts 的版本（对当前快照太新）
  2. 检查是否超出 end_bound，超出则停止
  3. 如果 key_ref == prev_key（已经emit过这个user key），跳过（旧版本）
  4. 如果 value 为空（tombstone）：记录 prev_key，跳过，继续循环
  5. 找到有效条目，break
```

`next()` 在推进内部迭代器前，先将当前 key 记入 `prev_key`，再调用 `move_to_key()`，防止同 user key 的旧版本被 emit。

### write_batch_inner

所有写入统一通过此方法，获得唯一且单调递增的时间戳：

```rust
pub(crate) fn write_batch_inner<T: AsRef<[u8]>>(
    &self, batch: &[WriteBatchRecord<T>]
) -> Result<u64> {
    let _write_lock = self.mvcc().write_lock.lock();   // 串行化写入
    let ts = {
        let mut guard = self.mvcc().ts.lock();
        let ts = guard.0 + 1;
        guard.0 = ts;
        ts
    };
    // 将 batch 中每条记录写入 memtable，key 携带 ts
    ...
    self.try_freeze(size)?;
    Ok(ts)
}
```

`write_batch()` / `put()` / `delete()` 统一委托给 `write_batch_inner()`。

### get_with_ts / scan_with_ts

```rust
// get：构建全层迭代器，创建 LsmIterator{read_ts}，检查第一个结果是否匹配
pub fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>>

// scan：构建全层迭代器，返回 LsmIterator{read_ts}
pub(crate) fn scan_with_ts(
    &self, lower: Bound<&[u8]>, upper: Bound<&[u8]>, read_ts: u64
) -> Result<LsmIterator>
```

`get()` 和 `scan()` 改为用 `mvcc().latest_commit_ts()` 作为 `read_ts` 调用上述方法，保证外部 API 的语义是"读当前最新快照"。

---

## Day 3.4：Watermark 与 GC 压实

### Watermark（水位线）

```rust
pub struct Watermark {
    readers: BTreeMap<u64, usize>,  // ts → 引用计数
}
```

- `add_reader(ts)`: 引用计数 +1
- `remove_reader(ts)`: 引用计数 -1，归零则移除
- `watermark()`: 返回 `readers` 中最小的 ts（`keys().next()`），即最老活跃事务的读时间戳
- `num_retained_snapshots()`: 返回 `readers.len()`（不同 ts 的数量，不是总引用数）

`LsmMvccInner::watermark()` 在没有活跃读者时返回 `latest_commit_ts`（所有版本均可 GC）。

### Compaction GC 逻辑

`compact_generate_sst_from_iter` 按以下规则决定是否保留每个条目：

```
watermark = self.mvcc().watermark()

对每个 entry (key_bytes, ts, value):
  same_key = (key_bytes == last_key)
  if !same_key: last_key = key_bytes; first_key_below_watermark = true

  if ts <= watermark:
    if first_key_below_watermark:
      first_key_below_watermark = false
      if compact_to_bottom_level && value.is_empty():
        跳过（tombstone GC：底层不需要保留删除标记）
      elif 匹配 compaction filter:
        跳过（前缀过滤器 GC）
      else:
        保留（watermark 以下的第一个版本，即该 key 的"基准版本"）
    else:
      跳过（watermark 以下的旧版本，已被第一个版本覆盖）
  else (ts > watermark):
    无条件保留（活跃快照可能需要这些版本）
```

**关键语义**：watermark 以下只保留每个 user key 的最新一个版本（即 `first_key_below_watermark` 对应的版本），其余旧版本被 GC 丢弃。这保证了所有 `read_ts >= watermark` 的快照仍能读到正确数据。

---

## Day 3.5：事务（Transaction）

### Transaction 结构

```rust
pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,  // 本地写缓冲
    pub(crate) committed: Arc<AtomicBool>,
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
    // None = 非可序列化; Some((read_set, write_set)) = 可序列化（OCC）
}
```

### new_txn()

```rust
// LsmMvccInner::new_txn()
let read_ts = ts_guard.0;          // 当前 latest_commit_ts
ts_guard.1.add_reader(read_ts);    // 注册到 watermark
Arc::new(Transaction { read_ts, inner, local_storage: SkipMap::new(), ... })
```

事务创建时快照 `read_ts`，并向 watermark 注册，阻止 GC 清理 `<= read_ts` 的版本。

### Transaction::Drop

```rust
impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}
```

事务销毁时从 watermark 注销，允许 GC 推进。

### Transaction::get()

```
1. 若可序列化：将 key hash 加入 read_set
2. 先查 local_storage（本地写覆盖）
   - 找到空值 → 返回 None（本地 tombstone）
   - 找到非空值 → 返回该值
3. fallback 到 inner.get_with_ts(key, self.read_ts)
```

### Transaction::scan()

创建 `TxnLocalIterator`（遍历 `local_storage` 中指定范围的条目）和 `FusedIterator<LsmIterator>`（scan_with_ts），用 `TwoMergeIterator` 合并后封装为 `TxnIterator`：

```
TxnIterator
  └── TwoMergeIterator
        ├── TxnLocalIterator  (本地写缓冲，优先级更高)
        └── FusedIterator<LsmIterator>  (LSM 快照，read_ts 过滤)
```

`TxnIterator` 在 create 和 next 中跳过 tombstone（value 为空的条目），并在可序列化模式下将每个 emit 的 key hash 加入 read_set。

### TxnLocalIterator

使用 `ouroboros::self_referencing` 实现自引用结构，模式与 `MemTableIterator` 相同：

```rust
#[self_referencing]
pub struct TxnLocalIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),  // 当前 key-value
}
```

`key()` 返回 `&self.borrow_item().0[..]`，`is_valid()` 通过 `!item.0.is_empty()` 判断。

### Transaction::commit()

```
1. committed.store(true)（防止重复提交或继续写入）
2. 若可序列化：获取 commit_lock（串行化所有可序列化提交）
3. 若可序列化：OCC 冲突检测
   - 遍历 committed_txns[read_ts+1..]
   - 若任意已提交事务的 write_set 与本事务 read_set 有交集 → 返回 Err
4. 将 local_storage 转换为 WriteBatchRecord，调用 write_batch_inner
5. 若可序列化：将本事务的 write_set 记录到 committed_txns[commit_ts]
   并清理 watermark 以下的过期 committed_txns 记录
```

---

## Day 3.6：可序列化快照隔离（OCC/SSI）

### 原理

OCC（Optimistic Concurrency Control）在事务提交时检测冲突，而非在读取时加锁。Mini-LSM 实现的是 **Read-Write 冲突检测**：

> 若事务 A 在事务 B 开始后提交，且 A 的 write_set 与 B 的 read_set 有交集（即 B 读了 A 写的 key），则 B 提交失败。

### 数据结构

```rust
pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,  // write_set（key 的 fingerprint32）
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,      // 串行化 write_batch_inner
    pub(crate) commit_lock: Mutex<()>,     // 串行化可序列化事务提交
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,  // (latest_commit_ts, watermark)
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}
```

### 冲突检测

`commit()` 持有 `commit_lock` 期间：

```rust
for (_, data) in committed_txns.range((self.read_ts + 1)..) {
    for &hash in &data.key_hashes {
        if read_set.contains(&hash) {
            return Err(anyhow::anyhow!("transaction conflict"));
        }
    }
}
```

只检查在本事务 `read_ts` 之后提交的事务（`read_ts + 1` 起），因为之前的提交已反映在快照中，不构成冲突。

### 锁的分工

| 锁 | 保护对象 | 持有时机 |
|----|---------|--------|
| `write_lock` | `latest_commit_ts` 的原子递增 + memtable 写入 | `write_batch_inner` 整个过程 |
| `commit_lock` | `committed_txns` 的读写一致性 | 可序列化事务的冲突检测到 committed_txns 记录插入之间 |

非可序列化事务不获取 `commit_lock`，多个非可序列化写可通过 `write_lock` 串行，性能更高。

### Read Set 追踪

- `txn.get(key)`: `read_set.insert(farmhash::fingerprint32(key))`
- `TxnIterator::create/next` 在每次推进到新 key 时：`read_set.insert(farmhash::fingerprint32(iter.key()))`

---

## Day 3.7：Compaction Filters（压实过滤器）

### 接口

```rust
pub enum CompactionFilter {
    Prefix(Bytes),  // 过滤掉所有 key 以此前缀开头的条目
}

// 动态添加过滤器
storage.add_compaction_filter(CompactionFilter::Prefix(Bytes::from("table2_")));
```

### 触发条件

过滤器**仅在 watermark 以下**生效，即 `ts <= watermark` 且 `first_key_below_watermark == true`（该 user key 的第一个存留版本）时，才检查过滤器：

```rust
if compact_to_bottom_level && value.is_empty() {
    false   // tombstone GC 优先
} else {
    !compaction_filters.iter().any(|f| match f {
        CompactionFilter::Prefix(prefix) => key_bytes.starts_with(prefix.as_ref()),
    })
}
```

**语义**：仅在该版本"可被安全 GC"时（watermark 以下）才应用过滤器。watermark 以上的版本（活跃快照可能读取）不受过滤器影响，保证正在进行的事务不会读到被误删的数据。

---

## MiniLsm 公共 API 变更

| 方法 | Week 2 | Week 3 |
|------|--------|--------|
| `new_txn()` | `Result<()>` | `Result<Arc<Transaction>>` |
| `scan()` | `Result<FusedIterator<LsmIterator>>` | `Result<TxnIterator>` |
| `get()` | 直接查各层 | 委托 `get_with_ts(latest_commit_ts)` |
| `put/delete/write_batch` | `TS_DEFAULT` | 通过 `write_batch_inner` 分配递增 ts |

`MiniLsm::scan()` 内部创建一个非可序列化的临时事务（`read_ts = latest_commit_ts`），调用 `txn.scan()` 返回 `TxnIterator`。这使得 `scan` 的返回类型统一为 `TxnIterator`，同时复用事务的 local + LSM 两路归并逻辑。

---

## 关键实现细节

### Key 排序与 seek

```
(key_bytes 升序, ts 降序)

seek_to(key, TS_RANGE_BEGIN=MAX):
  → 定位到该 user key 最新版本（ts 最大）
seek_to(key, TS_RANGE_END=0):
  → 用于上界排除：(key, 0) 是该 user key 所有版本中的最后一个
```

### Excluded 边界处理

对于 `Bound::Excluded(key)` 的扫描下界：在构建各层迭代器时 seek 到 `(key, TS_RANGE_BEGIN)` 后，跳过所有 `key_ref() == key` 的条目（跳过该 key 全部版本）；`LsmIterator` 的 `prev_key` 机制也不会重复 emit。

对于 `map_key_bound_plus_ts` 的 Excluded 下界：转换为 `Excluded(KeySlice{key, TS_RANGE_END})`，因为 `(key, 0)` 排在该 key 所有版本的最后，`Excluded` 此值即排除整个 key。

### SST 分裂只在 key 边界

同一 user key 的不同版本（ts 不同）必须落入同一 SST，保证 `SstConcatIterator` 可以按 user key 范围查找而不遗漏版本。分裂检查在 `iter.next()` 之后，仅当下一个条目的 `key_ref != last_key` 时才触发。

### OCC 与 watermark 协同

`committed_txns` 中过期的记录（`commit_ts < watermark`）在每次 commit 时被清理，防止 committed_txns 无限增长。因为 watermark 以下的事务已经结束，不可能再有新的 `read_ts <= watermark` 的事务需要检测冲突。

---

## Week 3 各模块文件对照

| 模块 | 文件 | 职责 |
|------|------|------|
| Key | `src/key.rs` | `Key<T>(T, u64)`，ts 语义，排序规则，常量定义 |
| Block | `src/block/builder.rs`, `src/block/iterator.rs` | ts 编解码，二分查找恢复 |
| SST | `src/table.rs`, `src/table/builder.rs` | BlockMeta ts 编解码，max_ts 追踪 |
| MemTable | `src/mem_table.rs` | `SkipMap<KeyBytes, Bytes>`，ts 感知 scan/get |
| WAL | `src/wal.rs` | 批量写格式，ts 序列化/反序列化 |
| LSM Iterator | `src/lsm_iterator.rs` | `read_ts`，`prev_key`，`move_to_key()` 快照语义 |
| Storage | `src/lsm_storage.rs` | `write_batch_inner`，`get/scan_with_ts`，`new_txn` |
| Compact | `src/compact.rs` | Key 边界分裂，watermark GC，compaction filters |
| MVCC | `src/mvcc.rs` | `LsmMvccInner`，`new_txn()`，`watermark()` |
| Watermark | `src/mvcc/watermark.rs` | `BTreeMap<ts, refcount>`，最小活跃 ts |
| Transaction | `src/mvcc/txn.rs` | `Transaction`，`TxnLocalIterator`，`TxnIterator`，OCC |

---

## Week 3 测试覆盖

```
Day 3.1: test_sst_build_multi_version_simple/hard  (2 tests)
Day 3.2: test_task3_compaction_integration  (1 test)  — key 边界分裂验证
Day 3.3: test_task2_memtable_mvcc, test_task2_lsm_iterator_mvcc, test_task3_sst_ts  (3 tests)
Day 3.4: test_task1_watermark, test_task2_snapshot_watermark, test_task3_mvcc_compaction  (3 tests)
Day 3.5: test_txn_integration  (1 test)
Day 3.6: test_serializable_1~5  (5 tests)
Day 3.7: test_task3_mvcc_compaction  (1 test)  — compaction filter + watermark GC

合计：16 tests，全部通过 ✓

回归（Week 1 + Week 2）：56 tests，全部通过 ✓
```
