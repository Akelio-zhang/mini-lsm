# Week 2: LSM 压实策略与持久化

## 总体架构

Week 2 在 Week 1 的基础上，实现了 LSM-Tree 的三大核心功能：
1. **Compaction（压实）**：将多个 SST 合并成更大的有序文件，减少读放大
2. **Manifest（清单文件）**：持久化存储引擎状态，支持崩溃恢复
3. **WAL（Write-Ahead Log）**：防止 memtable 中未刷盘数据的丢失

---

## Day 2.1：Compaction 实现

### SstConcatIterator（有序拼接迭代器）

L1+ 层的 SST 文件不重叠、按 key 顺序排列。`SstConcatIterator` 利用这一性质：

- **按需创建迭代器**：初始化时只打开第一个 SST，用完再打开下一个
- **seek_to_key**：用 `partition_point` 二分找到第一个 `last_key >= key` 的 SST，然后在该 SST 内 seek
- `num_active_iterators()` 固定返回 1，体现 concat 迭代器的合并语义

### compact() 核心逻辑

```
ForceFullCompaction:
  L0: MergeIterator<SsTableIterator>（L0 有重叠）
  L1: SstConcatIterator（L1 不重叠）
  → TwoMergeIterator::create(l0_merge, l1_concat)

Simple/Leveled（upper_level=None，即 L0→L1）:
  upper: MergeIterator（L0 SSTs）
  lower: SstConcatIterator
  → TwoMergeIterator

Simple/Leveled（upper_level=Some，即 L1→L2）:
  upper: SstConcatIterator
  lower: SstConcatIterator
  → TwoMergeIterator

Tiered:
  每个 tier: SstConcatIterator
  → MergeIterator<SstConcatIterator>（多路归并）
```

### compact_generate_sst_from_iter（合并生成 SST）

- 遍历合并迭代器，按序写入新的 `SsTableBuilder`
- **tombstone 处理**：只在 `compact_to_bottom_level=true` 时跳过空值（删除标记）；中间层保留 tombstone
- 每当 `estimated_size >= target_sst_size` 时，完成当前 SST，开始新的

### force_full_compaction()

- 读取快照中的 L0 和 L1 SST ID
- 调用 `compact()`，获得新 SST 列表
- 加锁更新状态：L0 使用 HashSet 过滤（并发 flush 可能增加新 L0），L1 直接替换
- 记录 Manifest，删除旧 SST 文件

### 读路径更新（Task 3）

`LsmIteratorInner` 类型从 2-way 改为 3-way：
```rust
TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,  // L1+ 层
>
```

`get()` 检查 L1+ 层：使用 `SstConcatIterator::create_and_seek_to_key`，配合 bloom filter 跳过无关 SST

---

## Day 2.2：Simple Leveled Compaction

### 触发条件

1. **L0 触发**：`l0_sstables.len() >= level0_file_num_compaction_trigger`，将全部 L0 压入 L1
2. **大小比率触发**：对于 L1..Lmax 相邻层，若 `lower_size / upper_size < size_ratio_percent / 100`，则压实 upper 层到 lower 层
   - 注意：`upper_size == 0` 时跳过（避免除零），否则会触发无效压实

### apply_compaction_result()

- upper 层是 L0 时使用 HashSet 过滤（并发 flush 保护）
- upper 层是 Lx 时直接 assert 匹配后清空
- lower 层直接替换为新 SST 输出

---

## Day 2.3：Tiered（Universal）Compaction

### 核心特点

- **无 L0**：每次 flush 直接创建新 tier（最新 tier 在 `levels[0]`）
- `flush_to_l0()` 返回 false，`force_flush_next_imm_memtable` 改为 `levels.insert(0, (sst_id, vec![sst_id]))`

### 三种触发条件（按优先级）

1. **空间放大比率（Space Amplification）**：
   `(sum(all_tiers_except_last) / last_tier_size) * 100 >= max_size_amplification_percent`
   触发全量压实（所有 tier）

2. **大小比率（Size Ratio）**：
   从第一个 tier 开始累加，找到 `next_tier / cumulative > (100 + size_ratio) / 100` 的位置，
   压实前面所有 tier（需满足 `id + 1 >= min_merge_width`）

3. **减少 sorted runs**：tier 数 >= `num_tiers` 时，合并前 `min(num_tiers, max_merge_width)` 个 tier

### apply_compaction_result()

使用 HashMap 追踪要移除的 tier。遍历所有 tier，跳过被压实的，在最后一个被移除的 tier 后插入新 tier（以 `output[0]` 为 tier ID）。

---

## Day 2.4：Leveled Compaction（RocksDB 风格）

### 动态目标大小计算

```
target[Lmax] = max(real_size[Lmax], base_level_size_mb)
target[L] = target[L+1] / level_size_multiplier  (若 target[L+1] > base_size)
base_level = 第一个 target > 0 的层
```

LSM 为空时，target 全为 0（除最后一层），L0 直接刷到 base_level。

### 触发优先级

1. **L0 优先**：L0 数量 >= `level0_file_num_compaction_trigger`，将全部 L0 压入 base_level
2. **最高优先级层**：计算 `real_size / target_size`，选优先级最高（>1.0）的层；选该层中 **最老（最小 SST ID）** 的 SST

### find_overlapping_ssts()

- 计算 upper SST 的 key 范围 `[begin_key, end_key]`
- 在 lower 层中找所有与之重叠的 SST：`!(last_key < begin_key || first_key > end_key)`

### apply_compaction_result()

- 从 upper/lower 层移除对应 SST（HashSet 过滤，L0 用 filter 保护并发）
- 将 output 加入 lower 层
- **非恢复模式下**按 first_key 排序 lower 层（恢复时 SST 未加载，不能排序）

---

## Day 2.5：Manifest 持久化

### 文件格式

每条记录：`| len_u64 | json_bytes | crc32_u32 |`

- JSON 序列化的 `ManifestRecord`（`Flush(sst_id)`, `NewMemtable(id)`, `Compaction(task, output)`）
- CRC32 校验防止数据损坏

### 写入时机

| 操作 | Manifest 记录 |
|------|--------------|
| freeze memtable | `NewMemtable(new_id)` |
| flush imm_memtable | `Flush(sst_id)` |
| trigger_compaction | `Compaction(task, output_ids)` |
| force_full_compaction | `Compaction(task, output_ids)` |

### 崩溃恢复（open 流程）

```
1. 如果 MANIFEST 不存在：全新启动，创建 manifest，记录初始 memtable
2. 如果 MANIFEST 存在：
   a. 回放所有 ManifestRecord：
      - NewMemtable: 加入 memtables 集合
      - Flush: 从 memtables 移除，加入 l0/tier
      - Compaction: apply_compaction_result(in_recovery=true)
   b. 加载所有 SST 文件
   c. Leveled 压实：对每层按 first_key 重新排序
   d. 创建新 memtable（max_id + 1），记录 NewMemtable
```

### MiniLsm::close()（无 WAL 时）

1. 停止 compaction/flush 后台线程
2. 如无 WAL：冻结当前 memtable，循环 flush 所有 imm_memtables
3. 如有 WAL：仅 sync 当前 WAL，不 flush

---

## Day 2.6：Write-Ahead Log（WAL）

### 文件格式

每条记录：`| key_len_u16 | key | value_len_u16 | value | crc32_u32 |`

- CRC32 使用 `crc32fast::Hasher` 增量计算（覆盖 key_len + key + value_len + value）
- 使用 `BufWriter` 提升写性能，仅在 `sync()` 时 flush + fsync

### MemTable WAL 集成

- `create_with_wal(id, path)`: 创建 SkipMap + WAL 文件
- `recover_from_wal(id, path)`: 从 WAL 文件恢复 SkipMap
- `put()`: 写入 SkipMap 后写入 WAL（如果存在）
- `sync_wal()`: 调用 WAL 的 flush + fsync

### 存储层 WAL 集成

- `force_freeze_memtable()`: WAL 模式下创建带 WAL 的 memtable，冻结前 sync 旧 memtable 的 WAL
- `force_flush_next_imm_memtable()`: flush 完成后删除 WAL 文件
- `open()` 恢复时：从 manifest 的 `memtables` 集合中恢复 WAL memtable
- 只恢复非空的 WAL memtable（空 WAL 不需要恢复）

---

## Day 2.7：write_batch API

### 实现

`write_batch()` 遍历批次中的每条记录，调用 memtable.put()，每条后检查 `try_freeze`。

`put()` 和 `delete()` 委托给 `write_batch()`，避免代码重复。

---

## 关键实现细节

### 并发安全
- **state_lock**：序列化所有写操作（freeze、flush、compact）
- **copy-on-modify**：写操作克隆 state，修改后原子替换 Arc
- **L0 HashSet 过滤**：compaction 期间新 L0 可能被并发 flush，用 HashSet remove 安全过滤

### 实现陷阱
- **Simple Leveled**：upper 层为空时不触发压实（除零保护）
- **Tiered 刷盘**：`flush_to_l0()` 为 false 时不加入 l0_sstables，改为在 levels 头部插入新 tier
- **Leveled 恢复**：`apply_compaction_result(in_recovery=true)` 不按 first_key 排序（SST 尚未加载）；恢复后统一排序
- **Manifest 和 WAL 顺序**：先刷盘数据（sync dir），再写 manifest；确保 manifest 记录的 SST 一定存在
- **close() 时不用 freeze_memtable**：直接用私有 API 避免在关闭时记录多余的 NewMemtable manifest 记录
