# Week 3: MVCC、事务与版本回收

本文记录 Week 3 各阶段的核心目标、数据结构设计与实现要点。  
Week 3 在 Week 1/2 的 LSM 框架上，引入了 **多版本并发控制（MVCC）**，使存储引擎具备：

1. 快照读（Snapshot Read）
2. 事务读写（Transaction）
3. 可串行化检查（Serializable）
4. 基于水位线（Watermark）的历史版本回收
5. 带 Compaction Filter 的底层清理

---

## 整体架构

Week 3 的核心是把“key”从单版本扩展为“用户 key + 时间戳”。

```
逻辑键： user_key
物理键： (user_key, ts)
排序规则：user_key 升序，ts 降序（同 key 下最新版本在前）
```

读取某个快照 `read_ts` 时：
- 可见版本条件：`version_ts <= read_ts`
- 同一个 user_key 只返回第一个可见版本
- 若该版本 value 为空（tombstone），视为已删除

---

## Day 1：Key 加时间戳

### 目标

把整条存储链路切换到带时间戳的 key：
- MemTable
- Block 编码
- SST 元信息
- 迭代器比较逻辑

### 关键设计

`Key<T>` 从“仅字节串”扩展为：
```rust
Key<T>(T, u64)
```

常用语义：
- `TS_MAX` / `TS_RANGE_BEGIN`：用于“从某个 user_key 的最新版本开始找”
- `TS_MIN` / `TS_RANGE_END`：用于边界控制

排序比较：
- 先比 `user_key`
- user_key 相同时，比 `Reverse(ts)`（ts 越大越靠前）

这样可保证“同一 user_key 的最新版本天然排在前面”，后续快照读可在线性迭代中快速跳过旧版本。

### Day 1 测试覆盖

`src/tests/week3_day1.rs` 主要验证“多版本 key 编码/解码链路”：

1. `test_sst_build_multi_version_simple`
- 构建同一 user_key（`233`）的两个版本（`ts=233` 与 `ts=0`）
- 验证 `SsTableBuilder` 可以写入多版本键而不崩溃

2. `test_sst_build_multi_version_hard`
- 构造 100 条 `(key, ts, value)` 测试数据（每 5 条共享同一 user_key，ts 递减）
- 写入 SST 后重新 `open` 并用 `SsTableIterator` 全量扫描
- 断言扫描结果与输入的 `(key, ts, value)` 顺序一致，覆盖：
  - 带 ts 的 key 持久化格式
  - block/sstable 迭代链路对 ts 的保真

---

## Day 2-3：Snapshot Read（快照读）

### 目标

让 `get`/`scan` 在指定读时间戳下返回一致视图。

### 核心接口

- `get_with_ts(key, read_ts)`
- `scan_with_ts(lower, upper, read_ts)`
- `new_txn()` 创建只读快照句柄（先用于 snapshot，再扩展到事务）

### 读取规则

对同一 user_key：
1. 跳过所有 `ts > read_ts` 的版本（“未来版本”对该快照不可见）
2. 选择第一个 `ts <= read_ts` 的版本
3. 若 value 为空，返回删除语义（点查为 `None`，扫描不产出该 key）

### 迭代器变化

`LsmIterator` 增加 `read_ts`，可见性逻辑变为：
- 先检查范围上界
- 再执行 MVCC 可见性过滤
- 最后执行 tombstone 过滤

这个顺序可以保证：
- 不跨范围返回
- 不把未来版本或被删除键泄漏给快照

### SST 额外信息

SST 记录 `max_ts`，用于恢复时推导全局初始提交时间戳（避免重启后时间倒退）。

---

## Day 4：Watermark 与版本 GC 基础

### 目标

追踪“当前系统中最老还活着的快照读时间戳”，作为版本回收边界。

### Watermark 结构

使用 `BTreeMap<u64, usize>` 计数：
- `add_reader(ts)`：读者 +1
- `remove_reader(ts)`：读者 -1，归零删除
- `watermark()`：返回最小活动 `read_ts`

语义：
- 小于 watermark 的历史版本，原则上可被 GC（视 compaction 场景）
- 如果没有活动读者，watermark 退化到最新提交时间戳

---

## Day 5：Transaction（本地写集 + 提交）

### 目标

实现事务对象：
- 读：快照一致
- 写：先写本地（未提交前外部不可见）
- 提交：一次性写入 LSM

### 事务结构

```text
Transaction {
  read_ts
  local_storage: SkipMap<user_key, value>
  committed: AtomicBool
}
```

### 行为

1. `get`
- 先查 `local_storage`
- 再查底层 `get_with_ts(key, read_ts)`

2. `scan`
- 构建本地迭代器 + 底层快照迭代器
- 用 `TwoMergeIterator` 归并（本地优先）
- 跳过 tombstone

3. `commit`
- 申请新 `commit_ts`
- 把本地写集转为 `(key, commit_ts)` 版本写入 memtable/WAL
- 成功后移除读者水位

补充：
- 重复 commit 会报错
- 空事务 commit 只做资源清理

---

## Day 6：Serializable（可串行化检查）

### 目标

在快照隔离基础上增加冲突检测，阻止典型读写偏斜。

### 方案（简化 OCC/SSI）

事务维护：
- 写集合哈希（write set hashes）
- 读集合哈希（read set hashes，来自 `get` 与 `scan` 访问）

提交时：
1. 加 `commit_lock`
2. 检查 `(read_ts, +inf)` 区间内已提交事务的写集合
3. 若与当前读集合有交集，提交失败（序列化冲突）
4. 否则写入并记录当前事务写集合

再结合 watermark 清理过旧提交元信息，避免 `committed_txns` 无界增长。

---

## Day 7：Bottom-level GC 与 Compaction Filter

### 目标

在最底层 compaction 时做真正的历史裁剪，并支持按前缀过滤清理。

### 底层版本回收规则

以 `watermark` 为边界，对每个 user_key：

1. 保留所有 `ts > watermark` 的版本（仍可能被活跃快照读取）
2. 在 `ts <= watermark` 的版本里，最多保留 1 个“基线版本”
3. 若该“基线版本”是 tombstone 且没有更高可见版本需要保留，可直接丢弃整键历史

这保证：
- 读者安全（不破坏仍在运行的快照）
- 历史可控（避免无限累积）

### Compaction Filter

支持按前缀注册过滤规则（如 `Prefix("table2_")`）：
- 命中过滤器的 key 在底层 compaction 中可更激进裁剪
- 常用于“整表清理”或 TTL/归档类策略扩展

---

## Week 3 模块文件对照

| 模块 | 文件 | 职责 |
|------|------|------|
| MVCC 元数据 | `src/mvcc.rs`, `src/mvcc/watermark.rs` | 全局 ts、watermark、提交元信息 |
| 事务 | `src/mvcc/txn.rs` | 本地写集、快照读、提交、可串行化检查 |
| 存储入口 | `src/lsm_storage.rs` | `new_txn/get_with_ts/scan_with_ts/write_batch_inner` |
| 读路径 | `src/lsm_iterator.rs` | `read_ts` 可见性过滤 + tombstone 处理 |
| 内存层 | `src/mem_table.rs`, `src/wal.rs` | 时间戳 key 写入与恢复 |
| 压实 | `src/compact.rs` | watermark 驱动的版本回收 + compaction filter |
| 表层 | `src/table.rs`, `src/table/builder.rs` | `max_ts` 维护与恢复 |

---

## Week 3 测试覆盖（day1-day7）

- day1：多版本 key 的 SST 构建、重开与迭代正确性（key+ts 保真）
- day2：compaction 集成（同 key 版本切分与合并）
- day3：memtable + lsm iterator 的 MVCC 可见性、SST `max_ts`
- day4：watermark 行为、snapshot 生命周期、MVCC compaction
- day5：事务本地读写与提交可见性
- day6：serializable 冲突检测（点查/扫描）
- day7：compaction filter + watermark 结合下的历史清理

在当前实现中，上述 week3 用例与既有 week1/week2 用例可同时通过。
