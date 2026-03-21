# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `mini-lsm-starter` — the student starter code for the [Mini-LSM course](https://skyzh.github.io/mini-lsm/), a hands-on 3-week curriculum for building an LSM-tree key-value storage engine in Rust. Nearly all implementation functions contain `unimplemented!()` as placeholders; the goal is to replace them with correct implementations week by week.

## Commands

All commands should be run from the **workspace root** (`/Users/zzn/repo/mini-lsm/`), not from this subdirectory — the project is a Cargo workspace.

```bash
# Install required tools (cargo-nextest, etc.)
cargo x install-tools

# Copy test cases for a specific week/day into src/tests/
cargo x copy-test --week 1 --day 1

# Run tests (requires nextest)
cargo nextest run -p mini-lsm-starter

# Run a single test
cargo nextest run -p mini-lsm-starter test_task1_memtable_get

# Run all tests for a module
cargo nextest run -p mini-lsm-starter week1

# Check starter code (fmt + clippy)
cargo x scheck

# Interactive CLI (for manual testing after implementation)
cargo run --bin mini-lsm-cli

# Compaction simulator
cargo run --bin compaction-simulator
```

## Architecture

### Storage Layers (bottom-up)

1. **`block`** — The smallest unit of read/cache. A `Block` contains sorted key-value pairs encoded as `[data | offsets | num_elements]`. `BlockBuilder` builds blocks; `BlockIterator` scans them.

2. **`table`** — A Sorted String Table (SST) file on disk. `SsTable` contains multiple `Block`s plus `BlockMeta` (first/last key per block), a Bloom filter, and a `max_ts`. `SsTableBuilder` writes SSTs; `SsTableIterator` scans them. `FileObject` handles the actual file I/O.

3. **`mem_table`** — An in-memory write buffer backed by `crossbeam_skiplist::SkipMap<Bytes, Bytes>`. Supports optional WAL (`wal.rs`). `MemTableIterator` uses `ouroboros` for self-referential lifetime management.

4. **`lsm_storage`** — The main engine. `LsmStorageState` holds:
   - `memtable`: current mutable memtable
   - `imm_memtables`: frozen immutable memtables (newest first)
   - `l0_sstables`: L0 SST IDs (newest first)
   - `levels`: L1–Lmax SST IDs (for leveled/simple) or tiers (for tiered)
   - `sstables`: map from SST ID → `Arc<SsTable>`

   `LsmStorageInner` wraps state in `Arc<RwLock<Arc<LsmStorageState>>>` for concurrent access. `state_lock: Mutex<()>` serializes write operations. `MiniLsm` is the public API wrapper that also manages background flush and compaction threads.

5. **`compact`** — Three compaction strategies: `SimpleLeveledCompactionController`, `TieredCompactionController`, `LeveledCompactionController`. Each implements `generate_compaction_task` and `apply_compaction_result`.

6. **`manifest`** — Persists storage state changes (SST additions, compactions) across restarts.

7. **`mvcc`** — Week 3 only. `LsmMvccInner` tracks commit timestamps and active transactions. `txn::Transaction` implements OCC/SSI. `watermark::Watermark` tracks the minimum active read timestamp for GC.

### Iterators (`iterators/`)

- `StorageIterator` trait: `key()`, `value()`, `is_valid()`, `next()`
- `MergeIterator`: heap-based k-way merge, picks smallest key (latest version wins among equal keys)
- `TwoMergeIterator`: merges two iterators, giving priority to the first
- `ConcatIterator`: chains non-overlapping SST iterators in key order (used for L1+ levels)
- `lsm_iterator::LsmIterator`: top-level iterator skipping tombstones (empty values)

### Key Type (`key.rs`)

`Key<T>` is a newtype over `T: AsRef<[u8]>`. Three aliases: `KeySlice<'a>`, `KeyVec`, `KeyBytes`. In weeks 1–2, keys are plain byte slices. In week 3, keys gain a timestamp suffix. The `TS_ENABLED` flag controls this. Methods marked `raw_ref()` will be removed in week 3.

### Concurrency Model

State is updated via copy-on-modify: hold `state_lock` for write operations, clone and modify `Arc<LsmStorageState>`, then atomically swap the `Arc` inside the `RwLock`. Readers only need the `RwLock` read lock and never block writers for long.

## Implementation Sequence

Tests are gated behind `cargo x copy-test --week W --day D`, which copies `src/tests/weekW_dayD.rs` into `src/tests/`. Implement in order:

| Week | Day | Focus | Key files |
|------|-----|-------|-----------|
| 1 | 1 | MemTable get/put/scan | `mem_table.rs` |
| 1 | 2 | Merge iterator | `iterators/merge_iterator.rs` |
| 1 | 3 | Block encode/decode + iterator | `block/builder.rs`, `block/iterator.rs`, `block.rs` |
| 1 | 4 | SST builder + open + iterator | `table/builder.rs`, `table/iterator.rs`, `table.rs` |
| 1 | 5 | Read path (get/scan across memtables + L0) | `lsm_storage.rs`, `lsm_iterator.rs` |
| 1 | 6 | Write path (freeze + flush + background thread) | `lsm_storage.rs`, `mem_table.rs` |
| 1 | 7 | Bloom filters + prefix key encoding | `table/bloom.rs`, `table/builder.rs` |
| 2 | 1 | Compaction implementation (compact task execution) | `compact.rs` |
| 2 | 2 | Simple leveled compaction | `compact/simple_leveled.rs` |
| 2 | 3 | Tiered compaction | `compact/tiered.rs` |
| 2 | 4 | Leveled compaction | `compact/leveled.rs` |
| 2 | 5 | Manifest | `manifest.rs`, `lsm_storage.rs` |
| 2 | 6 | WAL | `wal.rs`, `mem_table.rs` |
| 2 | 7 | Batch write + checksums | `lsm_storage.rs` |
| 3 | 1 | Timestamp key refactor | `key.rs` (add ts suffix) |
| 3 | 2–3 | Snapshot reads | `lsm_storage.rs`, `lsm_iterator.rs` |
| 3 | 4 | Watermark + GC | `mvcc/watermark.rs` |
| 3 | 5–6 | Transactions + OCC/SSI | `mvcc/txn.rs`, `mvcc.rs` |
| 3 | 7 | Compaction filters | `compact.rs`, `lsm_storage.rs` |

## Test Utilities

`src/tests/harness.rs` provides helpers used across test files. Tests use `tempfile::tempdir()` for isolated storage directories. `LsmStorageOptions::default_for_week1_test()` / `default_for_week2_test()` provide preset configs. `force_freeze_memtable` and `force_flush_next_imm_memtable` are exposed for test control.

## Implementation Gotchas

- **`trigger_flush`** lives in `src/compact.rs`, not `lsm_storage.rs`
- **SST file trailer** (after Day 7): 8 bytes `[meta_offset(4B)][bloom_offset(4B)]` — not 4 bytes as in Day 4 draft
- **ouroboros `MemTableIterator`**: must clone key/value data *inside* `with_iter_mut` closure — cannot return references from it; use `with_item_mut` separately for item mutation
- **`LsmIterator` upper bound**: SST iterators are unaware of scan bounds; carry `end_bound: Bound<Bytes>` in `LsmIterator` and check in `is_valid()` / `next()`
- **SST range filter bounds**: `Excluded(x)` means skip SST if `sst.first_key >= x` (not `>`); use `>=`/`<=` for Excluded, `>`/`<` for Included
- **Block `seek_to_key`**: must be linear scan from first (binary search breaks with prefix key compression)
