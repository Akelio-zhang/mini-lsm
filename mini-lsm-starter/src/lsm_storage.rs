// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::{
    StorageIterator,
    concat_iterator::SstConcatIterator,
    merge_iterator::MergeIterator,
    two_merge_iterator::TwoMergeIterator,
};
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::table::FileObject;
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // Stop background threads
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(t) = compaction_thread.take() {
            t.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(t) = flush_thread.take() {
            t.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // Flush all memtables to disk
        if !self.inner.state.read().memtable.is_empty() {
            self.inner.force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(o) => CompactionController::Leveled(LeveledCompactionController::new(o.clone())),
            CompactionOptions::Tiered(o) => CompactionController::Tiered(TieredCompactionController::new(o.clone())),
            CompactionOptions::Simple(o) => CompactionController::Simple(SimpleLeveledCompactionController::new(o.clone())),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let block_cache = Arc::new(BlockCache::new(1024));
        let manifest_path = path.join("MANIFEST");
        let mut next_sst_id = 1usize;
        let manifest;
        let mut state = LsmStorageState::create(&options);

        if !manifest_path.exists() {
            // Fresh start
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            // Recovery
            let (m, records) = Manifest::recover(&manifest_path)?;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::NewMemtable(id) => {
                        next_sst_id = next_sst_id.max(id);
                        memtables.insert(id);
                    }
                    ManifestRecord::Flush(sst_id) => {
                        memtables.remove(&sst_id);
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;
                        next_sst_id = next_sst_id
                            .max(output.iter().max().copied().unwrap_or(0));
                    }
                }
            }

            // Load SSTs
            for &sst_id in state.l0_sstables.iter().chain(state.levels.iter().flat_map(|(_, v)| v)) {
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, sst_id))?,
                )?;
                state.sstables.insert(sst_id, Arc::new(sst));
            }

            // Sort levels for leveled compaction after recovery
            if let CompactionController::Leveled(_) = &compaction_controller {
                for (_, ssts) in &mut state.levels {
                    ssts.sort_by(|a, b| {
                        state.sstables[a].first_key().cmp(state.sstables[b].first_key())
                    });
                }
            }

            next_sst_id += 1;

            // Recover WAL memtables
            if options.enable_wal {
                for &id in memtables.iter() {
                    let memtable = MemTable::recover_from_wal(
                        id,
                        Self::path_of_wal_static(path, id),
                    )?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }

            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;
        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state = self.state.read();
        // check current memtable
        if let Some(v) = state.memtable.get(key) {
            if v.is_empty() {
                return Ok(None); // tombstone
            }
            return Ok(Some(v));
        }
        // check immutable memtables (newest first)
        for imm in &state.imm_memtables {
            if let Some(v) = imm.get(key) {
                if v.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(v));
            }
        }
        // check L0 SSTs (newest first)
        let key_slice = KeySlice::from_slice(key);
        for &sst_id in &state.l0_sstables {
            let sst = state.sstables.get(&sst_id).unwrap().clone();
            if key_slice < sst.first_key().as_key_slice()
                || key_slice > sst.last_key().as_key_slice()
            {
                continue;
            }
            if let Some(ref bloom) = sst.bloom {
                if !bloom.may_contain(farmhash::fingerprint32(key)) {
                    continue;
                }
            }
            let iter = SsTableIterator::create_and_seek_to_key(sst, key_slice)?;
            if iter.is_valid() && iter.key() == key_slice {
                let v = Bytes::copy_from_slice(iter.value());
                if v.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(v));
            }
        }
        // check L1+ levels using SstConcatIterator
        for (_, level_sst_ids) in &state.levels {
            let mut level_ssts = Vec::new();
            for id in level_sst_ids {
                let sst = state.sstables.get(id).unwrap().clone();
                if key_slice < sst.first_key().as_key_slice()
                    || key_slice > sst.last_key().as_key_slice()
                {
                    continue;
                }
                if let Some(ref bloom) = sst.bloom {
                    if !bloom.may_contain(farmhash::fingerprint32(key)) {
                        continue;
                    }
                }
                level_ssts.push(sst);
            }
            let iter = SstConcatIterator::create_and_seek_to_key(level_ssts, key_slice)?;
            if iter.is_valid() && iter.key() == key_slice {
                let v = Bytes::copy_from_slice(iter.value());
                if v.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let size = {
            let state = self.state.read();
            state.memtable.put(key, value)?;
            state.memtable.approximate_size()
        };
        self.try_freeze(size)?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let size = {
            let state = self.state.read();
            state.memtable.put(key, b"")?;
            state.memtable.approximate_size()
        };
        self.try_freeze(size)?;
        Ok(())
    }

    fn try_freeze(&self, approximate_size: usize) -> Result<()> {
        if approximate_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        std::fs::File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(new_id, self.path_of_wal(new_id))?)
        } else {
            Arc::new(MemTable::create(new_id))
        };
        {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            let old = std::mem::replace(&mut snapshot.memtable, new_memtable);
            old.sync_wal()?;
            snapshot.imm_memtables.insert(0, old);
            *state = Arc::new(snapshot);
        }
        if let Some(manifest) = &self.manifest {
            manifest.add_record(state_lock_observer, ManifestRecord::NewMemtable(new_id))?;
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let imm = {
            let state = self.state.read();
            state.imm_memtables.last().unwrap().clone()
        };
        let mut builder = crate::table::SsTableBuilder::new(self.options.block_size);
        imm.flush(&mut builder)?;
        let sst_id = imm.id();
        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            snapshot.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // tiered compaction: each flush creates a new tier
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            snapshot.sstables.insert(sst_id, Arc::new(sst));
            *state = Arc::new(snapshot);
        }
        if self.options.enable_wal {
            let _ = std::fs::remove_file(self.path_of_wal(sst_id));
        }
        if let Some(manifest) = &self.manifest {
            manifest.add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = self.state.read();
        // merge memtable iterators
        let mut mem_iters: Vec<Box<MemTableIterator>> = Vec::new();
        mem_iters.push(Box::new(state.memtable.scan(lower, upper)));
        for imm in &state.imm_memtables {
            mem_iters.push(Box::new(imm.scan(lower, upper)));
        }
        let mem_merge = MergeIterator::create(mem_iters);

        // merge L0 SST iterators (newest first = index 0 is highest priority)
        let mut sst_iters: Vec<Box<SsTableIterator>> = Vec::new();
        for &sst_id in &state.l0_sstables {
            let sst = state.sstables.get(&sst_id).unwrap().clone();
            // range filter: skip SSTs that don't overlap with [lower, upper]
            match upper {
                Bound::Included(key) => {
                    if sst.first_key().raw_ref() > key {
                        continue;
                    }
                }
                Bound::Excluded(key) => {
                    if sst.first_key().raw_ref() >= key {
                        continue;
                    }
                }
                Bound::Unbounded => {}
            }
            match lower {
                Bound::Included(key) => {
                    if sst.last_key().raw_ref() < key {
                        continue;
                    }
                }
                Bound::Excluded(key) => {
                    if sst.last_key().raw_ref() <= key {
                        continue;
                    }
                }
                Bound::Unbounded => {}
            }
            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter =
                        SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst)?,
            };
            sst_iters.push(Box::new(iter));
        }
        let sst_merge = MergeIterator::create(sst_iters);

        let end_bound = match upper {
            Bound::Included(key) => Bound::Included(Bytes::copy_from_slice(key)),
            Bound::Excluded(key) => Bound::Excluded(Bytes::copy_from_slice(key)),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Build L1+ level iterators using SstConcatIterator
        let mut level_iters: Vec<Box<SstConcatIterator>> = Vec::new();
        for (_, level_sst_ids) in &state.levels {
            let mut level_ssts = Vec::new();
            for &id in level_sst_ids {
                let sst = state.sstables.get(&id).unwrap().clone();
                match upper {
                    Bound::Included(key) => {
                        if sst.first_key().raw_ref() > key { continue; }
                    }
                    Bound::Excluded(key) => {
                        if sst.first_key().raw_ref() >= key { continue; }
                    }
                    Bound::Unbounded => {}
                }
                match lower {
                    Bound::Included(key) => {
                        if sst.last_key().raw_ref() < key { continue; }
                    }
                    Bound::Excluded(key) => {
                        if sst.last_key().raw_ref() <= key { continue; }
                    }
                    Bound::Unbounded => {}
                }
                level_ssts.push(sst);
            }
            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }
        let level_merge = MergeIterator::create(level_iters);

        let two_merge = TwoMergeIterator::create(mem_merge, sst_merge)?;
        let three_merge = TwoMergeIterator::create(two_merge, level_merge)?;
        Ok(FusedIterator::new(LsmIterator::new(three_merge, end_bound)?))
    }
}
