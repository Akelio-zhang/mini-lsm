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

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + 'static,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let watermark = self.mvcc().watermark();
        let compaction_filters = self.compaction_filters.lock().clone();
        let mut builder: Option<SsTableBuilder> = None;
        let mut new_sst = Vec::new();
        let mut last_key: Vec<u8> = Vec::new();
        let mut first_key_below_watermark = true;
        let mut builder_has_entries = false;

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
                builder_has_entries = false;
            }
            let b = builder.as_mut().unwrap();

            let should_add;
            {
                let key_ref = iter.key().key_ref();
                let ts = iter.key().ts();
                let value = iter.value();

                let same_key = key_ref == last_key.as_slice();
                if !same_key {
                    last_key.clear();
                    last_key.extend_from_slice(key_ref);
                    first_key_below_watermark = true;
                }

                should_add = if ts <= watermark {
                    if first_key_below_watermark {
                        first_key_below_watermark = false;
                        if compact_to_bottom_level && value.is_empty() {
                            false
                        } else {
                            let k = last_key.clone();
                            !compaction_filters.iter().any(|f| match f {
                                CompactionFilter::Prefix(prefix) => k.starts_with(prefix.as_ref()),
                            })
                        }
                    } else {
                        false
                    }
                } else {
                    true
                };

                if should_add {
                    b.add(iter.key(), iter.value());
                    builder_has_entries = true;
                }
            }

            iter.next()?;

            let should_split = b.estimated_size() >= self.options.target_sst_size
                && (!iter.is_valid() || iter.key().key_ref() != last_key.as_slice());

            if should_split {
                if builder_has_entries {
                    let sst_id = self.next_sst_id();
                    let sst = Arc::new(builder.take().unwrap().build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?);
                    new_sst.push(sst);
                } else {
                    builder = None;
                }
                builder_has_entries = false;
            }
        }

        if let Some(b) = builder.take() {
            if builder_has_entries {
                let sst_id = self.next_sst_id();
                new_sst.push(Arc::new(b.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?));
            }
        }
        Ok(new_sst)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_iters = l0_sstables
                    .iter()
                    .map(|id| {
                        Box::new(
                            SsTableIterator::create_and_seek_to_first(
                                snapshot.sstables.get(id).unwrap().clone(),
                            )
                            .unwrap(),
                        )
                    })
                    .collect::<Vec<_>>();
                let l1_ssts = l1_sstables
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect::<Vec<_>>();
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_ssts)?,
                )?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                let lower_ssts = lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect::<Vec<_>>();
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                match upper_level {
                    Some(_) => {
                        let upper_ssts = upper_level_sst_ids
                            .iter()
                            .map(|id| snapshot.sstables.get(id).unwrap().clone())
                            .collect::<Vec<_>>();
                        let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                        self.compact_generate_sst_from_iter(
                            TwoMergeIterator::create(upper_iter, lower_iter)?,
                            task.compact_to_bottom_level(),
                        )
                    }
                    None => {
                        let upper_iters = upper_level_sst_ids
                            .iter()
                            .map(|id| {
                                Box::new(
                                    SsTableIterator::create_and_seek_to_first(
                                        snapshot.sstables.get(id).unwrap().clone(),
                                    )
                                    .unwrap(),
                                )
                            })
                            .collect::<Vec<_>>();
                        self.compact_generate_sst_from_iter(
                            TwoMergeIterator::create(
                                MergeIterator::create(upper_iters),
                                lower_iter,
                            )?,
                            task.compact_to_bottom_level(),
                        )
                    }
                }
            }
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                let iters = tiers
                    .iter()
                    .map(|(_, sst_ids)| {
                        let ssts = sst_ids
                            .iter()
                            .map(|id| snapshot.sstables.get(id).unwrap().clone())
                            .collect::<Vec<_>>();
                        Box::new(SstConcatIterator::create_and_seek_to_first(ssts).unwrap())
                    })
                    .collect::<Vec<_>>();
                self.compact_generate_sst_from_iter(
                    MergeIterator::create(iters),
                    task.compact_to_bottom_level(),
                )
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let new_ssts = self.compact(&task)?;
        let mut new_ids = Vec::with_capacity(new_ssts.len());
        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                assert!(state.sstables.remove(sst).is_some());
            }
            for sst in new_ssts {
                new_ids.push(sst.sst_id());
                state.sstables.insert(sst.sst_id(), sst);
            }
            // Remove compacted L0 SSTs (new ones may have been flushed concurrently)
            let mut l0_set: HashSet<usize> = l0_sstables.iter().copied().collect();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|id| !l0_set.remove(id))
                .copied()
                .collect();
            assert!(l0_set.is_empty());
            assert_eq!(state.levels[0].1, l1_sstables);
            state.levels[0].1 = new_ids.clone();
            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(task, new_ids.clone()),
                )?;
            }
        }
        for id in l0_sstables.iter().chain(l1_sstables.iter()) {
            let _ = std::fs::remove_file(self.path_of_sst(*id));
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = match self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        {
            Some(t) => t,
            None => return Ok(()),
        };
        let new_ssts = self.compact(&task)?;
        let output: Vec<usize> = new_ssts.iter().map(|s| s.sst_id()).collect();
        let ssts_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for sst in &new_ssts {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }
            let (mut new_snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            let mut to_remove = Vec::new();
            for id in &files_to_remove {
                let sst = new_snapshot.sstables.remove(id).unwrap();
                to_remove.push(sst);
            }
            *self.state.write() = Arc::new(new_snapshot);
            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(&state_lock, ManifestRecord::Compaction(task, output))?;
            }
            to_remove
        };
        for sst in ssts_to_remove {
            let _ = std::fs::remove_file(self.path_of_sst(sst.sst_id()));
        }
        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
