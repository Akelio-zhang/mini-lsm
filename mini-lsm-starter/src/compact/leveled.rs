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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlap = Vec::new();
        for &sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[&sst_id];
            if !(sst.last_key() < &begin_key || sst.first_key() > &end_key) {
                overlap.push(sst_id);
            }
        }
        overlap
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // Step 1: compute target level sizes from bottom up
        let mut target_sizes = vec![0usize; self.options.max_levels];
        let mut real_sizes = Vec::with_capacity(self.options.max_levels);
        for i in 0..self.options.max_levels {
            real_sizes.push(
                snapshot.levels[i].1.iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().table_size())
                    .sum::<u64>() as usize,
            );
        }

        let base_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        target_sizes[self.options.max_levels - 1] =
            real_sizes[self.options.max_levels - 1].max(base_size_bytes);

        let mut base_level = self.options.max_levels;
        for i in (0..self.options.max_levels - 1).rev() {
            let next = target_sizes[i + 1];
            if next > base_size_bytes {
                target_sizes[i] = next / self.options.level_size_multiplier;
            }
            if target_sizes[i] > 0 {
                base_level = i + 1;
            }
        }

        // Step 2: L0 flush takes highest priority
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        // Step 3: find level with highest priority (real/target > 1.0)
        let mut priorities: Vec<(f64, usize)> = Vec::new();
        for level in 0..self.options.max_levels {
            if target_sizes[level] == 0 {
                continue;
            }
            let prio = real_sizes[level] as f64 / target_sizes[level] as f64;
            if prio > 1.0 {
                priorities.push((prio, level + 1));
            }
        }
        priorities.sort_by(|a, b| b.partial_cmp(a).unwrap());

        if let Some((_, level)) = priorities.first() {
            let level = *level;
            // Pick oldest (smallest ID) SST
            let selected = *snapshot.levels[level - 1].1.iter().min().unwrap();
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![selected],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(snapshot, &[selected], level + 1),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();

        let mut upper_set: HashSet<usize> = task.upper_level_sst_ids.iter().copied().collect();
        let mut lower_set: HashSet<usize> = task.lower_level_sst_ids.iter().copied().collect();

        if let Some(upper_level) = task.upper_level {
            let new_upper = snapshot.levels[upper_level - 1].1.iter()
                .filter_map(|id| {
                    if upper_set.remove(id) { None } else { Some(*id) }
                })
                .collect();
            assert!(upper_set.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper;
        } else {
            // L0
            let new_l0 = snapshot.l0_sstables.iter()
                .filter_map(|id| {
                    if upper_set.remove(id) { None } else { Some(*id) }
                })
                .collect();
            assert!(upper_set.is_empty());
            snapshot.l0_sstables = new_l0;
        }

        files_to_remove.extend(&task.upper_level_sst_ids);
        files_to_remove.extend(&task.lower_level_sst_ids);

        let mut new_lower: Vec<usize> = snapshot.levels[task.lower_level - 1].1.iter()
            .filter_map(|id| {
                if lower_set.remove(id) { None } else { Some(*id) }
            })
            .collect();
        assert!(lower_set.is_empty());
        new_lower.extend(output);

        if !in_recovery {
            new_lower.sort_by(|a, b| {
                snapshot.sstables[a].first_key().cmp(snapshot.sstables[b].first_key())
            });
        }
        snapshot.levels[task.lower_level - 1].1 = new_lower;

        (snapshot, files_to_remove)
    }
}
