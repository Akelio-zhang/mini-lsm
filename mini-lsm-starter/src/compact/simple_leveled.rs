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

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // L0 trigger
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        // Size ratio trigger for levels 1..max_levels
        let mut level_sizes = vec![0usize]; // index 0 = L0 (unused here)
        for (_, files) in &snapshot.levels {
            level_sizes.push(files.len());
        }

        for i in 1..self.options.max_levels {
            let lower = i + 1;
            let upper_size = level_sizes[i];
            let lower_size = level_sizes[lower];
            // If upper level is empty, no compaction needed (avoid division by zero)
            if upper_size == 0 {
                continue;
            }
            let ratio = lower_size as f64 / upper_size as f64;
            if ratio < self.options.size_ratio_percent as f64 / 100.0 {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(i),
                    upper_level_sst_ids: snapshot.levels[i - 1].1.clone(),
                    lower_level: lower,
                    lower_level_sst_ids: snapshot.levels[lower - 1].1.clone(),
                    is_lower_level_bottom_level: lower == self.options.max_levels,
                });
            }
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();

        if let Some(upper_level) = task.upper_level {
            assert_eq!(task.upper_level_sst_ids, snapshot.levels[upper_level - 1].1);
            files_to_remove.extend(&snapshot.levels[upper_level - 1].1);
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            // L0 compaction — use HashSet since new L0s may have been flushed concurrently
            let mut l0_set: HashSet<usize> = task.upper_level_sst_ids.iter().copied().collect();
            files_to_remove.extend(task.upper_level_sst_ids.iter().copied());
            snapshot.l0_sstables = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|id| !l0_set.remove(id))
                .collect();
            assert!(l0_set.is_empty());
        }

        assert_eq!(
            task.lower_level_sst_ids,
            snapshot.levels[task.lower_level - 1].1
        );
        files_to_remove.extend(&snapshot.levels[task.lower_level - 1].1);
        snapshot.levels[task.lower_level - 1].1 = output.to_vec();

        (snapshot, files_to_remove)
    }
}
