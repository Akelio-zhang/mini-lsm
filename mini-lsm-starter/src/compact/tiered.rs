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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "tiered compaction should not have L0 SSTs"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Trigger 1: space amplification ratio
        let total_except_last: usize = snapshot.levels[..snapshot.levels.len() - 1]
            .iter()
            .map(|(_, files)| files.len())
            .sum();
        let last_size = snapshot.levels.last().unwrap().1.len();
        if last_size > 0 {
            let space_amp = (total_except_last as f64 / last_size as f64) * 100.0;
            if space_amp >= self.options.max_size_amplification_percent as f64 {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                });
            }
        }

        // Trigger 2: size ratio
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut cumulative = 0usize;
        for id in 0..(snapshot.levels.len() - 1) {
            cumulative += snapshot.levels[id].1.len();
            let next_size = snapshot.levels[id + 1].1.len();
            let ratio = next_size as f64 / cumulative as f64;
            if ratio > size_ratio_trigger && id + 1 >= self.options.min_merge_width {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..=id].to_vec(),
                    bottom_tier_included: false,
                });
            }
        }

        // Trigger 3: reduce sorted runs
        let num_to_take = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        Some(TieredCompactionTask {
            tiers: snapshot.levels[..num_to_take].to_vec(),
            bottom_tier_included: snapshot.levels.len() <= num_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "tiered compaction should not have L0 SSTs"
        );
        let mut snapshot = snapshot.clone();
        let mut tier_to_remove: HashMap<usize, &Vec<usize>> = task
            .tiers
            .iter()
            .map(|(id, files)| (*id, files))
            .collect();

        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();

        for (tier_id, files) in &snapshot.levels {
            if let Some(expected) = tier_to_remove.remove(tier_id) {
                assert_eq!(expected, files, "tier files changed after compaction task issued");
                files_to_remove.extend(files.iter().copied());
            } else {
                levels.push((*tier_id, files.clone()));
            }
            if tier_to_remove.is_empty() && !new_tier_added {
                new_tier_added = true;
                if !output.is_empty() {
                    levels.push((output[0], output.to_vec()));
                }
            }
        }
        assert!(tier_to_remove.is_empty(), "some tiers not found in snapshot");

        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
