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

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    fn seek_to_block_idx(table: &Arc<SsTable>, blk_idx: usize) -> Result<BlockIterator> {
        let block = table.read_block_cached(blk_idx)?;
        Ok(BlockIterator::create_and_seek_to_first(block))
    }

    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_iter = Self::seek_to_block_idx(&table, 0)?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter = Self::seek_to_block_idx(&self.table, 0)?;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        let mut iter = Self {
            table,
            blk_iter,
            blk_idx,
        };
        // If the block iterator is exhausted, try next blocks
        if !iter.blk_iter.is_valid() {
            iter.advance_to_next_block()?;
        }
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let blk_idx = self.table.find_block_idx(key);
        let block = self.table.read_block_cached(blk_idx)?;
        self.blk_idx = blk_idx;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        if !self.blk_iter.is_valid() {
            self.advance_to_next_block()?;
        }
        Ok(())
    }

    fn advance_to_next_block(&mut self) -> Result<()> {
        self.blk_idx += 1;
        while self.blk_idx < self.table.num_of_blocks() {
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
            if self.blk_iter.is_valid() {
                return Ok(());
            }
            self.blk_idx += 1;
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.blk_iter.key()
    }

    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.advance_to_next_block()?;
        }
        Ok(())
    }
}
