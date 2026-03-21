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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Decode the key and value at `idx` into self.key and self.value_range.
    fn decode_at(&mut self, idx: usize) {
        let offset = self.block.offsets[idx] as usize;
        let data = &self.block.data;
        // prefix-compressed key: overlap_len(2B) + rest_len(2B) + rest_key
        let overlap = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        let rest_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
        let rest_start = offset + 4;
        let rest_end = rest_start + rest_len;
        // reconstruct: first_key[..overlap] + rest
        let mut key = self.first_key.raw_ref()[..overlap].to_vec();
        key.extend_from_slice(&data[rest_start..rest_end]);
        self.key = KeyVec::from_vec(key);
        // value
        let val_len = u16::from_be_bytes([data[rest_end], data[rest_end + 1]]) as usize;
        let val_start = rest_end + 2;
        let val_end = val_start + val_len;
        self.value_range = (val_start, val_end);
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.is_empty() {
            return;
        }
        self.idx = 0;
        // First key has no prefix so we can decode it with empty first_key
        // (overlap will be 0 for the first entry)
        self.first_key = KeyVec::new(); // reset so decode_at uses overlap=0 correctly
        self.decode_at(0);
        self.first_key = self.key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
        } else {
            self.decode_at(self.idx);
        }
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // Must establish first_key first, then do linear scan from 0
        // (binary search doesn't work with prefix compression without random access)
        self.seek_to_first();
        if !self.is_valid() {
            return;
        }
        while self.is_valid() && self.key.as_key_slice() < key {
            self.next();
        }
    }
}
