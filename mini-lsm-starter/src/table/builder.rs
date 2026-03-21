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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use farmhash;

use super::{BlockMeta, FileObject, SsTable, bloom::Bloom};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            max_ts: 0,
        }
    }

    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let first_key = std::mem::take(&mut self.first_key).into_key_bytes();
        let last_key = std::mem::take(&mut self.last_key).into_key_bytes();
        let offset = self.data.len();
        let block = builder.build();
        let encoded = block.encode();
        self.data.extend_from_slice(&encoded);
        self.meta.push(BlockMeta {
            offset,
            first_key,
            last_key,
        });
    }

    /// Adds a key-value pair to SSTable.
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
        self.max_ts = self.max_ts.max(key.ts());
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }
        self.last_key.set_from_slice(key);
        if self.builder.add(key, value) {
            return;
        }
        // Block is full — finish it and start a new one
        self.finish_block();
        let added = self.builder.add(key, value);
        assert!(added, "single entry too large for block");
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // flush the last (possibly partial) block
        if !self.builder.is_empty() {
            self.finish_block();
        }
        let mut buf = self.data;
        let meta_offset = buf.len() as u32;
        // encode block meta
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        // build bloom filter
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
        let bloom_offset = buf.len() as u32;
        bloom.encode(&mut buf);
        buf.put_u32(meta_offset);
        buf.put_u32(bloom_offset);

        let first_key = self
            .meta
            .first()
            .map(|m| m.first_key.clone())
            .unwrap_or_default();
        let last_key = self
            .meta
            .last()
            .map(|m| m.last_key.clone())
            .unwrap_or_default();

        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
