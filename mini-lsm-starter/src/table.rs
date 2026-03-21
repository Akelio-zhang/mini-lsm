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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// Format: [num_blocks(4B)] ([offset(4B)][fk_len(2B)][fk_bytes][fk_ts(8B)][lk_len(2B)][lk_bytes][lk_ts(8B)])* [max_ts(8B)] [checksum(4B)]
    pub fn encode_block_meta(block_meta: &[BlockMeta], max_ts: u64, buf: &mut Vec<u8>) {
        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.key_len() as u16);
            buf.put_slice(meta.first_key.key_ref());
            buf.put_u64(meta.first_key.ts());
            buf.put_u16(meta.last_key.key_len() as u16);
            buf.put_slice(meta.last_key.key_ref());
            buf.put_u64(meta.last_key.ts());
        }
        buf.put_u64(max_ts);
        // checksum covers everything after num_blocks
        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
    }

    /// Decode block meta from a buffer.
    /// Format: [num_blocks(4B)] ([offset(4B)][fk_len(2B)][fk_bytes][fk_ts(8B)][lk_len(2B)][lk_bytes][lk_ts(8B)])* [max_ts(8B)] [checksum(4B)]
    /// The checksum covers everything after num_blocks(4B).
    pub fn decode_block_meta(buf: &[u8]) -> Result<(Vec<BlockMeta>, u64)> {
        // Verify checksum: covers buf[4..buf.len()-4]
        let checksum = u32::from_be_bytes(buf[buf.len() - 4..].try_into().unwrap());
        if crc32fast::hash(&buf[4..buf.len() - 4]) != checksum {
            bail!("block meta checksum mismatch");
        }
        let mut buf = &buf[..];
        let num = buf.get_u32() as usize;
        let mut metas = Vec::with_capacity(num);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let fk_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(fk_len), buf.get_u64());
            let lk_len = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(lk_len), buf.get_u64());
            metas.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        let max_ts = buf.get_u64();
        buf.get_u32(); // consume checksum (already verified above)
        Ok((metas, max_ts))
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let size = file.size();
        // last 8 bytes: [meta_offset(4B)][bloom_offset(4B)]
        let trailer = file.read(size - 8, 8)?;
        let block_meta_offset =
            u32::from_be_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]) as usize;
        let bloom_offset =
            u32::from_be_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]) as usize;
        // meta section: [block_meta_offset, bloom_offset)
        let meta_len = bloom_offset - block_meta_offset;
        let meta_raw = file.read(block_meta_offset as u64, meta_len as u64)?;
        let (block_meta, max_ts) = BlockMeta::decode_block_meta(meta_raw.as_slice())?;
        // bloom section: [bloom_offset, size-8)
        let bloom_len = size as usize - 8 - bloom_offset;
        let bloom_raw = file.read(bloom_offset as u64, bloom_len as u64)?;
        let bloom = Bloom::decode(&bloom_raw)?;
        let first_key = block_meta
            .first()
            .map(|m| m.first_key.clone())
            .unwrap_or_else(|| KeyBytes::from_bytes_with_ts(bytes::Bytes::new(), TS_DEFAULT));
        let last_key = block_meta
            .last()
            .map(|m| m.last_key.clone())
            .unwrap_or_else(|| KeyBytes::from_bytes_with_ts(bytes::Bytes::new(), TS_DEFAULT));
        Ok(Self {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset;
        let end = if block_idx + 1 < self.block_meta.len() {
            self.block_meta[block_idx + 1].offset
        } else {
            self.block_meta_offset
        };
        let data_with_checksum = self.file.read(offset as u64, (end - offset) as u64)?;
        let checksum = crc32fast::hash(&data_with_checksum[..data_with_checksum.len() - 4]);
        let expected = u32::from_be_bytes(
            data_with_checksum[data_with_checksum.len() - 4..]
                .try_into()
                .unwrap(),
        );
        if checksum != expected {
            bail!("block checksum mismatch");
        }
        Ok(Arc::new(Block::decode(
            &data_with_checksum[..data_with_checksum.len() - 4],
        )))
    }

    /// Read a block from disk, with block cache.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            if let Some(block) = cache.get(&(self.id, block_idx)) {
                return Ok(block);
            }
            let block = self.read_block(block_idx)?;
            cache.insert((self.id, block_idx), block.clone());
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // Find the last block whose first_key <= key
        self.block_meta
            .partition_point(|m| m.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
