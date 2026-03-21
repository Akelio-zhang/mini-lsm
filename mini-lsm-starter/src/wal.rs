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

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut ptr: &[u8] = &buf;
        while ptr.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = ptr.get_u16() as usize;
            hasher.write_u16(key_len as u16);
            let key = &ptr[..key_len];
            hasher.write(key);
            ptr.advance(key_len);
            let ts = ptr.get_u64();
            hasher.write_u64(ts);
            let val_len = ptr.get_u16() as usize;
            hasher.write_u16(val_len as u16);
            let value = Bytes::copy_from_slice(&ptr[..val_len]);
            hasher.write(&value);
            ptr.advance(val_len);
            let checksum = ptr.get_u32();
            if hasher.finalize() != checksum {
                bail!("WAL checksum mismatch");
            }
            skiplist.insert(
                KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key), ts),
                value,
            );
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_batch(&[(KeySlice::from_slice(key, TS_DEFAULT), value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::new();
        for (key, value) in data {
            let mut hasher = crc32fast::Hasher::new();
            hasher.write_u16(key.key_len() as u16);
            buf.put_u16(key.key_len() as u16);
            hasher.write(key.key_ref());
            buf.put_slice(key.key_ref());
            hasher.write_u64(key.ts());
            buf.put_u64(key.ts());
            hasher.write_u16(value.len() as u16);
            buf.put_u16(value.len() as u16);
            hasher.write(value);
            buf.put_slice(value);
            buf.put_u32(hasher.finalize());
        }
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
