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

use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::mem_table::MemTableIterator;
use crate::table::SsTableIterator;

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    read_ts: u64,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut s = Self {
            inner: iter,
            end_bound,
            read_ts,
            prev_key: Vec::new(),
        };
        s.move_to_key()?;
        Ok(s)
    }

    fn in_range(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        }
        match &self.end_bound {
            Bound::Unbounded => true,
            Bound::Included(key) => self.inner.key().key_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.inner.key().key_ref() < key.as_ref(),
        }
    }

    /// Advance to the next user-visible key: skip versions with ts > read_ts,
    /// skip duplicate key bytes (we already emitted this key), and skip tombstones.
    fn move_to_key(&mut self) -> Result<()> {
        loop {
            // Skip entries with ts too new for our snapshot
            while self.inner.is_valid() && self.inner.key().ts() > self.read_ts {
                self.inner.next()?;
            }
            if !self.in_range() {
                break;
            }
            // Skip versions of a key we've already emitted
            if self.inner.key().key_ref() == self.prev_key.as_slice() {
                self.inner.next()?;
                continue;
            }
            // Tombstone: mark key as visited and skip
            if self.inner.value().is_empty() {
                self.prev_key.clear();
                self.prev_key.extend_from_slice(self.inner.key().key_ref());
                self.inner.next()?;
                continue;
            }
            // Valid non-tombstone entry for a new key
            break;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.in_range() && !self.inner.value().is_empty()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }

    fn next(&mut self) -> Result<()> {
        // Mark current key as seen so we don't emit older versions
        self.prev_key.clear();
        self.prev_key.extend_from_slice(self.inner.key().key_ref());
        self.inner.next()?;
        self.move_to_key()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::anyhow!("iterator has errored"));
        }
        if self.iter.is_valid()
            && let Err(e) = self.iter.next()
        {
            self.has_errored = true;
            return Err(e);
        }
        Ok(())
    }
}
