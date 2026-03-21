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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use farmhash::fingerprint32;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
    mvcc::CommittedTxnData,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().1.insert(fingerprint32(key));
        }
        if let Some(value) = self.local_storage.get(key) {
            if value.value().is_empty() {
                return Ok(None);
            }
            return Ok(Some(value.value().clone()));
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let local_lower = map_bound(lower);
        let local_upper = map_bound(upper);
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((local_lower, local_upper)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        local_iter.next()?;
        let iter = TwoMergeIterator::create(
            local_iter,
            self.inner.scan_with_ts(lower, upper, self.read_ts)?,
        )?;
        TxnIterator::create(self.clone(), iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(fingerprint32(key));
        }
    }

    pub fn delete(&self, key: &[u8]) {
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(fingerprint32(key));
        }
    }

    pub fn commit(&self) -> Result<()> {
        if self
            .committed
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(anyhow!("transaction already committed"));
        }

        let mut batch = Vec::<WriteBatchRecord<Bytes>>::new();
        for entry in self.local_storage.iter() {
            if entry.value().is_empty() {
                batch.push(WriteBatchRecord::Del(entry.key().clone()));
            } else {
                batch.push(WriteBatchRecord::Put(
                    entry.key().clone(),
                    entry.value().clone(),
                ));
            }
        }

        if batch.is_empty() {
            self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
            return Ok(());
        }

        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        let serializable_meta = self.key_hashes.as_ref().map(|k| k.lock());
        if let Some(meta) = &serializable_meta {
            let read_set = &meta.1;
            let committed_txns = self.inner.mvcc().committed_txns.lock();
            for (_, txn_data) in
                committed_txns.range((Bound::Excluded(self.read_ts), Bound::Unbounded))
            {
                if txn_data
                    .key_hashes
                    .iter()
                    .any(|hash| read_set.contains(hash))
                {
                    self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
                    return Err(anyhow!("serializable check failed"));
                }
            }
        }

        let commit_ts = self.inner.write_batch_inner(&batch)?;

        if let Some(meta) = serializable_meta {
            let write_set = meta.0.clone();
            if !write_set.is_empty() {
                let mut committed_txns = self.inner.mvcc().committed_txns.lock();
                committed_txns.insert(
                    commit_ts,
                    CommittedTxnData {
                        key_hashes: write_set,
                        read_ts: self.read_ts,
                        commit_ts,
                    },
                );
                let watermark = self.inner.mvcc().watermark();
                committed_txns.retain(|ts, _| *ts >= watermark);
            }
        }

        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
        }
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next_item =
            self.with_iter_mut(|it| it.next().map(|e| (e.key().clone(), e.value().clone())));
        self.with_item_mut(|item| {
            *item = next_item.unwrap_or((Bytes::new(), Bytes::new()));
        });
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { _txn: txn, iter };
        while iter.iter.is_valid() && iter.iter.value().is_empty() {
            iter.iter.next()?;
        }
        if iter.iter.is_valid()
            && let Some(key_hashes) = &iter._txn.key_hashes
        {
            key_hashes.lock().1.insert(fingerprint32(iter.iter.key()));
        }
        Ok(iter)
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        if self.iter.is_valid()
            && let Some(key_hashes) = &self._txn.key_hashes
        {
            key_hashes.lock().1.insert(fingerprint32(self.iter.key()));
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
