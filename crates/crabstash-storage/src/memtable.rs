use bytes::Bytes;
use crabstash_common::{Key, Result};
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::iterator::StorageIterator;

pub struct MemTable {
    map: SkipMap<Key, Option<Bytes>>,
    size: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicUsize::new(0),
        }
    }

    pub fn get(&self, key: &Key) -> Option<Option<Bytes>> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub fn get_at_ts(&self, key_data: &[u8], ts: u64) -> Option<Option<Bytes>> {
        let search_key = Key::new(Bytes::copy_from_slice(key_data), ts);
        for entry in self.map.range(search_key..) {
            let entry_key = entry.key();
            if entry_key.data() != key_data {
                return None;
            }
            if entry_key.timestamp() <= ts {
                return Some(entry.value().clone());
            }
        }
        None
    }

    pub fn put(&self, key: Key, value: Bytes) {
        let size_delta = key.data().len() + value.len();
        self.map.insert(key, Some(value));
        self.size.fetch_add(size_delta, Ordering::Relaxed);
    }

    pub fn delete(&self, key: Key) {
        let size_delta = key.data().len();
        self.map.insert(key, None);
        self.size.fetch_add(size_delta, Ordering::Relaxed);
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn iter(&self) -> impl Iterator<Item = (Key, Option<Bytes>)> + '_ {
        self.map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MemTableIterator {
    entries: Vec<(Key, Option<Bytes>)>,
    index: usize,
}

impl MemTableIterator {
    pub fn new(memtable: &MemTable) -> Self {
        let entries: Vec<_> = memtable
            .map
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        Self { entries, index: 0 }
    }

    pub fn from_arc(memtable: &Arc<MemTable>) -> Self {
        Self::new(memtable.as_ref())
    }

    pub fn seek(&mut self, target: &[u8]) {
        self.index = self
            .entries
            .binary_search_by(|(key, _)| key.data().cmp(target))
            .unwrap_or_else(|idx| idx);
    }

    pub fn seek_to_first(&mut self) {
        self.index = 0;
    }
}

impl StorageIterator for MemTableIterator {
    fn key(&self) -> &Key {
        &self.entries[self.index].0
    }

    fn value(&self) -> Option<&Bytes> {
        self.entries[self.index].1.as_ref()
    }

    fn is_valid(&self) -> bool {
        self.index < self.entries.len()
    }

    fn next(&mut self) -> Result<()> {
        self.index += 1;
        Ok(())
    }
}
