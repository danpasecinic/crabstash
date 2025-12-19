use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crabstash_common::Key;
use std::sync::atomic::{AtomicUsize, Ordering};

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
        self.map.iter().map(|entry| (entry.key().clone(), entry.value().clone()))
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
