use bytes::Bytes;
use moka::sync::Cache;
use std::sync::Arc;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct BlockCacheKey {
    pub sst_id: u64,
    pub block_idx: usize,
}

impl BlockCacheKey {
    pub fn new(sst_id: u64, block_idx: usize) -> Self {
        Self { sst_id, block_idx }
    }
}

#[derive(Clone)]
pub struct BlockCache {
    inner: Cache<BlockCacheKey, Arc<Bytes>>,
}

impl BlockCache {
    pub fn new(capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(capacity_bytes)
            .weigher(|_key: &BlockCacheKey, value: &Arc<Bytes>| -> u32 {
                value.len().try_into().unwrap_or(u32::MAX)
            })
            .build();

        Self { inner: cache }
    }

    pub fn get(&self, key: &BlockCacheKey) -> Option<Arc<Bytes>> {
        self.inner.get(key)
    }

    pub fn insert(&self, key: BlockCacheKey, block: Bytes) {
        self.inner.insert(key, Arc::new(block));
    }

    pub fn invalidate_sst(&self, sst_id: u64) {
        self.inner
            .invalidate_entries_if(move |k, _| k.sst_id == sst_id)
            .expect("invalidation should not fail");
    }

    pub fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }

    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}
