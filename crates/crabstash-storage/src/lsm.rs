use crate::batch::{BatchOperation, WriteBatch};
use crate::cache::BlockCache;
use crate::compaction::Compactor;
use crate::iterator::{BoundedIterator, MergeIterator, StorageIterator, TwoMergeIterator};
use crate::manifest::Manifest;
use crate::memtable::{MemTable, MemTableIterator};
use crate::sstable::{CompressionType, SSTable, SSTableBuilder, SSTableIterator};
use crate::wal::{RecordType, Wal, WalRecord};
use bytes::Bytes;
use crabstash_common::{Key, Result};
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::{debug, info, instrument};

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entry_count: u64,
    pub weighted_size: u64,
    pub capacity: u64,
}

pub struct LsmOptions {
    pub memtable_size: usize,
    pub block_size: usize,
    pub bloom_fp_rate: f64,
    pub block_cache_capacity: u64,
    pub compression: CompressionType,
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
            block_size: 4096,
            bloom_fp_rate: 0.01,
            block_cache_capacity: 64 * 1024 * 1024,
            compression: CompressionType::Lz4,
        }
    }
}

struct LsmInner {
    memtable: MemTable,
    immutable_memtables: Vec<Arc<MemTable>>,
    levels: HashMap<u32, Vec<SSTable>>,
    wal: Wal,
    manifest: Manifest,
}

struct CompactionState {
    shutdown: AtomicBool,
    work_available: Mutex<bool>,
    condvar: Condvar,
}

pub struct Lsm {
    inner: RwLock<LsmInner>,
    dir: PathBuf,
    options: LsmOptions,
    next_ts: AtomicU64,
    block_cache: Arc<BlockCache>,
    compaction_state: Arc<CompactionState>,
    compaction_thread: Mutex<Option<JoinHandle<()>>>,
}

impl Lsm {
    #[instrument(skip(options), fields(dir = %dir.as_ref().display()))]
    pub fn open(dir: impl AsRef<Path>, options: LsmOptions) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let block_cache = Arc::new(BlockCache::new(options.block_cache_capacity));

        let manifest_path = dir.join("MANIFEST");
        let manifest = if manifest_path.exists() {
            Manifest::open(&manifest_path)?
        } else {
            Manifest::create(&manifest_path)?
        };

        let mut levels: HashMap<u32, Vec<SSTable>> = HashMap::new();
        for (level, meta) in &manifest.levels {
            let mut ssts = Vec::new();
            for &id in &meta.sst_ids {
                let path = dir.join(format!("{:06}.sst", id));
                if path.exists() {
                    ssts.push(SSTable::open_with_cache(
                        id,
                        path,
                        Some(block_cache.clone()),
                    )?);
                }
            }
            levels.insert(*level, ssts);
        }

        let wal_path = dir.join(format!("{:06}.wal", manifest.next_wal_id));
        let wal = Wal::create(&wal_path)?;

        let memtable = MemTable::new();

        for wal_id in 1..manifest.next_wal_id {
            let old_wal_path = dir.join(format!("{:06}.wal", wal_id));
            if old_wal_path.exists() {
                let records = Wal::recover(&old_wal_path)?;
                for record in records {
                    let key = Key::new(record.key, record.timestamp);
                    match record.record_type {
                        RecordType::Put => {
                            memtable.put(key, record.value.unwrap_or_default());
                        }
                        RecordType::Delete => {
                            memtable.delete(key);
                        }
                    }
                }
            }
        }

        let inner = LsmInner {
            memtable,
            immutable_memtables: Vec::new(),
            levels,
            wal,
            manifest,
        };

        let compaction_state = Arc::new(CompactionState {
            shutdown: AtomicBool::new(false),
            work_available: Mutex::new(false),
            condvar: Condvar::new(),
        });

        let lsm = Self {
            inner: RwLock::new(inner),
            dir: dir.clone(),
            options,
            next_ts: AtomicU64::new(1),
            block_cache,
            compaction_state: compaction_state.clone(),
            compaction_thread: Mutex::new(None),
        };

        let compactor = Compactor::new(&dir);

        let state = compaction_state;
        let compaction_dir = dir;
        let handle = thread::spawn(move || {
            Self::compaction_loop(state, compactor, compaction_dir);
        });

        *lsm.compaction_thread.lock() = Some(handle);

        Ok(lsm)
    }

    fn compaction_loop(state: Arc<CompactionState>, compactor: Compactor, dir: PathBuf) {
        loop {
            let mut work = state.work_available.lock();
            let result = state
                .condvar
                .wait_for(&mut work, Duration::from_millis(1000));

            if state.shutdown.load(Ordering::Relaxed) {
                break;
            }

            if result.timed_out() && !*work {
                continue;
            }

            *work = false;
            drop(work);

            let manifest_path = dir.join("MANIFEST");
            let manifest = match crate::manifest::Manifest::open(&manifest_path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            if let Some(task) = compactor.pick_compaction(&manifest) {
                let mut manifest = match crate::manifest::Manifest::open(&manifest_path) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                if let Err(e) = compactor.compact(&task, &mut manifest) {
                    eprintln!("Compaction error: {:?}", e);
                }
            }
        }
    }

    fn notify_compaction(&self) {
        let mut work = self.compaction_state.work_available.lock();
        *work = true;
        self.compaction_state.condvar.notify_one();
    }

    #[instrument(skip(self, key), fields(key_len = key.len()))]
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let ts = self.next_ts.load(Ordering::Relaxed);
        let search_key = Key::new(Bytes::copy_from_slice(key), ts);

        let inner = self.inner.read();

        if let Some(value) = inner.memtable.get(&search_key) {
            debug!("found in memtable");
            return Ok(value);
        }

        for imm in inner.immutable_memtables.iter().rev() {
            if let Some(value) = imm.get(&search_key) {
                debug!("found in immutable memtable");
                return Ok(value);
            }
        }

        drop(inner);

        for level in 0..7 {
            let mut inner = self.inner.write();
            if let Some(ssts) = inner.levels.get_mut(&level) {
                for sst in ssts.iter_mut().rev() {
                    if let Some(value) = sst.get(key)? {
                        debug!(level, "found in SSTable");
                        return Ok(Some(value));
                    }
                }
            }
        }

        debug!("key not found");
        Ok(None)
    }

    #[instrument(skip(self, key, value), fields(key_len = key.as_ref().len(), value_len = value.as_ref().len()))]
    pub fn put(
        &self,
        key: impl AsRef<[u8]> + Into<Bytes>,
        value: impl AsRef<[u8]> + Into<Bytes>,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();
        let ts = self.next_ts.fetch_add(1, Ordering::Relaxed);

        let mut inner = self.inner.write();

        let record = WalRecord {
            record_type: RecordType::Put,
            key: key.clone(),
            value: Some(value.clone()),
            timestamp: ts,
        };
        inner.wal.append(&record)?;

        let key = Key::new(key, ts);
        inner.memtable.put(key, value);

        if inner.memtable.size() >= self.options.memtable_size {
            info!("memtable full, rotating");
            self.rotate_memtable(&mut inner)?;
        }

        Ok(())
    }

    #[instrument(skip(self, key), fields(key_len = key.as_ref().len()))]
    pub fn delete(&self, key: impl AsRef<[u8]> + Into<Bytes>) -> Result<()> {
        let key = key.into();
        let ts = self.next_ts.fetch_add(1, Ordering::Relaxed);

        let mut inner = self.inner.write();

        let record = WalRecord {
            record_type: RecordType::Delete,
            key: key.clone(),
            value: None,
            timestamp: ts,
        };
        inner.wal.append(&record)?;

        let key = Key::new(key, ts);
        inner.memtable.delete(key);

        Ok(())
    }

    #[instrument(skip(self, batch), fields(batch_size = batch.len()))]
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let operations = batch.into_operations();
        let base_ts = self
            .next_ts
            .fetch_add(operations.len() as u64, Ordering::Relaxed);

        let wal_records: Vec<WalRecord> = operations
            .iter()
            .enumerate()
            .map(|(i, op)| {
                let ts = base_ts + i as u64;
                match op {
                    BatchOperation::Put { key, value } => WalRecord {
                        record_type: RecordType::Put,
                        key: key.clone(),
                        value: Some(value.clone()),
                        timestamp: ts,
                    },
                    BatchOperation::Delete { key } => WalRecord {
                        record_type: RecordType::Delete,
                        key: key.clone(),
                        value: None,
                        timestamp: ts,
                    },
                }
            })
            .collect();

        let mut inner = self.inner.write();
        inner.wal.append_batch(&wal_records)?;

        for (i, op) in operations.into_iter().enumerate() {
            let ts = base_ts + i as u64;
            match op {
                BatchOperation::Put { key, value } => {
                    inner.memtable.put(Key::new(key, ts), value);
                }
                BatchOperation::Delete { key } => {
                    inner.memtable.delete(Key::new(key, ts));
                }
            }
        }

        if inner.memtable.size() >= self.options.memtable_size {
            info!("memtable full, rotating");
            self.rotate_memtable(&mut inner)?;
        }

        Ok(())
    }

    #[instrument(skip(self, inner))]
    fn rotate_memtable(&self, inner: &mut LsmInner) -> Result<()> {
        let old_memtable = std::mem::take(&mut inner.memtable);
        let imm = Arc::new(old_memtable);
        inner.immutable_memtables.push(imm.clone());

        inner.wal.sync()?;
        let wal_id = inner.manifest.allocate_wal_id();
        inner.manifest.add_wal(wal_id)?;
        let new_wal_path = self.dir.join(format!("{:06}.wal", wal_id));
        inner.wal = Wal::create(&new_wal_path)?;

        self.flush_immutable(inner, imm)?;

        Ok(())
    }

    #[instrument(skip(self, inner, imm), fields(entries = imm.len()))]
    fn flush_immutable(&self, inner: &mut LsmInner, imm: Arc<MemTable>) -> Result<()> {
        let sst_id = inner.manifest.allocate_sst_id();
        let mut builder = SSTableBuilder::new_with_compression(
            sst_id,
            &self.dir,
            imm.len(),
            self.options.compression,
        )?;

        for (key, value) in imm.iter() {
            builder.add(&key, value.as_ref())?;
        }

        builder.finish()?;
        inner.manifest.add_sst(0, sst_id)?;

        let sst = SSTable::open_with_cache(
            sst_id,
            self.dir.join(format!("{:06}.sst", sst_id)),
            Some(self.block_cache.clone()),
        )?;
        inner.levels.entry(0).or_default().push(sst);

        inner.immutable_memtables.retain(|m| !Arc::ptr_eq(m, &imm));

        self.notify_compaction();

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.write();
        inner.wal.sync()?;
        Ok(())
    }

    pub fn block_cache(&self) -> &Arc<BlockCache> {
        &self.block_cache
    }

    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.block_cache.entry_count(),
            weighted_size: self.block_cache.weighted_size(),
            capacity: self.options.block_cache_capacity,
        }
    }

    pub fn scan(&self) -> Result<LsmIterator> {
        self.scan_range::<&[u8]>(Bound::Unbounded, Bound::Unbounded)
    }

    pub fn scan_range<K: AsRef<[u8]>>(
        &self,
        start: Bound<K>,
        end: Bound<K>,
    ) -> Result<LsmIterator> {
        let inner = self.inner.read();

        let start_key = match &start {
            Bound::Unbounded => None,
            Bound::Included(k) => Some(k.as_ref()),
            Bound::Excluded(k) => Some(k.as_ref()),
        };

        let end_bound = match end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(k) => Bound::Included(Bytes::copy_from_slice(k.as_ref())),
            Bound::Excluded(k) => Bound::Excluded(Bytes::copy_from_slice(k.as_ref())),
        };

        let mut memtable_iter = MemTableIterator::new(&inner.memtable);
        if let Some(key) = start_key {
            memtable_iter.seek(key);
        }

        let mut imm_iters: Vec<MemTableIterator> = inner
            .immutable_memtables
            .iter()
            .map(|m| {
                let mut iter = MemTableIterator::from_arc(m);
                if let Some(key) = start_key {
                    iter.seek(key);
                }
                iter
            })
            .collect();
        imm_iters.reverse();

        let imm_merge = MergeIterator::new(imm_iters);
        let mem_merge = TwoMergeIterator::new(memtable_iter, imm_merge);

        let mut l0_iters: Vec<SSTableIterator> = Vec::new();
        if let Some(l0_ssts) = inner.levels.get(&0) {
            for sst in l0_ssts.iter().rev() {
                let sst_arc = Arc::new(SSTable::open_with_cache(
                    sst.id,
                    sst.path(),
                    Some(self.block_cache.clone()),
                )?);
                let mut iter = SSTableIterator::new(sst_arc)?;
                if let Some(key) = start_key {
                    iter.seek(key)?;
                }
                l0_iters.push(iter);
            }
        }
        let l0_merge = MergeIterator::new(l0_iters);

        let mut level_iters: Vec<SSTableIterator> = Vec::new();
        for level in 1..7 {
            if let Some(ssts) = inner.levels.get(&level) {
                for sst in ssts {
                    let sst_arc = Arc::new(SSTable::open_with_cache(
                        sst.id,
                        sst.path(),
                        Some(self.block_cache.clone()),
                    )?);
                    let mut iter = SSTableIterator::new(sst_arc)?;
                    if let Some(key) = start_key {
                        iter.seek(key)?;
                    }
                    level_iters.push(iter);
                }
            }
        }
        let levels_merge = MergeIterator::new(level_iters);

        let sst_merge = TwoMergeIterator::new(l0_merge, levels_merge);
        let full_iter = TwoMergeIterator::new(mem_merge, sst_merge);

        let bounded = BoundedIterator::new(full_iter, end_bound);

        let mut iter = LsmIterator { inner: bounded };
        if let Bound::Excluded(k) = &start {
            while iter.is_valid() && iter.inner.key().data() == k.as_ref() {
                iter.inner.next()?;
            }
        }

        Ok(iter)
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        self.compaction_state
            .shutdown
            .store(true, Ordering::Relaxed);
        self.compaction_state.condvar.notify_one();

        if let Some(handle) = self.compaction_thread.lock().take() {
            let _ = handle.join();
        }
    }
}

type InnerLsmIterator = BoundedIterator<
    TwoMergeIterator<
        TwoMergeIterator<MemTableIterator, MergeIterator<MemTableIterator>>,
        TwoMergeIterator<MergeIterator<SSTableIterator>, MergeIterator<SSTableIterator>>,
    >,
>;

pub struct LsmIterator {
    inner: InnerLsmIterator,
}

impl LsmIterator {
    pub fn key(&self) -> Option<&[u8]> {
        if self.is_valid() {
            Some(self.inner.key().data())
        } else {
            None
        }
    }

    pub fn value(&self) -> Option<&Bytes> {
        if self.is_valid() {
            self.inner.value()
        } else {
            None
        }
    }

    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    pub fn advance(&mut self) -> Result<()> {
        self.inner.next()
    }
}
