use crate::memtable::MemTable;
use crate::sstable::{SSTable, SSTableBuilder};
use crate::wal::{Wal, WalRecord, RecordType};
use crate::manifest::Manifest;
use crate::compaction::Compactor;
use bytes::Bytes;
use crabstash_common::{Key, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct LsmOptions {
    pub memtable_size: usize,
    pub block_size: usize,
    pub bloom_fp_rate: f64,
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
            block_size: 4096,
            bloom_fp_rate: 0.01,
        }
    }
}

struct LsmInner {
    memtable: MemTable,
    immutable_memtables: Vec<Arc<MemTable>>,
    levels: HashMap<u32, Vec<SSTable>>,
    wal: Wal,
    manifest: Manifest,
    #[allow(dead_code)]
    compactor: Compactor,
}

pub struct Lsm {
    inner: RwLock<LsmInner>,
    dir: PathBuf,
    options: LsmOptions,
    next_ts: AtomicU64,
}

impl Lsm {
    pub fn open(dir: impl AsRef<Path>, options: LsmOptions) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

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
                    ssts.push(SSTable::open(id, path)?);
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

        let compactor = Compactor::new(&dir);

        let inner = LsmInner {
            memtable,
            immutable_memtables: Vec::new(),
            levels,
            wal,
            manifest,
            compactor,
        };

        Ok(Self {
            inner: RwLock::new(inner),
            dir,
            options,
            next_ts: AtomicU64::new(1),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let ts = self.next_ts.load(Ordering::Relaxed);
        let search_key = Key::new(Bytes::copy_from_slice(key), ts);

        let inner = self.inner.read();

        if let Some(value) = inner.memtable.get(&search_key) {
            return Ok(value);
        }

        for imm in inner.immutable_memtables.iter().rev() {
            if let Some(value) = imm.get(&search_key) {
                return Ok(value);
            }
        }

        drop(inner);

        for level in 0..7 {
            let mut inner = self.inner.write();
            if let Some(ssts) = inner.levels.get_mut(&level) {
                for sst in ssts.iter_mut().rev() {
                    if let Some(value) = sst.get(key)? {
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
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
            self.rotate_memtable(&mut inner)?;
        }

        Ok(())
    }

    pub fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
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

    fn flush_immutable(&self, inner: &mut LsmInner, imm: Arc<MemTable>) -> Result<()> {
        let sst_id = inner.manifest.allocate_sst_id();
        let mut builder = SSTableBuilder::new(sst_id, &self.dir, imm.len())?;

        for (key, value) in imm.iter() {
            builder.add(&key, value.as_ref())?;
        }

        builder.finish()?;
        inner.manifest.add_sst(0, sst_id)?;

        let sst = SSTable::open(sst_id, self.dir.join(format!("{:06}.sst", sst_id)))?;
        inner.levels.entry(0).or_default().push(sst);

        inner.immutable_memtables.retain(|m| !Arc::ptr_eq(m, &imm));

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.write();
        inner.wal.sync()?;
        Ok(())
    }
}
