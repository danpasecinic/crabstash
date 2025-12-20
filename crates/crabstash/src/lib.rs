use bytes::Bytes;
use crabstash_common::Result;
use crabstash_storage::{Lsm, LsmOptions};
use crabstash_txn::{IsolationLevel, LsmIterator, MvccEngine, Transaction};
use parking_lot::Mutex;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

pub use crabstash_common::Error as DbError;
pub use crabstash_txn::IsolationLevel as Isolation;

pub struct DbOptions {
    pub memtable_size: usize,
    pub sync_writes: bool,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
            sync_writes: false,
        }
    }
}

pub struct Db {
    engine: MvccEngine,
}

impl Db {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, DbOptions::default())
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: DbOptions) -> Result<Self> {
        let lsm_options = LsmOptions {
            memtable_size: options.memtable_size,
            ..Default::default()
        };

        let lsm = Arc::new(Lsm::open(path, lsm_options)?);
        let engine = MvccEngine::new(lsm);

        Ok(Self { engine })
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let txn = self.engine.begin();
        let result = self.engine.get(&txn, key.as_ref())?;
        self.engine.commit(&txn)?;
        Ok(result.map(|b| b.to_vec()))
    }

    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let txn = self.engine.begin();
        self.engine.put(&txn, key, value);
        self.engine.commit(&txn)
    }

    pub fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let txn = self.engine.begin();
        self.engine.delete(&txn, key);
        self.engine.commit(&txn)
    }

    pub fn begin(&self) -> Txn<'_> {
        Txn {
            inner: self.engine.begin(),
            engine: &self.engine,
        }
    }

    pub fn begin_with_isolation(&self, isolation: Isolation) -> Txn<'_> {
        let level = match isolation {
            Isolation::Snapshot => IsolationLevel::Snapshot,
            Isolation::Serializable => IsolationLevel::Serializable,
        };
        Txn {
            inner: self.engine.begin_with_isolation(level),
            engine: &self.engine,
        }
    }

    pub fn sync(&self) -> Result<()> {
        self.engine.storage().sync()
    }

    pub fn scan(&self) -> Result<DbIterator> {
        let inner = self.engine.scan()?;
        Ok(DbIterator { inner })
    }

    pub fn scan_range<K: AsRef<[u8]>>(
        &self,
        start: Bound<K>,
        end: Bound<K>,
    ) -> Result<DbIterator> {
        let inner = self.engine.scan_range(start, end)?;
        Ok(DbIterator { inner })
    }

    pub fn prefix_scan(&self, prefix: impl AsRef<[u8]>) -> Result<DbIterator> {
        let prefix = prefix.as_ref();
        let start = prefix.to_vec();
        let end = prefix_end_bound(prefix);
        let inner = self.engine.scan_range(Bound::Included(start), end)?;
        Ok(DbIterator { inner })
    }
}

fn prefix_end_bound(prefix: &[u8]) -> Bound<Vec<u8>> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            end.truncate(i + 1);
            return Bound::Excluded(end);
        }
    }
    Bound::Unbounded
}

pub struct Txn<'a> {
    inner: Arc<Mutex<Transaction>>,
    engine: &'a MvccEngine,
}

impl<'a> Txn<'a> {
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let result = self.engine.get(&self.inner, key.as_ref())?;
        Ok(result.map(|b| b.to_vec()))
    }

    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.engine.put(&self.inner, key, value);
    }

    pub fn delete(&self, key: impl Into<Bytes>) {
        self.engine.delete(&self.inner, key);
    }

    pub fn commit(self) -> Result<()> {
        self.engine.commit(&self.inner)
    }

    pub fn abort(self) {
        self.engine.abort(&self.inner);
    }
}

pub struct DbIterator {
    inner: LsmIterator,
}

impl DbIterator {
    pub fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value().map(|b| b.as_ref())
    }

    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    pub fn advance(&mut self) -> Result<()> {
        self.inner.advance()
    }
}

impl Iterator for DbIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.inner.is_valid() {
            return None;
        }

        let key = self.inner.key()?.to_vec();
        let value = self.inner.value()?.to_vec();

        if let Err(e) = self.inner.advance() {
            return Some(Err(e));
        }

        Some(Ok((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn assert_send<T: Send>() {}
    const fn assert_sync<T: Sync>() {}

    #[allow(dead_code)]
    const _: () = {
        assert_send::<Db>();
        assert_sync::<Db>();
    };
}
