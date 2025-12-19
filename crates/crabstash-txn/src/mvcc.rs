use bytes::Bytes;
use crabstash_common::Result;
use crabstash_storage::{Lsm, LsmIterator};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::timestamp::TimestampOracle;
use crate::transaction::{IsolationLevel, Transaction, TransactionManager};

pub struct MvccEngine {
    storage: Arc<Lsm>,
    ts_oracle: TimestampOracle,
    txn_manager: TransactionManager,
    next_txn_id: AtomicU64,
}

impl MvccEngine {
    pub fn new(storage: Arc<Lsm>) -> Self {
        Self {
            storage,
            ts_oracle: TimestampOracle::new(),
            txn_manager: TransactionManager::new(),
            next_txn_id: AtomicU64::new(1),
        }
    }

    pub fn begin(&self) -> Arc<Mutex<Transaction>> {
        self.begin_with_isolation(IsolationLevel::Snapshot)
    }

    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> Arc<Mutex<Transaction>> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        let start_ts = self.ts_oracle.get_timestamp();
        self.txn_manager.begin(txn_id, start_ts, isolation)
    }

    pub fn get(&self, txn: &Arc<Mutex<Transaction>>, key: &[u8]) -> Result<Option<Bytes>> {
        let mut txn_guard = txn.lock();

        if let Some(local_value) = txn_guard.write_set.get(key) {
            return Ok(local_value.cloned());
        }

        txn_guard.record_read(Bytes::copy_from_slice(key));
        drop(txn_guard);

        self.storage.get(key)
    }

    pub fn put(
        &self,
        txn: &Arc<Mutex<Transaction>>,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) {
        let mut txn_guard = txn.lock();
        txn_guard.write_set.put(key.into(), value.into());
    }

    pub fn delete(&self, txn: &Arc<Mutex<Transaction>>, key: impl Into<Bytes>) {
        let mut txn_guard = txn.lock();
        txn_guard.write_set.delete(key.into());
    }

    pub fn commit(&self, txn: &Arc<Mutex<Transaction>>) -> Result<()> {
        let mut txn_guard = txn.lock();
        let commit_ts = self.ts_oracle.get_timestamp();

        self.txn_manager.prepare_commit(&mut txn_guard, commit_ts)?;

        for (key, value) in txn_guard.write_set.iter_puts() {
            self.storage.put(key.clone(), value.clone())?;
        }

        for key in txn_guard.write_set.iter_deletes() {
            self.storage.delete(key.clone())?;
        }

        self.txn_manager.commit(&mut txn_guard)?;
        Ok(())
    }

    pub fn abort(&self, txn: &Arc<Mutex<Transaction>>) {
        let mut txn_guard = txn.lock();
        self.txn_manager.abort(&mut txn_guard);
    }

    pub fn storage(&self) -> &Arc<Lsm> {
        &self.storage
    }

    pub fn scan(&self) -> Result<LsmIterator> {
        self.storage.scan()
    }
}
