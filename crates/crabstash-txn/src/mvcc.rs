use bytes::Bytes;
use crabstash_common::{Error, Result};
use crabstash_lock::LockError;
use crabstash_lock::{LockConfig, LockManager, LockMode, SSIConflict, SSIManager};
use crabstash_storage::{Lsm, LsmIterator};
use parking_lot::Mutex;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, instrument};

use crate::timestamp::TimestampOracle;
use crate::transaction::{IsolationLevel, Transaction, TransactionManager};

pub struct MvccEngine {
    storage: Arc<Lsm>,
    ts_oracle: TimestampOracle,
    txn_manager: TransactionManager,
    next_txn_id: AtomicU64,
    lock_manager: LockManager,
    lock_timeout: Duration,
    ssi_manager: SSIManager,
}

impl MvccEngine {
    pub fn new(storage: Arc<Lsm>) -> Self {
        Self::with_lock_config(storage, LockConfig::default())
    }

    pub fn with_lock_config(storage: Arc<Lsm>, lock_config: LockConfig) -> Self {
        let lock_timeout = Duration::from_millis(lock_config.lock_timeout_ms);
        Self {
            storage,
            ts_oracle: TimestampOracle::new(),
            txn_manager: TransactionManager::new(),
            next_txn_id: AtomicU64::new(1),
            lock_manager: LockManager::new(lock_config),
            lock_timeout,
            ssi_manager: SSIManager::new(),
        }
    }

    pub fn begin(&self) -> Arc<Mutex<Transaction>> {
        self.begin_with_isolation(IsolationLevel::Snapshot)
    }

    #[instrument(skip(self))]
    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> Arc<Mutex<Transaction>> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        let start_ts = self.ts_oracle.get_timestamp();
        self.lock_manager.register_txn(txn_id, start_ts);

        if isolation == IsolationLevel::Serializable {
            self.ssi_manager.begin_txn(txn_id);
        }

        debug!(txn_id, start_ts, "transaction started");
        self.txn_manager.begin(txn_id, start_ts, isolation)
    }

    pub fn get(&self, txn: &Arc<Mutex<Transaction>>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn_guard = txn.lock();
        let txn_id = txn_guard.id;
        let isolation = txn_guard.isolation;
        let start_ts = txn_guard.start_ts;

        if let Some(local_value) = txn_guard.write_set.get(key) {
            return Ok(local_value.cloned());
        }
        drop(txn_guard);

        if isolation == IsolationLevel::Serializable {
            self.ssi_manager.record_read(txn_id, key);
        }

        let mut txn_guard = txn.lock();
        txn_guard.record_read(Bytes::copy_from_slice(key));
        drop(txn_guard);

        self.storage.get_at_ts(key, start_ts)
    }

    pub fn put(
        &self,
        txn: &Arc<Mutex<Transaction>>,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();

        let txn_guard = txn.lock();
        let txn_id = txn_guard.id;
        let isolation = txn_guard.isolation;
        drop(txn_guard);

        self.lock_manager
            .lock_key(txn_id, &key, LockMode::X, Some(self.lock_timeout))
            .map_err(lock_error_to_common)?;

        if isolation == IsolationLevel::Serializable {
            self.ssi_manager.record_write(txn_id, &key);
        }

        let mut txn_guard = txn.lock();
        txn_guard.write_set.put(key, value);
        Ok(())
    }

    pub fn delete(&self, txn: &Arc<Mutex<Transaction>>, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();

        let txn_guard = txn.lock();
        let txn_id = txn_guard.id;
        let isolation = txn_guard.isolation;
        drop(txn_guard);

        self.lock_manager
            .lock_key(txn_id, &key, LockMode::X, Some(self.lock_timeout))
            .map_err(lock_error_to_common)?;

        if isolation == IsolationLevel::Serializable {
            self.ssi_manager.record_write(txn_id, &key);
        }

        let mut txn_guard = txn.lock();
        txn_guard.write_set.delete(key);
        Ok(())
    }

    #[instrument(skip(self, txn))]
    pub fn commit(&self, txn: &Arc<Mutex<Transaction>>) -> Result<()> {
        let mut txn_guard = txn.lock();
        let txn_id = txn_guard.id;
        let start_ts = txn_guard.start_ts;
        let isolation = txn_guard.isolation;
        let commit_ts = self.ts_oracle.get_timestamp();

        if isolation == IsolationLevel::Serializable {
            self.ssi_manager
                .validate_and_commit(txn_id, start_ts, commit_ts)
                .map_err(ssi_error_to_common)?;
        }

        self.txn_manager.prepare_commit(&mut txn_guard, commit_ts)?;

        for (key, value) in txn_guard.write_set.iter_puts() {
            self.storage.put(key.clone(), value.clone())?;
        }

        for key in txn_guard.write_set.iter_deletes() {
            self.storage.delete(key.clone())?;
        }

        self.txn_manager.commit(&mut txn_guard)?;
        drop(txn_guard);

        self.lock_manager.release_all(txn_id);
        debug!(commit_ts, "transaction committed");
        Ok(())
    }

    #[instrument(skip(self, txn))]
    pub fn abort(&self, txn: &Arc<Mutex<Transaction>>) {
        let mut txn_guard = txn.lock();
        let txn_id = txn_guard.id;
        let isolation = txn_guard.isolation;
        debug!("transaction aborted");
        self.txn_manager.abort(&mut txn_guard);
        drop(txn_guard);

        if isolation == IsolationLevel::Serializable {
            self.ssi_manager.abort_txn(txn_id);
        }
        self.lock_manager.release_all(txn_id);
    }

    pub fn storage(&self) -> &Arc<Lsm> {
        &self.storage
    }

    pub fn scan(&self) -> Result<LsmIterator> {
        self.storage.scan()
    }

    pub fn scan_range<K: AsRef<[u8]>>(
        &self,
        start: Bound<K>,
        end: Bound<K>,
    ) -> Result<LsmIterator> {
        self.storage.scan_range(start, end)
    }
}

fn lock_error_to_common(err: LockError) -> Error {
    match err {
        LockError::Timeout => Error::LockTimeout,
        LockError::Deadlock => Error::Deadlock,
        LockError::TransactionAborted => Error::TransactionAborted,
        LockError::LockEscalationFailed
        | LockError::RangeConflict
        | LockError::RangeLocksDisabled
        | LockError::NotHeld
        | LockError::InvalidUpgrade => Error::LockConflict,
    }
}

fn ssi_error_to_common(err: SSIConflict) -> Error {
    match err {
        SSIConflict::WriteSkew { .. } => Error::WriteSkew,
        SSIConflict::Phantom { .. } => Error::PhantomRead,
        SSIConflict::DangerousStructure { .. } => Error::SerializableConflict,
        SSIConflict::TxnNotFound => Error::TransactionAborted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabstash_storage::LsmOptions;

    fn create_test_engine() -> MvccEngine {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Lsm::open(dir.path(), LsmOptions::default()).unwrap());
        MvccEngine::new(storage)
    }

    #[test]
    fn test_serializable_write_skew_detection() {
        let engine = create_test_engine();

        let setup_txn = engine.begin();
        engine.put(&setup_txn, "account_a", "100").unwrap();
        engine.put(&setup_txn, "account_b", "100").unwrap();
        engine.commit(&setup_txn).unwrap();

        let txn1 = engine.begin_with_isolation(IsolationLevel::Serializable);
        let txn2 = engine.begin_with_isolation(IsolationLevel::Serializable);

        engine.get(&txn1, b"account_a").unwrap();
        engine.get(&txn1, b"account_b").unwrap();
        engine.get(&txn2, b"account_a").unwrap();
        engine.get(&txn2, b"account_b").unwrap();

        engine.put(&txn1, "account_a", "0").unwrap();
        engine.put(&txn2, "account_b", "0").unwrap();

        engine.commit(&txn1).unwrap();
        let result = engine.commit(&txn2);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Write skew"));
    }

    #[test]
    fn test_serializable_no_conflict_disjoint() {
        let engine = create_test_engine();

        let txn1 = engine.begin_with_isolation(IsolationLevel::Serializable);
        let txn2 = engine.begin_with_isolation(IsolationLevel::Serializable);

        engine.put(&txn1, "key_a", "value_a").unwrap();
        engine.put(&txn2, "key_b", "value_b").unwrap();

        engine.commit(&txn1).unwrap();
        engine.commit(&txn2).unwrap();
    }

    #[test]
    fn test_serializable_sequential_commits() {
        let engine = create_test_engine();

        let txn1 = engine.begin_with_isolation(IsolationLevel::Serializable);
        engine.put(&txn1, "key", "value1").unwrap();
        engine.commit(&txn1).unwrap();

        let txn2 = engine.begin_with_isolation(IsolationLevel::Serializable);
        engine.get(&txn2, b"key").unwrap();
        engine.put(&txn2, "key", "value2").unwrap();
        engine.commit(&txn2).unwrap();
    }

    #[test]
    fn test_snapshot_isolation_allows_write_skew() {
        let engine = create_test_engine();

        let setup_txn = engine.begin();
        engine.put(&setup_txn, "account_a", "100").unwrap();
        engine.put(&setup_txn, "account_b", "100").unwrap();
        engine.commit(&setup_txn).unwrap();

        let txn1 = engine.begin_with_isolation(IsolationLevel::Snapshot);
        let txn2 = engine.begin_with_isolation(IsolationLevel::Snapshot);

        engine.get(&txn1, b"account_a").unwrap();
        engine.get(&txn1, b"account_b").unwrap();
        engine.get(&txn2, b"account_a").unwrap();
        engine.get(&txn2, b"account_b").unwrap();

        engine.put(&txn1, "account_a", "0").unwrap();
        engine.put(&txn2, "account_b", "0").unwrap();

        engine.commit(&txn1).unwrap();
        engine.commit(&txn2).unwrap();
    }

    #[test]
    fn test_abort_cleans_up_ssi_state() {
        let engine = create_test_engine();

        let txn1 = engine.begin_with_isolation(IsolationLevel::Serializable);
        engine.put(&txn1, "key", "value").unwrap();
        engine.abort(&txn1);

        let txn2 = engine.begin_with_isolation(IsolationLevel::Serializable);
        engine.put(&txn2, "key", "value2").unwrap();
        engine.commit(&txn2).unwrap();
    }
}
