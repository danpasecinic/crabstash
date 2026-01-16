mod deadlock;
mod escalation;
mod lock_entry;
mod lock_mode;
mod lock_table;
mod range_lock;

pub use deadlock::{DeadlockDetector, WaitForGraph};
pub use escalation::{EscalationConfig, EscalationState};
pub use lock_entry::LockEntry;
pub use lock_mode::LockMode;
pub use lock_table::{LockConfig, LockTable};
pub use range_lock::{IntervalTree, RangeLockEntry};

use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

#[derive(Debug, Clone, PartialEq)]
pub enum LockError {
    Timeout,
    Deadlock,
    TransactionAborted,
    LockEscalationFailed,
    RangeConflict,
    RangeLocksDisabled,
    NotHeld,
    InvalidUpgrade,
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Timeout => write!(f, "lock acquisition timed out"),
            LockError::Deadlock => write!(f, "deadlock detected"),
            LockError::TransactionAborted => write!(f, "transaction was aborted"),
            LockError::LockEscalationFailed => write!(f, "lock escalation failed"),
            LockError::RangeConflict => write!(f, "range lock conflict"),
            LockError::RangeLocksDisabled => write!(f, "range locks are disabled"),
            LockError::NotHeld => write!(f, "lock not held by transaction"),
            LockError::InvalidUpgrade => write!(f, "invalid lock upgrade path"),
        }
    }
}

impl std::error::Error for LockError {}

pub struct LockManager {
    lock_table: LockTable,
    config: LockConfig,
}

impl LockManager {
    pub fn new(config: LockConfig) -> Self {
        let lock_table = LockTable::new(config.clone());
        lock_table.start_deadlock_detection();

        Self { lock_table, config }
    }

    pub fn with_default_config() -> Self {
        Self::new(LockConfig::default())
    }

    pub fn register_txn(&self, txn_id: u64, start_ts: u64) {
        self.lock_table.register_txn(txn_id, start_ts, 0);
    }

    pub fn register_txn_with_priority(&self, txn_id: u64, start_ts: u64, priority: u32) {
        self.lock_table.register_txn(txn_id, start_ts, priority);
    }

    pub fn lock_key(
        &self,
        txn_id: u64,
        key: &[u8],
        mode: LockMode,
        timeout: Option<Duration>,
    ) -> Result<LockGuard, LockError> {
        let timeout = timeout.unwrap_or(Duration::from_millis(self.config.lock_timeout_ms));
        self.lock_table.lock_key(txn_id, key, mode, Some(timeout))?;

        Ok(LockGuard {
            txn_id,
            key: Bytes::copy_from_slice(key),
            mode,
        })
    }

    pub fn lock_range(
        &self,
        txn_id: u64,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        mode: LockMode,
        timeout: Option<Duration>,
    ) -> Result<RangeLockGuard, LockError> {
        let timeout = timeout.unwrap_or(Duration::from_millis(self.config.lock_timeout_ms));
        self.lock_table.lock_range(txn_id, start, end, mode, Some(timeout))?;

        Ok(RangeLockGuard {
            txn_id,
            start: bound_to_owned(start),
            end: bound_to_owned(end),
            mode,
        })
    }

    pub fn upgrade_lock(
        &self,
        txn_id: u64,
        key: &[u8],
        timeout: Option<Duration>,
    ) -> Result<(), LockError> {
        let timeout = timeout.unwrap_or(Duration::from_millis(self.config.lock_timeout_ms));
        self.lock_table.upgrade_lock(txn_id, key, Some(timeout))
    }

    pub fn release_all(&self, txn_id: u64) {
        self.lock_table.release_all(txn_id);
    }

    pub fn check_conflict(&self, txn_id: u64, key: &[u8], mode: LockMode) -> bool {
        self.lock_table.check_conflict(txn_id, key, mode)
    }

    pub fn check_range_conflict(&self, txn_id: u64, key: &[u8], mode: LockMode) -> bool {
        self.lock_table.check_range_conflict(txn_id, key, mode)
    }

    pub fn config(&self) -> &LockConfig {
        &self.config
    }
}

pub struct LockGuard {
    pub txn_id: u64,
    pub key: Bytes,
    pub mode: LockMode,
}

pub struct RangeLockGuard {
    pub txn_id: u64,
    pub start: Bound<Bytes>,
    pub end: Bound<Bytes>,
    pub mode: LockMode,
}

fn bound_to_owned(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(b) => Bound::Included(Bytes::copy_from_slice(b)),
        Bound::Excluded(b) => Bound::Excluded(Bytes::copy_from_slice(b)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_manager_basic() {
        let manager = LockManager::with_default_config();
        manager.register_txn(1, 100);

        let guard = manager.lock_key(1, b"key1", LockMode::X, None).unwrap();
        assert_eq!(guard.txn_id, 1);
        assert_eq!(guard.mode, LockMode::X);

        assert!(manager.check_conflict(2, b"key1", LockMode::S));

        manager.release_all(1);
        assert!(!manager.check_conflict(2, b"key1", LockMode::S));
    }

    #[test]
    fn test_lock_manager_range() {
        let manager = LockManager::with_default_config();
        manager.register_txn(1, 100);

        let guard = manager
            .lock_range(
                1,
                Bound::Included(b"a".as_slice()),
                Bound::Excluded(b"z".as_slice()),
                LockMode::S,
                None,
            )
            .unwrap();

        assert_eq!(guard.txn_id, 1);
        assert!(manager.check_range_conflict(2, b"m", LockMode::X));

        manager.release_all(1);
        assert!(!manager.check_range_conflict(2, b"m", LockMode::X));
    }

    #[test]
    fn test_multiple_transactions() {
        let manager = LockManager::with_default_config();
        manager.register_txn(1, 100);
        manager.register_txn(2, 200);

        manager.lock_key(1, b"key1", LockMode::S, None).unwrap();
        manager.lock_key(2, b"key1", LockMode::S, None).unwrap();

        manager.lock_key(1, b"key2", LockMode::X, None).unwrap();

        let result = manager.lock_key(2, b"key2", LockMode::S, Some(Duration::from_millis(10)));
        assert!(matches!(result, Err(LockError::Timeout)));

        manager.release_all(1);
        manager.lock_key(2, b"key2", LockMode::X, None).unwrap();
    }
}
