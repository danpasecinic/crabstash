use std::ops::Bound;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use tracing::{debug, instrument};

use crate::deadlock::{DeadlockDetector, WaitForGraph};
use crate::escalation::{
    check_escalation, handle_escalation_action, EscalationConfig, EscalationState,
};
use crate::lock_entry::LockEntry;
use crate::lock_mode::LockMode;
use crate::range_lock::{IntervalTree, RangeLockEntry};
use crate::{bound_to_owned, LockError};

#[derive(Debug, Clone)]
pub struct LockConfig {
    pub lock_timeout_ms: u64,
    pub deadlock_check_interval_ms: u64,
    pub enable_range_locks: bool,
    pub escalation: EscalationConfig,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            lock_timeout_ms: 10_000,
            deadlock_check_interval_ms: 10,
            enable_range_locks: true,
            escalation: EscalationConfig::default(),
        }
    }
}

pub struct LockTable {
    key_locks: DashMap<Bytes, Arc<LockEntry>>,
    range_locks: RwLock<IntervalTree>,
    waiter_graph: Arc<Mutex<WaitForGraph>>,
    escalation_state: DashMap<u64, EscalationState>,
    config: LockConfig,
    deadlock_detector: Option<DeadlockDetector>,
    abort_receiver: Mutex<Option<Receiver<u64>>>,
}

impl LockTable {
    pub fn new(config: LockConfig) -> Self {
        let waiter_graph = Arc::new(Mutex::new(WaitForGraph::new()));

        let (detector, receiver) = DeadlockDetector::new(
            waiter_graph.clone(),
            Duration::from_millis(config.deadlock_check_interval_ms),
        );

        Self {
            key_locks: DashMap::new(),
            range_locks: RwLock::new(IntervalTree::new()),
            waiter_graph,
            escalation_state: DashMap::new(),
            config,
            deadlock_detector: Some(detector),
            abort_receiver: Mutex::new(Some(receiver)),
        }
    }

    pub fn start_deadlock_detection(&self) {
        if let Some(ref detector) = self.deadlock_detector {
            detector.start();
        }
    }

    pub fn register_txn(&self, txn_id: u64, start_ts: u64, priority: u32) {
        self.waiter_graph.lock().register_txn(txn_id, start_ts, priority);
        self.escalation_state.insert(txn_id, EscalationState::new());
    }

    pub fn unregister_txn(&self, txn_id: u64) {
        self.waiter_graph.lock().unregister_txn(txn_id);
        self.escalation_state.remove(&txn_id);
    }

    #[instrument(skip(self), fields(txn_id, key = %String::from_utf8_lossy(key)))]
    pub fn lock_key(
        &self,
        txn_id: u64,
        key: &[u8],
        mode: LockMode,
        timeout: Option<Duration>,
    ) -> Result<(), LockError> {
        let key_bytes = Bytes::copy_from_slice(key);

        if self.config.enable_range_locks {
            let range_locks = self.range_locks.read();
            let conflicting = range_locks.find_containing_key(key);
            for range_entry in conflicting {
                if range_entry.txn_id != txn_id && !mode.is_compatible(&range_entry.mode) {
                    debug!(
                        txn_id,
                        holder = range_entry.txn_id,
                        "blocked by range lock"
                    );
                    return Err(LockError::RangeConflict);
                }
            }
        }

        let entry = self
            .key_locks
            .entry(key_bytes.clone())
            .or_insert_with(|| Arc::new(LockEntry::new(key_bytes.clone())))
            .clone();

        if entry.try_acquire(txn_id, mode) {
            self.record_lock_acquired(txn_id, key_bytes);
            return Ok(());
        }

        let holders = entry.get_holders();
        {
            let mut graph = self.waiter_graph.lock();
            for (holder_id, _) in &holders {
                graph.add_wait_edge(txn_id, *holder_id);
            }
        }

        let result = entry.acquire(txn_id, mode, timeout);

        {
            let mut graph = self.waiter_graph.lock();
            for (holder_id, _) in &holders {
                graph.remove_wait_edge(txn_id, *holder_id);
            }
        }

        match result {
            Ok(()) => {
                self.record_lock_acquired(txn_id, key_bytes);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self), fields(txn_id))]
    pub fn lock_range(
        &self,
        txn_id: u64,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        mode: LockMode,
        _timeout: Option<Duration>,
    ) -> Result<(), LockError> {
        if !self.config.enable_range_locks {
            return Err(LockError::RangeLocksDisabled);
        }

        let start_owned = bound_to_owned(start);
        let end_owned = bound_to_owned(end);

        {
            let range_locks = self.range_locks.read();
            let overlapping = range_locks.find_overlapping(&start_owned, &end_owned);

            for entry in overlapping {
                if entry.txn_id != txn_id && !mode.is_compatible(&entry.mode) {
                    debug!(
                        txn_id,
                        holder = entry.txn_id,
                        "range lock conflict"
                    );
                    return Err(LockError::RangeConflict);
                }
            }
        }

        for entry in self.key_locks.iter() {
            let key_entry = entry.value();
            let key = key_entry.key();

            let in_range = key_in_range(key, &start_owned, &end_owned);
            if !in_range {
                continue;
            }

            let holders = key_entry.get_holders();
            for (holder_id, held_mode) in holders {
                if holder_id != txn_id && !mode.is_compatible(&held_mode) {
                    debug!(
                        txn_id,
                        holder = holder_id,
                        key = %String::from_utf8_lossy(key),
                        "range blocked by key lock"
                    );
                    return Err(LockError::RangeConflict);
                }
            }
        }

        let entry = RangeLockEntry::new(txn_id, mode, start_owned, end_owned);
        self.range_locks.write().insert(entry);

        debug!(txn_id, "range lock acquired");
        Ok(())
    }

    #[instrument(skip(self), fields(txn_id, key = %String::from_utf8_lossy(key)))]
    pub fn upgrade_lock(
        &self,
        txn_id: u64,
        key: &[u8],
        timeout: Option<Duration>,
    ) -> Result<(), LockError> {
        let key_bytes = Bytes::copy_from_slice(key);

        let entry = self
            .key_locks
            .get(&key_bytes)
            .ok_or(LockError::NotHeld)?
            .clone();

        entry.upgrade(txn_id, LockMode::X, timeout)
    }

    #[instrument(skip(self), fields(txn_id))]
    pub fn release_all(&self, txn_id: u64) {
        let mut released_count = 0;

        for entry in self.key_locks.iter() {
            if entry.value().release(txn_id) {
                released_count += 1;
            }
        }

        self.range_locks.write().remove_by_txn(txn_id);

        self.waiter_graph.lock().remove_all_waits(txn_id);
        self.unregister_txn(txn_id);

        debug!(txn_id, released_count, "released all locks");
    }

    pub fn check_conflict(&self, txn_id: u64, key: &[u8], mode: LockMode) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);

        if let Some(entry) = self.key_locks.get(&key_bytes) {
            let holders = entry.get_holders();
            for (holder_id, held_mode) in holders {
                if holder_id != txn_id && !mode.is_compatible(&held_mode) {
                    return true;
                }
            }
        }

        if self.config.enable_range_locks {
            let range_locks = self.range_locks.read();
            let conflicting = range_locks.find_containing_key(key);
            for range_entry in conflicting {
                if range_entry.txn_id != txn_id && !mode.is_compatible(&range_entry.mode) {
                    return true;
                }
            }
        }

        false
    }

    pub fn check_range_conflict(
        &self,
        txn_id: u64,
        key: &[u8],
        mode: LockMode,
    ) -> bool {
        if !self.config.enable_range_locks {
            return false;
        }

        let range_locks = self.range_locks.read();
        let conflicting = range_locks.find_containing_key(key);

        for range_entry in conflicting {
            if range_entry.txn_id != txn_id && !mode.is_compatible(&range_entry.mode) {
                return true;
            }
        }

        false
    }

    pub fn get_abort_receiver(&self) -> Option<Receiver<u64>> {
        self.abort_receiver.lock().take()
    }

    fn record_lock_acquired(&self, txn_id: u64, key: Bytes) {
        if let Some(state) = self.escalation_state.get(&txn_id) {
            state.record_key_lock(key);

            let count = state.get_lock_count();
            let action = check_escalation(count, &self.config.escalation);

            if handle_escalation_action(txn_id, action, &state) {
                if let Some(range) = state.compute_bounding_range() {
                    let _ = self.lock_range(
                        txn_id,
                        bound_ref(&range.0),
                        bound_ref(&range.1),
                        LockMode::X,
                        None,
                    );
                }
            }
        }

        self.waiter_graph.lock().increment_lock_count(txn_id);
    }

    #[cfg(test)]
    pub fn key_lock_count(&self) -> usize {
        self.key_locks.len()
    }

    #[cfg(test)]
    pub fn range_lock_count(&self) -> usize {
        self.range_locks.read().len()
    }
}

impl Drop for LockTable {
    fn drop(&mut self) {
        if let Some(ref detector) = self.deadlock_detector {
            detector.stop();
        }
    }
}

fn bound_ref(bound: &Bound<Bytes>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(b) => Bound::Included(b.as_ref()),
        Bound::Excluded(b) => Bound::Excluded(b.as_ref()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn key_in_range(key: &Bytes, start: &Bound<Bytes>, end: &Bound<Bytes>) -> bool {
    let after_start = match start {
        Bound::Included(s) => key >= s,
        Bound::Excluded(s) => key > s,
        Bound::Unbounded => true,
    };

    let before_end = match end {
        Bound::Included(e) => key <= e,
        Bound::Excluded(e) => key < e,
        Bound::Unbounded => true,
    };

    after_start && before_end
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_lock_unlock() {
        let table = LockTable::new(LockConfig::default());
        table.register_txn(1, 100, 0);

        table.lock_key(1, b"key1", LockMode::X, None).unwrap();
        assert!(table.check_conflict(2, b"key1", LockMode::S));

        table.release_all(1);
        assert!(!table.check_conflict(2, b"key1", LockMode::S));
    }

    #[test]
    fn test_shared_locks_compatible() {
        let table = LockTable::new(LockConfig::default());
        table.register_txn(1, 100, 0);
        table.register_txn(2, 200, 0);

        table.lock_key(1, b"key1", LockMode::S, None).unwrap();
        table.lock_key(2, b"key1", LockMode::S, None).unwrap();
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let table = LockTable::new(LockConfig::default());
        table.register_txn(1, 100, 0);
        table.register_txn(2, 200, 0);

        table.lock_key(1, b"key1", LockMode::X, None).unwrap();

        let result = table.lock_key(
            2,
            b"key1",
            LockMode::S,
            Some(Duration::from_millis(10)),
        );
        assert!(matches!(result, Err(LockError::Timeout)));
    }

    #[test]
    fn test_range_lock() {
        let table = LockTable::new(LockConfig::default());
        table.register_txn(1, 100, 0);

        table
            .lock_range(
                1,
                Bound::Included(b"a".as_slice()),
                Bound::Excluded(b"z".as_slice()),
                LockMode::S,
                None,
            )
            .unwrap();

        assert!(table.check_range_conflict(2, b"m", LockMode::X));
        assert!(!table.check_range_conflict(1, b"m", LockMode::X));
    }

    #[test]
    fn test_concurrent_locking() {
        let table = Arc::new(LockTable::new(LockConfig::default()));

        let table1 = table.clone();
        let h1 = thread::spawn(move || {
            table1.register_txn(1, 100, 0);
            for i in 0..100 {
                let key = format!("key{}", i);
                table1.lock_key(1, key.as_bytes(), LockMode::S, None).unwrap();
            }
            table1.release_all(1);
        });

        let table2 = table.clone();
        let h2 = thread::spawn(move || {
            table2.register_txn(2, 200, 0);
            for i in 100..200 {
                let key = format!("key{}", i);
                table2.lock_key(2, key.as_bytes(), LockMode::S, None).unwrap();
            }
            table2.release_all(2);
        });

        h1.join().unwrap();
        h2.join().unwrap();
    }
}
