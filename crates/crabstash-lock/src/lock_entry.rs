use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::{Condvar, Mutex};

use crate::LockError;
use crate::lock_mode::LockMode;

pub struct LockEntry {
    key: Bytes,
    holders: Mutex<HashMap<u64, LockMode>>,
    wait_queue: Mutex<VecDeque<LockRequest>>,
    condvar: Condvar,
}

pub struct LockRequest {
    pub txn_id: u64,
    pub mode: LockMode,
    pub granted: AtomicBool,
    pub enqueue_time: Instant,
    pub is_upgrade: bool,
}

impl LockRequest {
    pub fn new(txn_id: u64, mode: LockMode, is_upgrade: bool) -> Self {
        Self {
            txn_id,
            mode,
            granted: AtomicBool::new(false),
            enqueue_time: Instant::now(),
            is_upgrade,
        }
    }

    pub fn is_granted(&self) -> bool {
        self.granted.load(Ordering::Acquire)
    }

    pub fn grant(&self) {
        self.granted.store(true, Ordering::Release);
    }

    pub fn elapsed(&self) -> Duration {
        self.enqueue_time.elapsed()
    }
}

impl LockEntry {
    pub fn new(key: Bytes) -> Self {
        Self {
            key,
            holders: Mutex::new(HashMap::new()),
            wait_queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        }
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub fn try_acquire(&self, txn_id: u64, mode: LockMode) -> bool {
        let mut holders = self.holders.lock();

        if let Some(&held_mode) = holders.get(&txn_id) {
            if held_mode == mode || !held_mode.can_upgrade_to(&mode) {
                return true;
            }
        }

        if self.is_compatible_with_holders(&holders, txn_id, mode) {
            holders.insert(txn_id, mode);
            return true;
        }

        false
    }

    pub fn acquire(
        &self,
        txn_id: u64,
        mode: LockMode,
        timeout: Option<Duration>,
    ) -> Result<(), LockError> {
        if self.try_acquire(txn_id, mode) {
            return Ok(());
        }

        let request = LockRequest::new(txn_id, mode, false);
        {
            let mut queue = self.wait_queue.lock();
            queue.push_back(request);
        }

        let deadline = timeout.map(|t| Instant::now() + t);

        loop {
            let mut holders = self.holders.lock();

            if self.is_compatible_with_holders(&holders, txn_id, mode) {
                holders.insert(txn_id, mode);
                self.remove_from_queue(txn_id);
                return Ok(());
            }

            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    self.remove_from_queue(txn_id);
                    return Err(LockError::Timeout);
                }
                let wait_result = self.condvar.wait_for(&mut holders, remaining);
                if wait_result.timed_out() {
                    self.remove_from_queue(txn_id);
                    return Err(LockError::Timeout);
                }
            } else {
                self.condvar.wait(&mut holders);
            }
        }
    }

    pub fn upgrade(
        &self,
        txn_id: u64,
        target_mode: LockMode,
        timeout: Option<Duration>,
    ) -> Result<(), LockError> {
        {
            let holders = self.holders.lock();
            if let Some(&current_mode) = holders.get(&txn_id) {
                if current_mode == target_mode {
                    return Ok(());
                }
                if !current_mode.can_upgrade_to(&target_mode) {
                    return Err(LockError::InvalidUpgrade);
                }
            } else {
                return Err(LockError::NotHeld);
            }
        }

        let request = LockRequest::new(txn_id, target_mode, true);
        {
            let mut queue = self.wait_queue.lock();
            queue.push_front(request);
        }

        let deadline = timeout.map(|t| Instant::now() + t);

        loop {
            let mut holders = self.holders.lock();

            let can_upgrade = holders
                .iter()
                .all(|(&id, mode)| id == txn_id || mode.is_compatible(&target_mode));

            if can_upgrade {
                holders.insert(txn_id, target_mode);
                self.remove_from_queue(txn_id);
                return Ok(());
            }

            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    self.remove_from_queue(txn_id);
                    return Err(LockError::Timeout);
                }
                let wait_result = self.condvar.wait_for(&mut holders, remaining);
                if wait_result.timed_out() {
                    self.remove_from_queue(txn_id);
                    return Err(LockError::Timeout);
                }
            } else {
                self.condvar.wait(&mut holders);
            }
        }
    }

    pub fn release(&self, txn_id: u64) -> bool {
        let mut holders = self.holders.lock();
        let removed = holders.remove(&txn_id).is_some();
        if removed {
            drop(holders);
            self.condvar.notify_all();
        }
        removed
    }

    pub fn is_held_by(&self, txn_id: u64) -> bool {
        self.holders.lock().contains_key(&txn_id)
    }

    pub fn get_mode(&self, txn_id: u64) -> Option<LockMode> {
        self.holders.lock().get(&txn_id).copied()
    }

    pub fn holder_count(&self) -> usize {
        self.holders.lock().len()
    }

    pub fn waiter_count(&self) -> usize {
        self.wait_queue.lock().len()
    }

    pub fn get_holders(&self) -> Vec<(u64, LockMode)> {
        self.holders
            .lock()
            .iter()
            .map(|(&id, &mode)| (id, mode))
            .collect()
    }

    pub fn get_waiters(&self) -> Vec<u64> {
        self.wait_queue.lock().iter().map(|r| r.txn_id).collect()
    }

    fn is_compatible_with_holders(
        &self,
        holders: &HashMap<u64, LockMode>,
        txn_id: u64,
        mode: LockMode,
    ) -> bool {
        holders
            .iter()
            .all(|(&id, held_mode)| id == txn_id || mode.is_compatible(held_mode))
    }

    fn remove_from_queue(&self, txn_id: u64) {
        let mut queue = self.wait_queue.lock();
        queue.retain(|r| r.txn_id != txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_shared_lock() {
        let entry = LockEntry::new(Bytes::from("key1"));
        assert!(entry.try_acquire(1, LockMode::S));
        assert!(entry.is_held_by(1));
        assert_eq!(entry.get_mode(1), Some(LockMode::S));
    }

    #[test]
    fn test_multiple_shared_locks() {
        let entry = LockEntry::new(Bytes::from("key1"));
        assert!(entry.try_acquire(1, LockMode::S));
        assert!(entry.try_acquire(2, LockMode::S));
        assert!(entry.try_acquire(3, LockMode::S));
        assert_eq!(entry.holder_count(), 3);
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let entry = LockEntry::new(Bytes::from("key1"));
        assert!(entry.try_acquire(1, LockMode::X));
        assert!(!entry.try_acquire(2, LockMode::S));
        assert!(!entry.try_acquire(3, LockMode::X));
    }

    #[test]
    fn test_shared_blocks_exclusive() {
        let entry = LockEntry::new(Bytes::from("key1"));
        assert!(entry.try_acquire(1, LockMode::S));
        assert!(!entry.try_acquire(2, LockMode::X));
    }

    #[test]
    fn test_release_notifies_waiters() {
        let entry = LockEntry::new(Bytes::from("key1"));
        assert!(entry.try_acquire(1, LockMode::X));
        assert!(entry.release(1));
        assert!(entry.try_acquire(2, LockMode::X));
    }

    #[test]
    fn test_reentrant_lock() {
        let entry = LockEntry::new(Bytes::from("key1"));
        assert!(entry.try_acquire(1, LockMode::S));
        assert!(entry.try_acquire(1, LockMode::S));
        assert_eq!(entry.holder_count(), 1);
    }
}
