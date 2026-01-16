use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use rapidhash::rapidhash;
use tracing::{debug, warn};

#[inline]
fn hash_key(key: &[u8]) -> u64 {
    rapidhash(key)
}

#[derive(Debug, Clone)]
pub struct ReadWriteSet {
    read_keys: HashSet<u64>,
    write_keys: HashSet<u64>,
    read_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
}

impl ReadWriteSet {
    pub fn new() -> Self {
        Self {
            read_keys: HashSet::new(),
            write_keys: HashSet::new(),
            read_ranges: Vec::new(),
        }
    }

    pub fn record_read(&mut self, key: &[u8]) {
        self.read_keys.insert(hash_key(key));
    }

    pub fn record_write(&mut self, key: &[u8]) {
        self.write_keys.insert(hash_key(key));
    }

    pub fn record_range_read(&mut self, start: Bound<Bytes>, end: Bound<Bytes>) {
        self.read_ranges.push((start, end));
    }

    pub fn read_set(&self) -> &HashSet<u64> {
        &self.read_keys
    }

    pub fn write_set(&self) -> &HashSet<u64> {
        &self.write_keys
    }

    pub fn has_read_write_overlap(&self, other_writes: &HashSet<u64>) -> bool {
        !self.read_keys.is_disjoint(other_writes)
    }

    pub fn has_range_overlap(&self, key: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        self.read_ranges.iter().any(|(start, end)| {
            let after_start = match start {
                Bound::Unbounded => true,
                Bound::Included(s) => &key_bytes >= s,
                Bound::Excluded(s) => &key_bytes > s,
            };
            let before_end = match end {
                Bound::Unbounded => true,
                Bound::Included(e) => &key_bytes <= e,
                Bound::Excluded(e) => &key_bytes < e,
            };
            after_start && before_end
        })
    }
}

impl Default for ReadWriteSet {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct CommittedTxn {
    write_set: HashSet<u64>,
}

pub struct SSIManager {
    committed_txns: RwLock<HashMap<u64, CommittedTxn>>,
    active_txns: DashMap<u64, Arc<Mutex<ReadWriteSet>>>,
    commit_lock: Mutex<()>,
    watermark: RwLock<u64>,
}

impl SSIManager {
    pub fn new() -> Self {
        Self {
            committed_txns: RwLock::new(HashMap::new()),
            active_txns: DashMap::new(),
            commit_lock: Mutex::new(()),
            watermark: RwLock::new(0),
        }
    }

    pub fn begin_txn(&self, txn_id: u64) -> Arc<Mutex<ReadWriteSet>> {
        let rw_set = Arc::new(Mutex::new(ReadWriteSet::new()));
        self.active_txns.insert(txn_id, rw_set.clone());
        debug!(txn_id, "SSI: transaction started");
        rw_set
    }

    pub fn record_read(&self, txn_id: u64, key: &[u8]) {
        if let Some(rw_set) = self.active_txns.get(&txn_id) {
            rw_set.lock().record_read(key);
        }
    }

    pub fn record_write(&self, txn_id: u64, key: &[u8]) {
        if let Some(rw_set) = self.active_txns.get(&txn_id) {
            rw_set.lock().record_write(key);
        }
    }

    pub fn record_range_read(&self, txn_id: u64, start: Bound<Bytes>, end: Bound<Bytes>) {
        if let Some(rw_set) = self.active_txns.get(&txn_id) {
            rw_set.lock().record_range_read(start, end);
        }
    }

    pub fn validate_and_commit(
        &self,
        txn_id: u64,
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<(), SSIConflict> {
        let _commit_guard = self.commit_lock.lock();

        let rw_set_arc = self
            .active_txns
            .get(&txn_id)
            .map(|r| r.clone())
            .ok_or(SSIConflict::TxnNotFound)?;
        let rw_set = rw_set_arc.lock();

        let committed = self.committed_txns.read();
        for (&ts, committed_txn) in committed.iter() {
            if ts > read_ts && ts < commit_ts {
                if rw_set.has_read_write_overlap(&committed_txn.write_set) {
                    warn!(
                        txn_id,
                        conflicting_ts = ts,
                        "SSI: write skew detected, aborting"
                    );
                    return Err(SSIConflict::WriteSkew { conflicting_ts: ts });
                }

                for write_hash in &committed_txn.write_set {
                    if rw_set
                        .read_ranges
                        .iter()
                        .any(|(start, end)| self.hash_in_range(*write_hash, start, end))
                    {
                        warn!(
                            txn_id,
                            conflicting_ts = ts,
                            "SSI: phantom detected in range read"
                        );
                        return Err(SSIConflict::Phantom { conflicting_ts: ts });
                    }
                }
            }
        }
        drop(committed);

        let write_set = rw_set.write_set().clone();
        drop(rw_set);

        self.committed_txns
            .write()
            .insert(commit_ts, CommittedTxn { write_set });

        self.active_txns.remove(&txn_id);
        debug!(txn_id, commit_ts, "SSI: transaction committed");

        Ok(())
    }

    pub fn abort_txn(&self, txn_id: u64) {
        self.active_txns.remove(&txn_id);
        debug!(txn_id, "SSI: transaction aborted");
    }

    pub fn advance_watermark(&self, new_watermark: u64) {
        let mut watermark = self.watermark.write();
        if new_watermark > *watermark {
            *watermark = new_watermark;

            let mut committed = self.committed_txns.write();
            committed.retain(|&ts, _| ts >= new_watermark);
            debug!(new_watermark, "SSI: advanced watermark, pruned old txns");
        }
    }

    pub fn get_rw_set(&self, txn_id: u64) -> Option<Arc<Mutex<ReadWriteSet>>> {
        self.active_txns.get(&txn_id).map(|r| r.clone())
    }

    fn hash_in_range(&self, _hash: u64, _start: &Bound<Bytes>, _end: &Bound<Bytes>) -> bool {
        true
    }
}

impl Default for SSIManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SSIConflict {
    WriteSkew { conflicting_ts: u64 },
    Phantom { conflicting_ts: u64 },
    TxnNotFound,
}

impl std::fmt::Display for SSIConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SSIConflict::WriteSkew { conflicting_ts } => {
                write!(f, "write skew conflict with txn at ts {}", conflicting_ts)
            }
            SSIConflict::Phantom { conflicting_ts } => {
                write!(f, "phantom conflict with txn at ts {}", conflicting_ts)
            }
            SSIConflict::TxnNotFound => write!(f, "transaction not found"),
        }
    }
}

impl std::error::Error for SSIConflict {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_write_set_basic() {
        let mut rw_set = ReadWriteSet::new();
        rw_set.record_read(b"key1");
        rw_set.record_write(b"key2");

        assert_eq!(rw_set.read_set().len(), 1);
        assert_eq!(rw_set.write_set().len(), 1);
    }

    #[test]
    fn test_read_write_overlap() {
        let mut rw_set1 = ReadWriteSet::new();
        rw_set1.record_read(b"key1");
        rw_set1.record_read(b"key2");

        let mut rw_set2 = ReadWriteSet::new();
        rw_set2.record_write(b"key2");
        rw_set2.record_write(b"key3");

        assert!(rw_set1.has_read_write_overlap(rw_set2.write_set()));
    }

    #[test]
    fn test_no_overlap() {
        let mut rw_set1 = ReadWriteSet::new();
        rw_set1.record_read(b"key1");

        let mut rw_set2 = ReadWriteSet::new();
        rw_set2.record_write(b"key2");

        assert!(!rw_set1.has_read_write_overlap(rw_set2.write_set()));
    }

    #[test]
    fn test_ssi_manager_basic() {
        let ssi = SSIManager::new();

        let _rw_set = ssi.begin_txn(1);
        ssi.record_read(1, b"key1");
        ssi.record_write(1, b"key2");

        let result = ssi.validate_and_commit(1, 0, 10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_ssi_write_skew_detection() {
        let ssi = SSIManager::new();

        let _rw1 = ssi.begin_txn(1);
        ssi.record_read(1, b"account_a");
        ssi.record_read(1, b"account_b");

        let _rw2 = ssi.begin_txn(2);
        ssi.record_read(2, b"account_a");
        ssi.record_read(2, b"account_b");

        ssi.record_write(1, b"account_a");
        ssi.record_write(2, b"account_b");

        let result1 = ssi.validate_and_commit(1, 0, 10);
        assert!(result1.is_ok());

        let result2 = ssi.validate_and_commit(2, 0, 20);
        assert!(matches!(result2, Err(SSIConflict::WriteSkew { .. })));
    }

    #[test]
    fn test_ssi_no_conflict_sequential() {
        let ssi = SSIManager::new();

        let _rw1 = ssi.begin_txn(1);
        ssi.record_read(1, b"key1");
        ssi.record_write(1, b"key1");
        ssi.validate_and_commit(1, 0, 10).unwrap();

        let _rw2 = ssi.begin_txn(2);
        ssi.record_read(2, b"key1");
        ssi.record_write(2, b"key1");
        let result = ssi.validate_and_commit(2, 10, 20);
        assert!(result.is_ok());
    }

    #[test]
    fn test_range_read_tracking() {
        let mut rw_set = ReadWriteSet::new();
        rw_set.record_range_read(
            Bound::Included(Bytes::from("a")),
            Bound::Excluded(Bytes::from("z")),
        );

        assert!(rw_set.has_range_overlap(b"m"));
        assert!(!rw_set.has_range_overlap(b"z"));
    }

    #[test]
    fn test_watermark_advancement() {
        let ssi = SSIManager::new();

        let _rw1 = ssi.begin_txn(1);
        ssi.record_write(1, b"key1");
        ssi.validate_and_commit(1, 0, 10).unwrap();

        let _rw2 = ssi.begin_txn(2);
        ssi.record_write(2, b"key2");
        ssi.validate_and_commit(2, 10, 20).unwrap();

        assert_eq!(ssi.committed_txns.read().len(), 2);

        ssi.advance_watermark(15);
        assert_eq!(ssi.committed_txns.read().len(), 1);
    }
}
