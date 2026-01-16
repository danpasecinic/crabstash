use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use rapidhash::rapidhash;
use tracing::{debug, info, warn};

#[inline]
fn hash_key(key: &[u8]) -> u64 {
    rapidhash(key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType {
    ReadWrite,
    WriteRead,
}

#[derive(Debug, Clone)]
pub struct RWConflict {
    pub from_txn: u64,
    pub to_txn: u64,
    pub conflict_type: ConflictType,
}

#[derive(Debug, Clone)]
pub struct ReadWriteSet {
    read_keys: HashSet<u64>,
    write_keys: HashSet<u64>,
    read_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
    inbound_conflicts: Vec<RWConflict>,
    outbound_conflicts: Vec<RWConflict>,
}

impl ReadWriteSet {
    pub fn new() -> Self {
        Self {
            read_keys: HashSet::new(),
            write_keys: HashSet::new(),
            read_ranges: Vec::new(),
            inbound_conflicts: Vec::new(),
            outbound_conflicts: Vec::new(),
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

    pub fn add_inbound_conflict(&mut self, conflict: RWConflict) {
        self.inbound_conflicts.push(conflict);
    }

    pub fn add_outbound_conflict(&mut self, conflict: RWConflict) {
        self.outbound_conflicts.push(conflict);
    }

    pub fn has_inbound_conflict(&self) -> bool {
        !self.inbound_conflicts.is_empty()
    }

    pub fn has_outbound_conflict(&self) -> bool {
        !self.outbound_conflicts.is_empty()
    }

    pub fn has_dangerous_structure(&self) -> bool {
        self.has_inbound_conflict() && self.has_outbound_conflict()
    }

    pub fn has_read_write_overlap(&self, other_writes: &HashSet<u64>) -> bool {
        !self.read_keys.is_disjoint(other_writes)
    }

    pub fn has_write_read_overlap(&self, other_reads: &HashSet<u64>) -> bool {
        !self.write_keys.is_disjoint(other_reads)
    }

    pub fn has_range_overlap(&self, key: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        self.read_ranges.iter().any(|(start, end)| {
            let after_start = match start {
                Bound::Unbounded => true,
                Bound::Included(s) => key_bytes >= *s,
                Bound::Excluded(s) => key_bytes > *s,
            };
            let before_end = match end {
                Bound::Unbounded => true,
                Bound::Included(e) => key_bytes <= *e,
                Bound::Excluded(e) => key_bytes < *e,
            };
            after_start && before_end
        })
    }

    pub fn read_ranges(&self) -> &[(Bound<Bytes>, Bound<Bytes>)] {
        &self.read_ranges
    }
}

impl Default for ReadWriteSet {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct CommittedTxn {
    start_ts: u64,
    read_set: HashSet<u64>,
    write_set: HashSet<u64>,
    write_keys: Vec<Bytes>,
}

#[derive(Debug, Default)]
pub struct SSIStats {
    pub commits: AtomicU64,
    pub aborts_write_skew: AtomicU64,
    pub aborts_phantom: AtomicU64,
    pub aborts_dangerous_structure: AtomicU64,
    pub conflicts_detected: AtomicU64,
}

impl SSIStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> SSIStatsSnapshot {
        SSIStatsSnapshot {
            commits: self.commits.load(Ordering::Relaxed),
            aborts_write_skew: self.aborts_write_skew.load(Ordering::Relaxed),
            aborts_phantom: self.aborts_phantom.load(Ordering::Relaxed),
            aborts_dangerous_structure: self.aborts_dangerous_structure.load(Ordering::Relaxed),
            conflicts_detected: self.conflicts_detected.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SSIStatsSnapshot {
    pub commits: u64,
    pub aborts_write_skew: u64,
    pub aborts_phantom: u64,
    pub aborts_dangerous_structure: u64,
    pub conflicts_detected: u64,
}

pub struct SSIManager {
    committed_txns: RwLock<HashMap<u64, CommittedTxn>>,
    active_txns: DashMap<u64, Arc<Mutex<ReadWriteSet>>>,
    active_start_ts: DashMap<u64, u64>,
    commit_lock: Mutex<()>,
    watermark: RwLock<u64>,
    safe_snapshot: AtomicU64,
    stats: SSIStats,
    recently_committed: Mutex<VecDeque<(u64, u64)>>,
}

impl SSIManager {
    const MAX_RECENTLY_COMMITTED: usize = 1000;

    pub fn new() -> Self {
        Self {
            committed_txns: RwLock::new(HashMap::new()),
            active_txns: DashMap::new(),
            active_start_ts: DashMap::new(),
            commit_lock: Mutex::new(()),
            watermark: RwLock::new(0),
            safe_snapshot: AtomicU64::new(0),
            stats: SSIStats::new(),
            recently_committed: Mutex::new(VecDeque::with_capacity(Self::MAX_RECENTLY_COMMITTED)),
        }
    }

    pub fn begin_txn(&self, txn_id: u64) -> Arc<Mutex<ReadWriteSet>> {
        self.begin_txn_with_ts(txn_id, txn_id)
    }

    pub fn begin_txn_with_ts(&self, txn_id: u64, start_ts: u64) -> Arc<Mutex<ReadWriteSet>> {
        let rw_set = Arc::new(Mutex::new(ReadWriteSet::new()));
        self.active_txns.insert(txn_id, rw_set.clone());
        self.active_start_ts.insert(txn_id, start_ts);
        self.update_safe_snapshot();
        debug!(txn_id, start_ts, "SSI: transaction started");
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
        self.validate_and_commit_with_keys(txn_id, read_ts, commit_ts, Vec::new())
    }

    pub fn validate_and_commit_with_keys(
        &self,
        txn_id: u64,
        read_ts: u64,
        commit_ts: u64,
        written_keys: Vec<Bytes>,
    ) -> Result<(), SSIConflict> {
        let _commit_guard = self.commit_lock.lock();

        let rw_set_arc = self
            .active_txns
            .get(&txn_id)
            .map(|r| r.clone())
            .ok_or(SSIConflict::TxnNotFound)?;
        let mut rw_set = rw_set_arc.lock();

        let committed = self.committed_txns.read();
        for (&ts, committed_txn) in committed.iter() {
            if ts > read_ts && ts < commit_ts {
                if rw_set.has_read_write_overlap(&committed_txn.write_set) {
                    self.stats.conflicts_detected.fetch_add(1, Ordering::Relaxed);
                    rw_set.add_inbound_conflict(RWConflict {
                        from_txn: ts,
                        to_txn: txn_id,
                        conflict_type: ConflictType::ReadWrite,
                    });
                }

                if rw_set.has_write_read_overlap(&committed_txn.read_set) {
                    self.stats.conflicts_detected.fetch_add(1, Ordering::Relaxed);
                    rw_set.add_outbound_conflict(RWConflict {
                        from_txn: txn_id,
                        to_txn: ts,
                        conflict_type: ConflictType::WriteRead,
                    });
                }

                for write_key in &committed_txn.write_keys {
                    if self.key_in_ranges(write_key, rw_set.read_ranges()) {
                        warn!(
                            txn_id,
                            conflicting_ts = ts,
                            "SSI: phantom detected in range read"
                        );
                        self.stats.aborts_phantom.fetch_add(1, Ordering::Relaxed);
                        return Err(SSIConflict::Phantom { conflicting_ts: ts });
                    }
                }
            }
        }
        drop(committed);

        if rw_set.has_dangerous_structure() {
            warn!(
                txn_id,
                inbound = rw_set.inbound_conflicts.len(),
                outbound = rw_set.outbound_conflicts.len(),
                "SSI: dangerous structure detected (T1->T2->T3)"
            );
            self.stats
                .aborts_dangerous_structure
                .fetch_add(1, Ordering::Relaxed);
            return Err(SSIConflict::DangerousStructure {
                inbound_from: rw_set
                    .inbound_conflicts
                    .first()
                    .map(|c| c.from_txn)
                    .unwrap_or(0),
                outbound_to: rw_set
                    .outbound_conflicts
                    .first()
                    .map(|c| c.to_txn)
                    .unwrap_or(0),
            });
        }

        if rw_set.has_inbound_conflict() {
            let conflicting_ts = rw_set.inbound_conflicts.first().unwrap().from_txn;
            warn!(
                txn_id,
                conflicting_ts, "SSI: write skew detected, aborting"
            );
            self.stats.aborts_write_skew.fetch_add(1, Ordering::Relaxed);
            return Err(SSIConflict::WriteSkew { conflicting_ts });
        }

        let read_set = rw_set.read_set().clone();
        let write_set = rw_set.write_set().clone();
        drop(rw_set);

        let start_ts = self
            .active_start_ts
            .get(&txn_id)
            .map(|r| *r)
            .unwrap_or(read_ts);

        self.committed_txns.write().insert(
            commit_ts,
            CommittedTxn {
                start_ts,
                read_set,
                write_set,
                write_keys: written_keys,
            },
        );

        {
            let mut recent = self.recently_committed.lock();
            recent.push_back((txn_id, commit_ts));
            while recent.len() > Self::MAX_RECENTLY_COMMITTED {
                recent.pop_front();
            }
        }

        self.active_txns.remove(&txn_id);
        self.active_start_ts.remove(&txn_id);
        self.update_safe_snapshot();
        self.stats.commits.fetch_add(1, Ordering::Relaxed);
        debug!(txn_id, commit_ts, "SSI: transaction committed");

        Ok(())
    }

    pub fn abort_txn(&self, txn_id: u64) {
        self.active_txns.remove(&txn_id);
        self.active_start_ts.remove(&txn_id);
        self.update_safe_snapshot();
        debug!(txn_id, "SSI: transaction aborted");
    }

    pub fn advance_watermark(&self, new_watermark: u64) {
        let mut watermark = self.watermark.write();
        if new_watermark > *watermark {
            *watermark = new_watermark;

            let mut committed = self.committed_txns.write();
            let before = committed.len();
            committed.retain(|&ts, _| ts >= new_watermark);
            let pruned = before - committed.len();
            if pruned > 0 {
                info!(
                    new_watermark,
                    pruned, "SSI: advanced watermark, pruned old txns"
                );
            }
        }
    }

    pub fn get_rw_set(&self, txn_id: u64) -> Option<Arc<Mutex<ReadWriteSet>>> {
        self.active_txns.get(&txn_id).map(|r| r.clone())
    }

    pub fn safe_snapshot(&self) -> u64 {
        self.safe_snapshot.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> SSIStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn active_txn_count(&self) -> usize {
        self.active_txns.len()
    }

    pub fn committed_txn_count(&self) -> usize {
        self.committed_txns.read().len()
    }

    fn update_safe_snapshot(&self) {
        let min_start_ts = self
            .active_start_ts
            .iter()
            .map(|r| *r.value())
            .min()
            .unwrap_or(u64::MAX);
        self.safe_snapshot.store(min_start_ts, Ordering::Release);
    }

    fn key_in_ranges(&self, key: &Bytes, ranges: &[(Bound<Bytes>, Bound<Bytes>)]) -> bool {
        ranges.iter().any(|(start, end)| {
            let after_start = match start {
                Bound::Unbounded => true,
                Bound::Included(s) => key >= s,
                Bound::Excluded(s) => key > s,
            };
            let before_end = match end {
                Bound::Unbounded => true,
                Bound::Included(e) => key <= e,
                Bound::Excluded(e) => key < e,
            };
            after_start && before_end
        })
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
    DangerousStructure { inbound_from: u64, outbound_to: u64 },
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
            SSIConflict::DangerousStructure {
                inbound_from,
                outbound_to,
            } => {
                write!(
                    f,
                    "dangerous structure: T{} -> this -> T{}",
                    inbound_from, outbound_to
                )
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
