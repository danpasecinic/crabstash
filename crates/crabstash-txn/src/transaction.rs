use bytes::Bytes;
use crabstash_common::{Error, Result};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    Snapshot,
    Serializable,
    SerializableReadOnly,
    SerializableDeferrable,
}

impl IsolationLevel {
    pub fn is_serializable(self) -> bool {
        matches!(
            self,
            IsolationLevel::Serializable
                | IsolationLevel::SerializableReadOnly
                | IsolationLevel::SerializableDeferrable
        )
    }

    pub fn is_read_only(self) -> bool {
        matches!(
            self,
            IsolationLevel::SerializableReadOnly | IsolationLevel::SerializableDeferrable
        )
    }

    pub fn is_deferrable(self) -> bool {
        matches!(self, IsolationLevel::SerializableDeferrable)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

pub struct WriteSet {
    puts: HashMap<Bytes, Bytes>,
    deletes: HashSet<Bytes>,
}

impl WriteSet {
    fn new() -> Self {
        Self {
            puts: HashMap::new(),
            deletes: HashSet::new(),
        }
    }

    pub fn put(&mut self, key: Bytes, value: Bytes) {
        self.deletes.remove(&key);
        self.puts.insert(key, value);
    }

    pub fn delete(&mut self, key: Bytes) {
        self.puts.remove(&key);
        self.deletes.insert(key);
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<&Bytes>> {
        if self.deletes.contains(key) {
            return Some(None);
        }
        self.puts.get(key).map(Some)
    }

    pub fn iter_puts(&self) -> impl Iterator<Item = (&Bytes, &Bytes)> {
        self.puts.iter()
    }

    pub fn iter_deletes(&self) -> impl Iterator<Item = &Bytes> {
        self.deletes.iter()
    }
}

pub struct Transaction {
    pub id: u64,
    pub start_ts: u64,
    pub commit_ts: Option<u64>,
    pub isolation: IsolationLevel,
    pub state: TransactionState,
    pub write_set: WriteSet,
    read_set: HashSet<Bytes>,
}

impl Transaction {
    pub fn new(id: u64, start_ts: u64, isolation: IsolationLevel) -> Self {
        Self {
            id,
            start_ts,
            commit_ts: None,
            isolation,
            state: TransactionState::Active,
            write_set: WriteSet::new(),
            read_set: HashSet::new(),
        }
    }

    pub fn record_read(&mut self, key: Bytes) {
        if self.isolation.is_serializable() && !self.isolation.is_read_only() {
            self.read_set.insert(key);
        }
    }

    pub fn read_set(&self) -> &HashSet<Bytes> {
        &self.read_set
    }

    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }
}

pub struct TransactionManager {
    active_txns: Mutex<HashMap<u64, Arc<Mutex<Transaction>>>>,
    committed_txns: Mutex<Vec<(u64, u64, HashSet<Bytes>)>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_txns: Mutex::new(HashMap::new()),
            committed_txns: Mutex::new(Vec::new()),
        }
    }

    pub fn begin(
        &self,
        id: u64,
        start_ts: u64,
        isolation: IsolationLevel,
    ) -> Arc<Mutex<Transaction>> {
        let txn = Arc::new(Mutex::new(Transaction::new(id, start_ts, isolation)));
        self.active_txns.lock().insert(id, txn.clone());
        txn
    }

    pub fn prepare_commit(&self, txn: &mut Transaction, commit_ts: u64) -> Result<()> {
        if txn.isolation == IsolationLevel::Serializable {
            self.validate_serializable(txn, commit_ts)?;
        }
        txn.commit_ts = Some(commit_ts);
        Ok(())
    }

    pub fn is_read_only_txn(&self, txn: &Transaction) -> bool {
        txn.isolation.is_read_only()
    }

    fn validate_serializable(&self, txn: &Transaction, commit_ts: u64) -> Result<()> {
        let committed = self.committed_txns.lock();

        for (other_commit_ts, other_start_ts, other_writes) in committed.iter() {
            if *other_commit_ts > txn.start_ts && *other_start_ts < commit_ts {
                for read_key in txn.read_set() {
                    if other_writes.contains(read_key) {
                        return Err(Error::TransactionConflict.into());
                    }
                }
            }
        }

        Ok(())
    }

    pub fn commit(&self, txn: &mut Transaction) -> Result<()> {
        if !txn.is_active() {
            return Err(Error::TransactionAborted.into());
        }

        let write_keys: HashSet<Bytes> = txn
            .write_set
            .puts
            .keys()
            .cloned()
            .chain(txn.write_set.deletes.iter().cloned())
            .collect();

        let commit_ts = txn.commit_ts.ok_or(Error::TransactionAborted)?;
        self.committed_txns
            .lock()
            .push((commit_ts, txn.start_ts, write_keys));

        txn.state = TransactionState::Committed;
        self.active_txns.lock().remove(&txn.id);

        Ok(())
    }

    pub fn abort(&self, txn: &mut Transaction) {
        txn.state = TransactionState::Aborted;
        self.active_txns.lock().remove(&txn.id);
    }

    #[allow(dead_code)]
    pub fn gc(&self, watermark: u64) {
        let mut committed = self.committed_txns.lock();
        committed.retain(|(commit_ts, _, _)| *commit_ts > watermark);
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
