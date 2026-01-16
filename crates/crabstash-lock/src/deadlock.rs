use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::Mutex;
use tracing::{debug, info, warn};

pub struct WaitForGraph {
    edges: HashMap<u64, HashSet<u64>>,
    txn_info: HashMap<u64, TxnInfo>,
}

#[derive(Clone)]
pub struct TxnInfo {
    pub start_ts: u64,
    pub lock_count: usize,
    pub priority: u32,
}

impl Default for TxnInfo {
    fn default() -> Self {
        Self {
            start_ts: 0,
            lock_count: 0,
            priority: u32::MAX,
        }
    }
}

impl WaitForGraph {
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
            txn_info: HashMap::new(),
        }
    }

    pub fn register_txn(&mut self, txn_id: u64, start_ts: u64, priority: u32) {
        self.txn_info.insert(txn_id, TxnInfo {
            start_ts,
            lock_count: 0,
            priority,
        });
    }

    pub fn unregister_txn(&mut self, txn_id: u64) {
        self.edges.remove(&txn_id);
        self.txn_info.remove(&txn_id);
        for waiters in self.edges.values_mut() {
            waiters.remove(&txn_id);
        }
    }

    pub fn add_wait_edge(&mut self, waiter: u64, holder: u64) {
        if waiter == holder {
            return;
        }
        self.edges.entry(waiter).or_default().insert(holder);
        debug!(waiter, holder, "added wait edge");
    }

    pub fn remove_wait_edge(&mut self, waiter: u64, holder: u64) {
        if let Some(holders) = self.edges.get_mut(&waiter) {
            holders.remove(&holder);
            if holders.is_empty() {
                self.edges.remove(&waiter);
            }
        }
    }

    pub fn remove_all_waits(&mut self, waiter: u64) {
        self.edges.remove(&waiter);
    }

    pub fn increment_lock_count(&mut self, txn_id: u64) {
        if let Some(info) = self.txn_info.get_mut(&txn_id) {
            info.lock_count += 1;
        }
    }

    pub fn decrement_lock_count(&mut self, txn_id: u64) {
        if let Some(info) = self.txn_info.get_mut(&txn_id) {
            info.lock_count = info.lock_count.saturating_sub(1);
        }
    }

    pub fn detect_deadlock(&self) -> Option<Vec<u64>> {
        let mut index_counter = 0;
        let mut stack = Vec::new();
        let mut indices: HashMap<u64, usize> = HashMap::new();
        let mut lowlinks: HashMap<u64, usize> = HashMap::new();
        let mut on_stack: HashSet<u64> = HashSet::new();
        let mut sccs: Vec<Vec<u64>> = Vec::new();

        for &v in self.edges.keys() {
            if !indices.contains_key(&v) {
                self.strongconnect(
                    v,
                    &mut index_counter,
                    &mut stack,
                    &mut indices,
                    &mut lowlinks,
                    &mut on_stack,
                    &mut sccs,
                );
            }
        }

        for scc in sccs {
            if scc.len() > 1 {
                info!(?scc, "deadlock detected");
                return Some(self.select_victims(&scc));
            }
        }

        None
    }

    fn strongconnect(
        &self,
        v: u64,
        index_counter: &mut usize,
        stack: &mut Vec<u64>,
        indices: &mut HashMap<u64, usize>,
        lowlinks: &mut HashMap<u64, usize>,
        on_stack: &mut HashSet<u64>,
        sccs: &mut Vec<Vec<u64>>,
    ) {
        indices.insert(v, *index_counter);
        lowlinks.insert(v, *index_counter);
        *index_counter += 1;
        stack.push(v);
        on_stack.insert(v);

        if let Some(successors) = self.edges.get(&v) {
            for &w in successors {
                if !indices.contains_key(&w) {
                    self.strongconnect(w, index_counter, stack, indices, lowlinks, on_stack, sccs);
                    let low_w = *lowlinks.get(&w).unwrap();
                    let low_v = lowlinks.get_mut(&v).unwrap();
                    *low_v = (*low_v).min(low_w);
                } else if on_stack.contains(&w) {
                    let idx_w = *indices.get(&w).unwrap();
                    let low_v = lowlinks.get_mut(&v).unwrap();
                    *low_v = (*low_v).min(idx_w);
                }
            }
        }

        let low_v = *lowlinks.get(&v).unwrap();
        let idx_v = *indices.get(&v).unwrap();

        if low_v == idx_v {
            let mut scc = Vec::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack.remove(&w);
                scc.push(w);
                if w == v {
                    break;
                }
            }
            sccs.push(scc);
        }
    }

    fn select_victims(&self, cycle: &[u64]) -> Vec<u64> {
        let mut candidates: Vec<_> = cycle
            .iter()
            .filter_map(|&id| self.txn_info.get(&id).map(|info| (id, info.clone())))
            .collect();

        candidates.sort_by(|a, b| {
            a.1.priority
                .cmp(&b.1.priority)
                .reverse()
                .then_with(|| a.1.start_ts.cmp(&b.1.start_ts).reverse())
                .then_with(|| a.1.lock_count.cmp(&b.1.lock_count))
        });

        vec![candidates.first().map(|(id, _)| *id).unwrap_or(cycle[0])]
    }
}

impl Default for WaitForGraph {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DeadlockDetector {
    graph: std::sync::Arc<Mutex<WaitForGraph>>,
    shutdown: AtomicBool,
    check_interval: Duration,
    abort_sender: Mutex<Option<mpsc::Sender<u64>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl DeadlockDetector {
    pub fn new(
        graph: std::sync::Arc<Mutex<WaitForGraph>>,
        check_interval: Duration,
    ) -> (Self, mpsc::Receiver<u64>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                graph,
                shutdown: AtomicBool::new(false),
                check_interval,
                abort_sender: Mutex::new(Some(tx)),
                handle: Mutex::new(None),
            },
            rx,
        )
    }

    pub fn start(&self) {
        let graph = self.graph.clone();
        let interval = self.check_interval;
        let sender = self.abort_sender.lock().clone();

        let shutdown = &self.shutdown as *const AtomicBool;
        let shutdown_ptr = shutdown as usize;

        let handle = thread::spawn(move || {
            let shutdown = unsafe { &*(shutdown_ptr as *const AtomicBool) };

            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(interval);

                let graph_guard = graph.lock();
                if let Some(victims) = graph_guard.detect_deadlock() {
                    drop(graph_guard);
                    if let Some(ref sender) = sender {
                        for victim in victims {
                            warn!(txn_id = victim, "aborting deadlock victim");
                            let _ = sender.send(victim);
                        }
                    }
                }
            }
        });

        *self.handle.lock() = Some(handle);
    }

    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for DeadlockDetector {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_deadlock() {
        let mut graph = WaitForGraph::new();
        graph.register_txn(1, 100, 0);
        graph.register_txn(2, 200, 0);
        graph.add_wait_edge(1, 2);

        assert!(graph.detect_deadlock().is_none());
    }

    #[test]
    fn test_simple_deadlock() {
        let mut graph = WaitForGraph::new();
        graph.register_txn(1, 100, 0);
        graph.register_txn(2, 200, 0);
        graph.add_wait_edge(1, 2);
        graph.add_wait_edge(2, 1);

        let victims = graph.detect_deadlock();
        assert!(victims.is_some());
        let victims = victims.unwrap();
        assert_eq!(victims.len(), 1);
        assert!(victims.contains(&1) || victims.contains(&2));
    }

    #[test]
    fn test_three_way_deadlock() {
        let mut graph = WaitForGraph::new();
        graph.register_txn(1, 100, 0);
        graph.register_txn(2, 200, 0);
        graph.register_txn(3, 300, 0);
        graph.add_wait_edge(1, 2);
        graph.add_wait_edge(2, 3);
        graph.add_wait_edge(3, 1);

        let victims = graph.detect_deadlock();
        assert!(victims.is_some());
    }

    #[test]
    fn test_victim_selection_youngest() {
        let mut graph = WaitForGraph::new();
        graph.register_txn(1, 100, 0);
        graph.register_txn(2, 200, 0);
        graph.add_wait_edge(1, 2);
        graph.add_wait_edge(2, 1);

        let victims = graph.detect_deadlock().unwrap();
        assert_eq!(victims, vec![2]);
    }

    #[test]
    fn test_unregister_breaks_cycle() {
        let mut graph = WaitForGraph::new();
        graph.register_txn(1, 100, 0);
        graph.register_txn(2, 200, 0);
        graph.add_wait_edge(1, 2);
        graph.add_wait_edge(2, 1);

        assert!(graph.detect_deadlock().is_some());

        graph.unregister_txn(1);
        assert!(graph.detect_deadlock().is_none());
    }
}
