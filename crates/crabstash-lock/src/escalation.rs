use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use tracing::{debug, warn};

#[derive(Debug, Clone, Copy)]
pub struct EscalationConfig {
    pub warning_threshold: usize,
    pub auto_escalate_threshold: usize,
    pub emergency_threshold: usize,
}

impl Default for EscalationConfig {
    fn default() -> Self {
        Self {
            warning_threshold: 1000,
            auto_escalate_threshold: 5000,
            emergency_threshold: 10000,
        }
    }
}

pub struct EscalationState {
    pub key_lock_count: AtomicUsize,
    locked_keys: Mutex<Vec<Bytes>>,
    pub escalated: AtomicBool,
    warning_logged: AtomicBool,
}

impl EscalationState {
    pub fn new() -> Self {
        Self {
            key_lock_count: AtomicUsize::new(0),
            locked_keys: Mutex::new(Vec::new()),
            escalated: AtomicBool::new(false),
            warning_logged: AtomicBool::new(false),
        }
    }

    pub fn record_key_lock(&self, key: Bytes) {
        self.key_lock_count.fetch_add(1, Ordering::Relaxed);
        self.locked_keys.lock().push(key);
    }

    pub fn get_lock_count(&self) -> usize {
        self.key_lock_count.load(Ordering::Relaxed)
    }

    pub fn is_escalated(&self) -> bool {
        self.escalated.load(Ordering::Acquire)
    }

    pub fn mark_escalated(&self) -> bool {
        !self.escalated.swap(true, Ordering::AcqRel)
    }

    pub fn compute_bounding_range(&self) -> Option<(Bound<Bytes>, Bound<Bytes>)> {
        let keys = self.locked_keys.lock();
        if keys.is_empty() {
            return None;
        }

        let min_key = keys.iter().min().cloned()?;
        let max_key = keys.iter().max().cloned()?;

        Some((Bound::Included(min_key), Bound::Included(max_key)))
    }

    pub fn clear(&self) {
        self.key_lock_count.store(0, Ordering::Relaxed);
        self.locked_keys.lock().clear();
        self.escalated.store(false, Ordering::Release);
        self.warning_logged.store(false, Ordering::Release);
    }
}

impl Default for EscalationState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EscalationAction {
    None,
    Warn,
    Escalate,
    ForceEscalate,
}

pub fn check_escalation(count: usize, config: &EscalationConfig) -> EscalationAction {
    if count >= config.emergency_threshold {
        EscalationAction::ForceEscalate
    } else if count >= config.auto_escalate_threshold {
        EscalationAction::Escalate
    } else if count >= config.warning_threshold {
        EscalationAction::Warn
    } else {
        EscalationAction::None
    }
}

pub fn handle_escalation_action(
    txn_id: u64,
    action: EscalationAction,
    state: &EscalationState,
) -> bool {
    match action {
        EscalationAction::None => false,
        EscalationAction::Warn => {
            if !state.warning_logged.swap(true, Ordering::AcqRel) {
                warn!(
                    txn_id,
                    lock_count = state.get_lock_count(),
                    "transaction approaching lock escalation threshold"
                );
            }
            false
        }
        EscalationAction::Escalate | EscalationAction::ForceEscalate => {
            if state.mark_escalated() {
                debug!(
                    txn_id,
                    lock_count = state.get_lock_count(),
                    forced = matches!(action, EscalationAction::ForceEscalate),
                    "escalating to range lock"
                );
                true
            } else {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escalation_thresholds() {
        let config = EscalationConfig::default();

        assert_eq!(check_escalation(500, &config), EscalationAction::None);
        assert_eq!(check_escalation(1000, &config), EscalationAction::Warn);
        assert_eq!(check_escalation(3000, &config), EscalationAction::Warn);
        assert_eq!(check_escalation(5000, &config), EscalationAction::Escalate);
        assert_eq!(
            check_escalation(10000, &config),
            EscalationAction::ForceEscalate
        );
    }

    #[test]
    fn test_escalation_state() {
        let state = EscalationState::new();

        state.record_key_lock(Bytes::from("key1"));
        state.record_key_lock(Bytes::from("key2"));
        state.record_key_lock(Bytes::from("key3"));

        assert_eq!(state.get_lock_count(), 3);
        assert!(!state.is_escalated());

        let range = state.compute_bounding_range();
        assert!(range.is_some());

        let (start, end) = range.unwrap();
        assert_eq!(start, Bound::Included(Bytes::from("key1")));
        assert_eq!(end, Bound::Included(Bytes::from("key3")));
    }

    #[test]
    fn test_mark_escalated_once() {
        let state = EscalationState::new();

        assert!(state.mark_escalated());
        assert!(!state.mark_escalated());
        assert!(state.is_escalated());
    }
}
