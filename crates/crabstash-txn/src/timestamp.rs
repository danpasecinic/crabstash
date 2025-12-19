use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TimestampOracle {
    logical: AtomicU64,
    last_physical: AtomicU64,
}

impl TimestampOracle {
    pub fn new() -> Self {
        Self {
            logical: AtomicU64::new(0),
            last_physical: AtomicU64::new(0),
        }
    }

    pub fn get_timestamp(&self) -> u64 {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let last = self.last_physical.load(Ordering::Acquire);

        if physical > last {
            self.last_physical.store(physical, Ordering::Release);
            self.logical.store(0, Ordering::Release);
            physical << 18
        } else {
            let logical = self.logical.fetch_add(1, Ordering::AcqRel);
            (last << 18) | (logical & 0x3FFFF)
        }
    }

    pub fn physical(ts: u64) -> u64 {
        ts >> 18
    }

    pub fn logical(ts: u64) -> u64 {
        ts & 0x3FFFF
    }
}

impl Default for TimestampOracle {
    fn default() -> Self {
        Self::new()
    }
}
