use bytes::Bytes;
use std::cmp::Ordering;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Key {
    data: Bytes,
    timestamp: u64,
}

impl Key {
    pub fn new(data: impl Into<Bytes>, timestamp: u64) -> Self {
        Self {
            data: data.into(),
            timestamp,
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.data
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.data.cmp(&other.data) {
            Ordering::Equal => other.timestamp.cmp(&self.timestamp),
            ord => ord,
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
