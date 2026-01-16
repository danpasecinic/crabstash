use std::cmp::Ordering;
use std::ops::Bound;

use bytes::Bytes;

use crate::lock_mode::LockMode;

#[derive(Debug, Clone)]
pub struct RangeLockEntry {
    pub txn_id: u64,
    pub mode: LockMode,
    pub start: Bound<Bytes>,
    pub end: Bound<Bytes>,
}

impl RangeLockEntry {
    pub fn new(txn_id: u64, mode: LockMode, start: Bound<Bytes>, end: Bound<Bytes>) -> Self {
        Self {
            txn_id,
            mode,
            start,
            end,
        }
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        let after_start = match &self.start {
            Bound::Included(s) => key >= s.as_ref(),
            Bound::Excluded(s) => key > s.as_ref(),
            Bound::Unbounded => true,
        };

        let before_end = match &self.end {
            Bound::Included(e) => key <= e.as_ref(),
            Bound::Excluded(e) => key < e.as_ref(),
            Bound::Unbounded => true,
        };

        after_start && before_end
    }

    pub fn overlaps(&self, other_start: &Bound<Bytes>, other_end: &Bound<Bytes>) -> bool {
        !self.is_before(other_start) && !self.is_after(other_end)
    }

    fn is_before(&self, other_start: &Bound<Bytes>) -> bool {
        match (&self.end, other_start) {
            (Bound::Unbounded, _) => false,
            (_, Bound::Unbounded) => false,
            (Bound::Included(e), Bound::Included(s)) => e < s,
            (Bound::Included(e), Bound::Excluded(s)) => e <= s,
            (Bound::Excluded(e), Bound::Included(s)) => e <= s,
            (Bound::Excluded(e), Bound::Excluded(s)) => e <= s,
        }
    }

    fn is_after(&self, other_end: &Bound<Bytes>) -> bool {
        match (&self.start, other_end) {
            (Bound::Unbounded, _) => false,
            (_, Bound::Unbounded) => false,
            (Bound::Included(s), Bound::Included(e)) => s > e,
            (Bound::Included(s), Bound::Excluded(e)) => s >= e,
            (Bound::Excluded(s), Bound::Included(e)) => s >= e,
            (Bound::Excluded(s), Bound::Excluded(e)) => s >= e,
        }
    }
}

pub struct IntervalTree {
    nodes: Vec<IntervalNode>,
}

struct IntervalNode {
    entry: RangeLockEntry,
    max_end: Bytes,
    left: Option<usize>,
    right: Option<usize>,
}

impl IntervalTree {
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn insert(&mut self, entry: RangeLockEntry) {
        if self.nodes.is_empty() {
            let max_end = Self::bound_to_bytes(&entry.end);
            self.nodes.push(IntervalNode {
                entry,
                max_end,
                left: None,
                right: None,
            });
            return;
        }

        let new_idx = self.nodes.len();
        let max_end = Self::bound_to_bytes(&entry.end);

        let mut idx = 0;
        loop {
            let node_max = &self.nodes[idx].max_end;
            if max_end > *node_max {
                self.nodes[idx].max_end = max_end.clone();
            }

            let cmp = Self::compare_starts(&entry.start, &self.nodes[idx].entry.start);

            if cmp == Ordering::Less {
                if let Some(left_idx) = self.nodes[idx].left {
                    idx = left_idx;
                } else {
                    self.nodes[idx].left = Some(new_idx);
                    break;
                }
            } else if let Some(right_idx) = self.nodes[idx].right {
                idx = right_idx;
            } else {
                self.nodes[idx].right = Some(new_idx);
                break;
            }
        }

        self.nodes.push(IntervalNode {
            entry,
            max_end,
            left: None,
            right: None,
        });
    }

    pub fn find_overlapping(
        &self,
        start: &Bound<Bytes>,
        end: &Bound<Bytes>,
    ) -> Vec<&RangeLockEntry> {
        let mut results = Vec::new();
        if !self.nodes.is_empty() {
            self.find_overlapping_recursive(0, start, end, &mut results);
        }
        results
    }

    fn find_overlapping_recursive<'a>(
        &'a self,
        idx: usize,
        start: &Bound<Bytes>,
        end: &Bound<Bytes>,
        results: &mut Vec<&'a RangeLockEntry>,
    ) {
        let node = &self.nodes[idx];

        if Self::bound_less_than(&node.max_end, start) {
            return;
        }

        if let Some(left_idx) = node.left {
            self.find_overlapping_recursive(left_idx, start, end, results);
        }

        if node.entry.overlaps(start, end) {
            results.push(&node.entry);
        }

        if !Self::bound_greater_than(&node.entry.start, end) {
            if let Some(right_idx) = node.right {
                self.find_overlapping_recursive(right_idx, start, end, results);
            }
        }
    }

    pub fn find_containing_key(&self, key: &[u8]) -> Vec<&RangeLockEntry> {
        let key_bytes = Bytes::copy_from_slice(key);
        let start = Bound::Included(key_bytes.clone());
        let end = Bound::Included(key_bytes);
        self.find_overlapping(&start, &end)
    }

    pub fn remove_by_txn(&mut self, txn_id: u64) {
        self.nodes.retain(|node| node.entry.txn_id != txn_id);
        self.rebuild();
    }

    fn rebuild(&mut self) {
        if self.nodes.is_empty() {
            return;
        }

        let entries: Vec<RangeLockEntry> = self.nodes.drain(..).map(|n| n.entry).collect();
        for entry in entries {
            self.insert(entry);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    fn bound_to_bytes(bound: &Bound<Bytes>) -> Bytes {
        match bound {
            Bound::Included(b) | Bound::Excluded(b) => b.clone(),
            Bound::Unbounded => Bytes::from_static(&[0xFF; 256]),
        }
    }

    fn compare_starts(a: &Bound<Bytes>, b: &Bound<Bytes>) -> Ordering {
        match (a, b) {
            (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
            (Bound::Unbounded, _) => Ordering::Less,
            (_, Bound::Unbounded) => Ordering::Greater,
            (Bound::Included(a), Bound::Included(b)) => a.cmp(b),
            (Bound::Included(a), Bound::Excluded(b)) => {
                if a <= b {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (Bound::Excluded(a), Bound::Included(b)) => {
                if a < b {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (Bound::Excluded(a), Bound::Excluded(b)) => a.cmp(b),
        }
    }

    fn bound_less_than(max_end: &Bytes, start: &Bound<Bytes>) -> bool {
        match start {
            Bound::Unbounded => false,
            Bound::Included(s) => max_end < s,
            Bound::Excluded(s) => max_end <= s,
        }
    }

    fn bound_greater_than(node_start: &Bound<Bytes>, end: &Bound<Bytes>) -> bool {
        match (node_start, end) {
            (Bound::Unbounded, _) => false,
            (_, Bound::Unbounded) => false,
            (Bound::Included(s), Bound::Included(e)) => s > e,
            (Bound::Included(s), Bound::Excluded(e)) => s >= e,
            (Bound::Excluded(s), Bound::Included(e)) => s > e,
            (Bound::Excluded(s), Bound::Excluded(e)) => s >= e,
        }
    }
}

impl Default for IntervalTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_contains_key() {
        let range = RangeLockEntry::new(
            1,
            LockMode::X,
            Bound::Included(Bytes::from("a")),
            Bound::Excluded(Bytes::from("z")),
        );

        assert!(range.contains_key(b"a"));
        assert!(range.contains_key(b"m"));
        assert!(range.contains_key(b"y"));
        assert!(!range.contains_key(b"z"));
        assert!(!range.contains_key(b"0"));
    }

    #[test]
    fn test_range_overlaps() {
        let range = RangeLockEntry::new(
            1,
            LockMode::X,
            Bound::Included(Bytes::from("d")),
            Bound::Excluded(Bytes::from("h")),
        );

        assert!(range.overlaps(
            &Bound::Included(Bytes::from("a")),
            &Bound::Included(Bytes::from("e"))
        ));

        assert!(range.overlaps(
            &Bound::Included(Bytes::from("f")),
            &Bound::Included(Bytes::from("z"))
        ));

        assert!(!range.overlaps(
            &Bound::Included(Bytes::from("a")),
            &Bound::Excluded(Bytes::from("d"))
        ));

        assert!(!range.overlaps(
            &Bound::Included(Bytes::from("h")),
            &Bound::Included(Bytes::from("z"))
        ));
    }

    #[test]
    fn test_interval_tree_insert_find() {
        let mut tree = IntervalTree::new();

        tree.insert(RangeLockEntry::new(
            1,
            LockMode::S,
            Bound::Included(Bytes::from("a")),
            Bound::Excluded(Bytes::from("d")),
        ));

        tree.insert(RangeLockEntry::new(
            2,
            LockMode::S,
            Bound::Included(Bytes::from("f")),
            Bound::Excluded(Bytes::from("j")),
        ));

        let overlapping = tree.find_overlapping(
            &Bound::Included(Bytes::from("b")),
            &Bound::Included(Bytes::from("g")),
        );

        assert_eq!(overlapping.len(), 2);
    }

    #[test]
    fn test_interval_tree_find_containing_key() {
        let mut tree = IntervalTree::new();

        tree.insert(RangeLockEntry::new(
            1,
            LockMode::X,
            Bound::Included(Bytes::from("a")),
            Bound::Excluded(Bytes::from("m")),
        ));

        tree.insert(RangeLockEntry::new(
            2,
            LockMode::X,
            Bound::Included(Bytes::from("n")),
            Bound::Excluded(Bytes::from("z")),
        ));

        let containing = tree.find_containing_key(b"c");
        assert_eq!(containing.len(), 1);
        assert_eq!(containing[0].txn_id, 1);

        let containing = tree.find_containing_key(b"p");
        assert_eq!(containing.len(), 1);
        assert_eq!(containing[0].txn_id, 2);

        let containing = tree.find_containing_key(b"m");
        assert_eq!(containing.len(), 0);
    }

    #[test]
    fn test_remove_by_txn() {
        let mut tree = IntervalTree::new();

        tree.insert(RangeLockEntry::new(
            1,
            LockMode::X,
            Bound::Included(Bytes::from("a")),
            Bound::Excluded(Bytes::from("m")),
        ));

        tree.insert(RangeLockEntry::new(
            2,
            LockMode::X,
            Bound::Included(Bytes::from("n")),
            Bound::Excluded(Bytes::from("z")),
        ));

        assert_eq!(tree.len(), 2);

        tree.remove_by_txn(1);
        assert_eq!(tree.len(), 1);

        let containing = tree.find_containing_key(b"c");
        assert_eq!(containing.len(), 0);
    }
}
