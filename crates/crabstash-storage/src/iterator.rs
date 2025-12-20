use bytes::Bytes;
use crabstash_common::{Key, Result};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ops::Bound;

pub struct Entry {
    pub key: Key,
    pub value: Option<Bytes>,
}

pub trait StorageIterator {
    fn key(&self) -> &Key;
    fn value(&self) -> Option<&Bytes>;
    fn is_valid(&self) -> bool;
    fn next(&mut self) -> Result<()>;
}

struct HeapEntry<I: StorageIterator> {
    iter: I,
    index: usize,
}

impl<I: StorageIterator> Eq for HeapEntry<I> {}

impl<I: StorageIterator> PartialEq for HeapEntry<I> {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key()
    }
}

impl<I: StorageIterator> Ord for HeapEntry<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .iter
            .key()
            .cmp(self.iter.key())
            .then_with(|| self.index.cmp(&other.index))
    }
}

impl<I: StorageIterator> PartialOrd for HeapEntry<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MergeIterator<I: StorageIterator> {
    heap: BinaryHeap<HeapEntry<I>>,
    current: Option<HeapEntry<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn new(iters: Vec<I>) -> Self {
        let mut heap = BinaryHeap::new();
        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapEntry { iter, index });
            }
        }
        let current = heap.pop();
        Self { heap, current }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &Key {
        self.current.as_ref().unwrap().iter.key()
    }

    fn value(&self) -> Option<&Bytes> {
        self.current.as_ref().unwrap().iter.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let mut entry = self.current.take().unwrap();
        let current_key = entry.iter.key().clone();

        entry.iter.next()?;
        if entry.iter.is_valid() {
            self.heap.push(entry);
        }

        while let Some(top) = self.heap.peek() {
            if top.iter.key().data() != current_key.data() {
                break;
            }
            let mut dup = self.heap.pop().unwrap();
            dup.iter.next()?;
            if dup.iter.is_valid() {
                self.heap.push(dup);
            }
        }

        self.current = self.heap.pop();
        Ok(())
    }
}

pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    use_a: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn new(a: A, b: B) -> Self {
        let use_a = Self::choose_a(&a, &b);
        Self { a, b, use_a }
    }

    fn choose_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            return false;
        }
        if !b.is_valid() {
            return true;
        }
        a.key() <= b.key()
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &Key {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> Option<&Bytes> {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            if self.b.is_valid() && self.a.key().data() == self.b.key().data() {
                self.b.next()?;
            }
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.use_a = Self::choose_a(&self.a, &self.b);
        Ok(())
    }
}

pub struct BoundedIterator<I: StorageIterator> {
    inner: I,
    end_bound: Bound<Bytes>,
    valid: bool,
}

impl<I: StorageIterator> BoundedIterator<I> {
    pub fn new(inner: I, end_bound: Bound<Bytes>) -> Self {
        let mut iter = Self {
            inner,
            end_bound,
            valid: true,
        };
        iter.check_bound();
        iter
    }

    fn check_bound(&mut self) {
        if !self.inner.is_valid() {
            self.valid = false;
            return;
        }

        let key = self.inner.key().data();
        self.valid = match &self.end_bound {
            Bound::Unbounded => true,
            Bound::Included(end) => key <= end.as_ref(),
            Bound::Excluded(end) => key < end.as_ref(),
        };
    }
}

impl<I: StorageIterator> StorageIterator for BoundedIterator<I> {
    fn key(&self) -> &Key {
        self.inner.key()
    }

    fn value(&self) -> Option<&Bytes> {
        self.inner.value()
    }

    fn is_valid(&self) -> bool {
        self.valid && self.inner.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.check_bound();
        Ok(())
    }
}
