use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum BatchOperation {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
    approximate_size: usize,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            operations: Vec::with_capacity(capacity),
            approximate_size: 0,
        }
    }

    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        let key = key.into();
        let value = value.into();
        self.approximate_size += key.len() + value.len();
        self.operations.push(BatchOperation::Put { key, value });
    }

    pub fn delete(&mut self, key: impl Into<Bytes>) {
        let key = key.into();
        self.approximate_size += key.len();
        self.operations.push(BatchOperation::Delete { key });
    }

    pub fn clear(&mut self) {
        self.operations.clear();
        self.approximate_size = 0;
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    pub fn iter(&self) -> impl Iterator<Item = &BatchOperation> {
        self.operations.iter()
    }

    pub(crate) fn into_operations(self) -> Vec<BatchOperation> {
        self.operations
    }
}
