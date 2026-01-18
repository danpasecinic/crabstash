use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabstash_common::{Error, Result};
use parking_lot::{Condvar, Mutex, RwLock};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const COMPRESSION_FLAG: u8 = 0x80;
const COMPRESSION_THRESHOLD: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SyncMode {
    #[default]
    NoSync,
    Sync,
    DataSync,
    GroupSync,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
}

impl TryFrom<u8> for RecordType {
    type Error = Error;

    fn try_from(value: u8) -> std::result::Result<Self, Error> {
        match value & !COMPRESSION_FLAG {
            1 => Ok(RecordType::Put),
            2 => Ok(RecordType::Delete),
            _ => Err(Error::Corruption(format!("Invalid record type: {value}"))),
        }
    }
}

pub struct WalRecord {
    pub record_type: RecordType,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WalOptions {
    pub sync_mode: SyncMode,
    pub enable_compression: bool,
    pub group_commit_interval_ms: u64,
    pub group_commit_size_bytes: usize,
}

struct WalInner {
    writer: BufWriter<File>,
    pending_bytes: usize,
}

struct GroupCommitState {
    notify: Condvar,
    committed_seq: AtomicU64,
    pending_seq: AtomicU64,
}

pub struct Wal {
    inner: Mutex<WalInner>,
    path: std::path::PathBuf,
    options: WalOptions,
    group_commit: Option<Arc<GroupCommitState>>,
    shutdown: Arc<AtomicBool>,
    bg_thread: Option<JoinHandle<()>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Self::create_with_options(path, WalOptions::default())
    }

    pub fn create_with_options(path: impl AsRef<Path>, options: WalOptions) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        Self::new_with_file(file, path.as_ref().to_path_buf(), options)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, WalOptions::default())
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: WalOptions) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;

        Self::new_with_file(file, path.as_ref().to_path_buf(), options)
    }

    fn new_with_file(file: File, path: std::path::PathBuf, options: WalOptions) -> Result<Self> {
        let group_commit = if options.sync_mode == SyncMode::GroupSync {
            Some(Arc::new(GroupCommitState {
                notify: Condvar::new(),
                committed_seq: AtomicU64::new(0),
                pending_seq: AtomicU64::new(0),
            }))
        } else {
            None
        };

        let shutdown = Arc::new(AtomicBool::new(false));

        let bg_thread = if options.sync_mode == SyncMode::GroupSync {
            let shutdown_clone = shutdown.clone();
            let interval_ms = options.group_commit_interval_ms.max(1);

            Some(thread::spawn(move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(interval_ms));
                }
            }))
        } else {
            None
        };

        Ok(Self {
            inner: Mutex::new(WalInner {
                writer: BufWriter::new(file),
                pending_bytes: 0,
            }),
            path,
            options,
            group_commit,
            shutdown,
            bg_thread,
        })
    }

    pub fn append(&self, record: &WalRecord) -> Result<()> {
        self.append_internal(record, true)
    }

    fn append_internal(&self, record: &WalRecord, should_sync: bool) -> Result<()> {
        let buf = self.encode_record(record);
        let checksum = crc32fast::hash(&buf);

        let mut inner = self.inner.lock();
        inner.writer.write_all(&checksum.to_le_bytes())?;
        inner.writer.write_all(&(buf.len() as u32).to_le_bytes())?;
        inner.writer.write_all(&buf)?;
        inner.pending_bytes += 8 + buf.len();

        if should_sync {
            self.maybe_sync_internal(&mut inner)?;
        }

        Ok(())
    }

    pub fn append_batch(&self, records: &[WalRecord]) -> Result<()> {
        let mut inner = self.inner.lock();

        for record in records {
            let buf = self.encode_record(record);
            let checksum = crc32fast::hash(&buf);
            inner.writer.write_all(&checksum.to_le_bytes())?;
            inner.writer.write_all(&(buf.len() as u32).to_le_bytes())?;
            inner.writer.write_all(&buf)?;
            inner.pending_bytes += 8 + buf.len();
        }

        self.maybe_sync_internal(&mut inner)?;
        Ok(())
    }

    fn encode_record(&self, record: &WalRecord) -> BytesMut {
        let mut buf = BytesMut::new();
        let mut record_type_byte = record.record_type as u8;

        let mut payload = BytesMut::new();
        payload.put_u64_le(record.timestamp);
        payload.put_u32_le(record.key.len() as u32);
        payload.put_slice(&record.key);
        if let Some(ref value) = record.value {
            payload.put_u32_le(value.len() as u32);
            payload.put_slice(value);
        }

        if self.options.enable_compression && payload.len() >= COMPRESSION_THRESHOLD {
            let compressed = lz4_flex::compress_prepend_size(&payload);
            if compressed.len() < payload.len() {
                record_type_byte |= COMPRESSION_FLAG;
                buf.put_u8(record_type_byte);
                buf.put_slice(&compressed);
                return buf;
            }
        }

        buf.put_u8(record_type_byte);
        buf.put_slice(&payload);
        buf
    }

    fn maybe_sync_internal(&self, inner: &mut WalInner) -> Result<()> {
        match self.options.sync_mode {
            SyncMode::NoSync => {}
            SyncMode::Sync => {
                inner.writer.flush()?;
                inner.writer.get_ref().sync_all()?;
                inner.pending_bytes = 0;
            }
            SyncMode::DataSync => {
                inner.writer.flush()?;
                inner.writer.get_ref().sync_data()?;
                inner.pending_bytes = 0;
            }
            SyncMode::GroupSync => {
                let should_sync = inner.pending_bytes >= self.options.group_commit_size_bytes;
                if should_sync {
                    inner.writer.flush()?;
                    inner.writer.get_ref().sync_all()?;
                    inner.pending_bytes = 0;

                    if let Some(ref gc) = self.group_commit {
                        let seq = gc.pending_seq.load(Ordering::Acquire);
                        gc.committed_seq.store(seq, Ordering::Release);
                        gc.notify.notify_all();
                    }
                }
            }
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.writer.flush()?;
        inner.writer.get_ref().sync_all()?;
        inner.pending_bytes = 0;

        if let Some(ref gc) = self.group_commit {
            let seq = gc.pending_seq.load(Ordering::Acquire);
            gc.committed_seq.store(seq, Ordering::Release);
            gc.notify.notify_all();
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.writer.flush()?;
        Ok(())
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
        Self::recover_with_compression(path, true)
    }

    pub fn recover_with_compression(
        path: impl AsRef<Path>,
        decompress: bool,
    ) -> Result<Vec<WalRecord>> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        loop {
            let mut header = [0u8; 8];
            match reader.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let checksum = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;

            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;

            if crc32fast::hash(&data) != checksum {
                return Err(Error::Corruption("WAL checksum mismatch".into()).into());
            }

            let record_type_byte = data[0];
            let is_compressed = record_type_byte & COMPRESSION_FLAG != 0;
            let record_type = RecordType::try_from(record_type_byte)?;

            let payload = if is_compressed && decompress {
                lz4_flex::decompress_size_prepended(&data[1..])
                    .map_err(|e| Error::Corruption(format!("WAL decompression failed: {e}")))?
            } else {
                data[1..].to_vec()
            };

            let mut buf = &payload[..];
            let timestamp = buf.get_u64_le();
            let key_len = buf.get_u32_le() as usize;
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            buf.advance(key_len);

            let value = if record_type == RecordType::Put {
                let value_len = buf.get_u32_le() as usize;
                Some(Bytes::copy_from_slice(&buf[..value_len]))
            } else {
                None
            };

            records.push(WalRecord {
                record_type,
                key,
                value,
                timestamp,
            });
        }

        Ok(records)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn options(&self) -> &WalOptions {
        &self.options
    }

    pub fn pending_bytes(&self) -> usize {
        self.inner.lock().pending_bytes
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.bg_thread.take() {
            let _ = handle.join();
        }
        let _ = self.sync();
    }
}

const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
const WAL_SEGMENT_PREFIX: &str = "wal_";
const WAL_SEGMENT_EXT: &str = ".log";

#[derive(Debug, Clone)]
pub struct WalManagerOptions {
    pub wal_options: WalOptions,
    pub segment_size_bytes: u64,
    pub archive_dir: Option<PathBuf>,
    pub min_segments_to_keep: usize,
}

impl Default for WalManagerOptions {
    fn default() -> Self {
        Self {
            wal_options: WalOptions::default(),
            segment_size_bytes: DEFAULT_SEGMENT_SIZE,
            archive_dir: None,
            min_segments_to_keep: 2,
        }
    }
}

struct WalManagerInner {
    current_wal: Wal,
    current_segment: u64,
    current_size: u64,
}

pub struct WalManager {
    inner: RwLock<WalManagerInner>,
    wal_dir: PathBuf,
    options: WalManagerOptions,
    first_segment: AtomicU64,
}

impl WalManager {
    pub fn open(wal_dir: impl AsRef<Path>, options: WalManagerOptions) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        fs::create_dir_all(&wal_dir)?;

        if let Some(ref archive_dir) = options.archive_dir {
            fs::create_dir_all(archive_dir)?;
        }

        let (segment_id, first_segment) = Self::find_latest_segment(&wal_dir)?;
        let wal_path = Self::segment_path(&wal_dir, segment_id);
        let current_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        let current_wal = Wal::open_with_options(&wal_path, options.wal_options.clone())?;

        Ok(Self {
            inner: RwLock::new(WalManagerInner {
                current_wal,
                current_segment: segment_id,
                current_size,
            }),
            wal_dir,
            options,
            first_segment: AtomicU64::new(first_segment),
        })
    }

    fn find_latest_segment(wal_dir: &Path) -> Result<(u64, u64)> {
        let mut max_segment = 0u64;
        let mut min_segment = u64::MAX;
        let mut found = false;

        if let Ok(entries) = fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(id) = Self::parse_segment_name(&name_str) {
                    max_segment = max_segment.max(id);
                    min_segment = min_segment.min(id);
                    found = true;
                }
            }
        }

        if !found {
            Ok((1, 1))
        } else {
            Ok((max_segment, min_segment))
        }
    }

    fn parse_segment_name(name: &str) -> Option<u64> {
        if name.starts_with(WAL_SEGMENT_PREFIX) && name.ends_with(WAL_SEGMENT_EXT) {
            let num_str = name
                .strip_prefix(WAL_SEGMENT_PREFIX)?
                .strip_suffix(WAL_SEGMENT_EXT)?;
            num_str.parse().ok()
        } else {
            None
        }
    }

    fn segment_path(wal_dir: &Path, segment_id: u64) -> PathBuf {
        wal_dir.join(format!(
            "{}{:08}{}",
            WAL_SEGMENT_PREFIX, segment_id, WAL_SEGMENT_EXT
        ))
    }

    pub fn append(&self, record: &WalRecord) -> Result<()> {
        let estimated_size = 8
            + 1
            + 8
            + 4
            + record.key.len()
            + record.value.as_ref().map(|v| 4 + v.len()).unwrap_or(0);

        {
            let inner = self.inner.read();
            if inner.current_size + estimated_size as u64 <= self.options.segment_size_bytes {
                inner.current_wal.append(record)?;
                drop(inner);

                let mut inner = self.inner.write();
                inner.current_size += estimated_size as u64;
                return Ok(());
            }
        }

        self.rotate_segment()?;

        let mut inner = self.inner.write();
        inner.current_wal.append(record)?;
        inner.current_size += estimated_size as u64;
        Ok(())
    }

    pub fn append_batch(&self, records: &[WalRecord]) -> Result<()> {
        let estimated_size: usize = records
            .iter()
            .map(|r| {
                8 + 1 + 8 + 4 + r.key.len() + r.value.as_ref().map(|v| 4 + v.len()).unwrap_or(0)
            })
            .sum();

        {
            let inner = self.inner.read();
            if inner.current_size + estimated_size as u64 <= self.options.segment_size_bytes {
                inner.current_wal.append_batch(records)?;
                drop(inner);

                let mut inner = self.inner.write();
                inner.current_size += estimated_size as u64;
                return Ok(());
            }
        }

        self.rotate_segment()?;

        let mut inner = self.inner.write();
        inner.current_wal.append_batch(records)?;
        inner.current_size += estimated_size as u64;
        inner.current_size += estimated_size as u64;
        Ok(())
    }

    fn rotate_segment(&self) -> Result<()> {
        let mut inner = self.inner.write();

        inner.current_wal.sync()?;

        let old_segment = inner.current_segment;
        let new_segment = old_segment + 1;
        let new_path = Self::segment_path(&self.wal_dir, new_segment);
        let new_wal = Wal::create_with_options(&new_path, self.options.wal_options.clone())?;

        inner.current_wal = new_wal;
        inner.current_segment = new_segment;
        inner.current_size = 0;

        drop(inner);

        if self.options.archive_dir.is_some() {
            self.archive_old_segments()?;
        }

        Ok(())
    }

    fn archive_old_segments(&self) -> Result<()> {
        let archive_dir = match &self.options.archive_dir {
            Some(dir) => dir,
            None => return Ok(()),
        };

        let current_segment = self.inner.read().current_segment;
        let first_segment = self.first_segment.load(Ordering::Acquire);

        if current_segment <= first_segment + self.options.min_segments_to_keep as u64 {
            return Ok(());
        }

        let archive_up_to = current_segment - self.options.min_segments_to_keep as u64;

        for segment_id in first_segment..archive_up_to {
            let src_path = Self::segment_path(&self.wal_dir, segment_id);
            if src_path.exists() {
                let dst_path = archive_dir.join(format!(
                    "{}{:08}{}",
                    WAL_SEGMENT_PREFIX, segment_id, WAL_SEGMENT_EXT
                ));
                fs::rename(&src_path, &dst_path)?;
            }
        }

        self.first_segment.store(archive_up_to, Ordering::Release);
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.read().current_wal.sync()
    }

    pub fn recover_all(wal_dir: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
        Self::recover_from_timestamp(wal_dir, 0)
    }

    pub fn recover_from_timestamp(
        wal_dir: impl AsRef<Path>,
        min_timestamp: u64,
    ) -> Result<Vec<WalRecord>> {
        let wal_dir = wal_dir.as_ref();
        let mut segments: Vec<u64> = Vec::new();

        if let Ok(entries) = fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(id) = Self::parse_segment_name(&name_str) {
                    segments.push(id);
                }
            }
        }

        segments.sort();

        let mut all_records = Vec::new();

        for segment_id in segments {
            let path = Self::segment_path(wal_dir, segment_id);
            match Wal::recover(&path) {
                Ok(records) => {
                    for record in records {
                        if record.timestamp >= min_timestamp {
                            all_records.push(record);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to recover WAL segment {}: {}", segment_id, e);
                }
            }
        }

        Ok(all_records)
    }

    pub fn recover_pitr(
        wal_dir: impl AsRef<Path>,
        archive_dir: impl AsRef<Path>,
        target_timestamp: u64,
    ) -> Result<Vec<WalRecord>> {
        let wal_dir = wal_dir.as_ref();
        let archive_dir = archive_dir.as_ref();

        let mut segments: Vec<(u64, PathBuf)> = Vec::new();

        if let Ok(entries) = fs::read_dir(archive_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(id) = Self::parse_segment_name(&name_str) {
                    segments.push((id, entry.path()));
                }
            }
        }

        if let Ok(entries) = fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(id) = Self::parse_segment_name(&name_str) {
                    segments.push((id, entry.path()));
                }
            }
        }

        segments.sort_by_key(|(id, _)| *id);
        segments.dedup_by_key(|(id, _)| *id);

        let mut all_records = Vec::new();

        for (_, path) in segments {
            match Wal::recover(&path) {
                Ok(records) => {
                    for record in records {
                        if record.timestamp <= target_timestamp {
                            all_records.push(record);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to recover WAL segment {:?}: {}", path, e);
                }
            }
        }

        Ok(all_records)
    }

    pub fn current_segment(&self) -> u64 {
        self.inner.read().current_segment
    }

    pub fn first_segment(&self) -> u64 {
        self.first_segment.load(Ordering::Acquire)
    }

    pub fn list_segments(&self) -> Vec<u64> {
        let mut segments = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(id) = Self::parse_segment_name(&name_str) {
                    segments.push(id);
                }
            }
        }
        segments.sort();
        segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_basic_write_recover() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = Wal::create(&wal_path).unwrap();
            wal.append(&WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from("key1"),
                value: Some(Bytes::from("value1")),
                timestamp: 1,
            })
            .unwrap();
            wal.append(&WalRecord {
                record_type: RecordType::Delete,
                key: Bytes::from("key2"),
                value: None,
                timestamp: 2,
            })
            .unwrap();
            wal.sync().unwrap();
        }

        let records = Wal::recover(&wal_path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key.as_ref(), b"key1");
        assert_eq!(records[0].value.as_ref().unwrap().as_ref(), b"value1");
        assert_eq!(records[1].key.as_ref(), b"key2");
        assert!(records[1].value.is_none());
    }

    #[test]
    fn test_wal_compression() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("compressed.wal");

        let options = WalOptions {
            enable_compression: true,
            ..Default::default()
        };

        let large_value = "x".repeat(1024);

        {
            let wal = Wal::create_with_options(&wal_path, options).unwrap();
            wal.append(&WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from("key"),
                value: Some(Bytes::from(large_value.clone())),
                timestamp: 1,
            })
            .unwrap();
            wal.sync().unwrap();
        }

        let records = Wal::recover(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].value.as_ref().unwrap().as_ref(),
            large_value.as_bytes()
        );
    }

    #[test]
    fn test_wal_sync_modes() {
        let dir = tempdir().unwrap();

        for sync_mode in [SyncMode::NoSync, SyncMode::Sync, SyncMode::DataSync] {
            let wal_path = dir.path().join(format!("wal_{:?}.wal", sync_mode));
            let options = WalOptions {
                sync_mode,
                ..Default::default()
            };

            let wal = Wal::create_with_options(&wal_path, options).unwrap();
            wal.append(&WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from("key"),
                value: Some(Bytes::from("value")),
                timestamp: 1,
            })
            .unwrap();
            wal.sync().unwrap();

            let records = Wal::recover(&wal_path).unwrap();
            assert_eq!(records.len(), 1);
        }
    }

    #[test]
    fn test_wal_group_sync() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("group.wal");

        let options = WalOptions {
            sync_mode: SyncMode::GroupSync,
            group_commit_size_bytes: 100,
            group_commit_interval_ms: 10,
            ..Default::default()
        };

        let wal = Wal::create_with_options(&wal_path, options).unwrap();

        for i in 0..10 {
            wal.append(&WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from(format!("key{}", i)),
                value: Some(Bytes::from(format!("value{}", i))),
                timestamp: i as u64,
            })
            .unwrap();
        }

        wal.sync().unwrap();
        drop(wal);

        let records = Wal::recover(&wal_path).unwrap();
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn test_wal_batch() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("batch.wal");

        let wal = Wal::create(&wal_path).unwrap();

        let records: Vec<WalRecord> = (0..5)
            .map(|i| WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from(format!("key{}", i)),
                value: Some(Bytes::from(format!("value{}", i))),
                timestamp: i as u64,
            })
            .collect();

        wal.append_batch(&records).unwrap();
        wal.sync().unwrap();
        drop(wal);

        let recovered = Wal::recover(&wal_path).unwrap();
        assert_eq!(recovered.len(), 5);
    }

    #[test]
    fn test_wal_manager_basic() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mgr = WalManager::open(&wal_dir, WalManagerOptions::default()).unwrap();

        for i in 0..5 {
            mgr.append(&WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from(format!("key{}", i)),
                value: Some(Bytes::from(format!("value{}", i))),
                timestamp: i as u64,
            })
            .unwrap();
        }

        mgr.sync().unwrap();
        drop(mgr);

        let records = WalManager::recover_all(&wal_dir).unwrap();
        assert_eq!(records.len(), 5);
    }

    #[test]
    fn test_wal_manager_segment_rotation() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let options = WalManagerOptions {
            segment_size_bytes: 100,
            ..Default::default()
        };

        let mgr = WalManager::open(&wal_dir, options).unwrap();

        for i in 0..20 {
            mgr.append(&WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from(format!("key{}", i)),
                value: Some(Bytes::from("x".repeat(20))),
                timestamp: i as u64,
            })
            .unwrap();
        }

        mgr.sync().unwrap();

        let segments = mgr.list_segments();
        assert!(segments.len() > 1, "Expected multiple segments");

        drop(mgr);

        let records = WalManager::recover_all(&wal_dir).unwrap();
        assert_eq!(records.len(), 20);
    }

    #[test]
    fn test_wal_manager_archiving() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let archive_dir = dir.path().join("archive");

        let options = WalManagerOptions {
            segment_size_bytes: 100,
            archive_dir: Some(archive_dir.clone()),
            min_segments_to_keep: 1,
            ..Default::default()
        };

        let mgr = WalManager::open(&wal_dir, options).unwrap();

        for i in 0..50 {
            mgr.append(&WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from(format!("key{}", i)),
                value: Some(Bytes::from("x".repeat(20))),
                timestamp: i as u64,
            })
            .unwrap();
        }

        mgr.sync().unwrap();

        let archived: Vec<_> = fs::read_dir(&archive_dir).unwrap().flatten().collect();
        assert!(!archived.is_empty(), "Expected archived segments");

        drop(mgr);

        let records = WalManager::recover_pitr(&wal_dir, &archive_dir, u64::MAX).unwrap();
        assert_eq!(records.len(), 50);
    }

    #[test]
    fn test_wal_manager_pitr() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let options = WalManagerOptions {
            segment_size_bytes: 100,
            ..Default::default()
        };

        let mgr = WalManager::open(&wal_dir, options).unwrap();

        for i in 0..30 {
            mgr.append(&WalRecord {
                record_type: RecordType::Put,
                key: Bytes::from(format!("key{}", i)),
                value: Some(Bytes::from("value")),
                timestamp: i as u64 * 10,
            })
            .unwrap();
        }

        mgr.sync().unwrap();
        drop(mgr);

        let records = WalManager::recover_from_timestamp(&wal_dir, 100).unwrap();
        assert!(records.iter().all(|r| r.timestamp >= 100));
        assert!(records.len() < 30);
    }
}
