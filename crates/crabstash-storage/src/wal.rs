use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabstash_common::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

#[allow(dead_code)]
const WAL_RECORD_HEADER_SIZE: usize = 9;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
}

impl TryFrom<u8> for RecordType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(RecordType::Put),
            2 => Ok(RecordType::Delete),
            _ => Err(Error::Corruption(format!("Invalid record type: {}", value))),
        }
    }
}

pub struct WalRecord {
    pub record_type: RecordType,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub timestamp: u64,
}

pub struct Wal {
    writer: BufWriter<File>,
    path: std::path::PathBuf,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        Ok(Self {
            writer: BufWriter::new(file),
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;

        Ok(Self {
            writer: BufWriter::new(file),
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn append(&mut self, record: &WalRecord) -> Result<()> {
        let mut buf = BytesMut::new();

        buf.put_u8(record.record_type as u8);
        buf.put_u64_le(record.timestamp);
        buf.put_u32_le(record.key.len() as u32);
        buf.put_slice(&record.key);

        if let Some(ref value) = record.value {
            buf.put_u32_le(value.len() as u32);
            buf.put_slice(value);
        }

        let checksum = crc32fast::hash(&buf);
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&(buf.len() as u32).to_le_bytes())?;
        self.writer.write_all(&buf)?;

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
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
                return Err(Error::Corruption("WAL checksum mismatch".into()));
            }

            let mut buf = &data[..];
            let record_type = RecordType::try_from(buf.get_u8())?;
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
}
