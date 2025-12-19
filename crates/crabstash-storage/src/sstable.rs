use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabstash_common::{Error, Key, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::bloom::BloomFilter;

const SSTABLE_MAGIC: u32 = 0x53535442;
const BLOCK_SIZE: usize = 4096;

#[derive(Debug)]
pub struct BlockMeta {
    pub offset: u64,
    pub length: u32,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

pub struct SSTable {
    file: File,
    path: PathBuf,
    block_metas: Vec<BlockMeta>,
    bloom: BloomFilter,
    pub id: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
}

impl SSTable {
    pub fn open(id: u64, path: impl AsRef<Path>) -> Result<Self> {
        let mut file = File::open(path.as_ref())?;

        file.seek(SeekFrom::End(-4))?;
        let mut magic_buf = [0u8; 4];
        file.read_exact(&mut magic_buf)?;
        let magic = u32::from_le_bytes(magic_buf);
        if magic != SSTABLE_MAGIC {
            return Err(Error::Corruption("Invalid SSTable magic".into()));
        }

        file.seek(SeekFrom::End(-12))?;
        let mut footer_buf = [0u8; 8];
        file.read_exact(&mut footer_buf)?;
        let meta_offset = u64::from_le_bytes(footer_buf);

        file.seek(SeekFrom::Start(meta_offset))?;
        let mut meta_data = Vec::new();
        let mut reader = BufReader::new(&file);
        reader.read_to_end(&mut meta_data)?;
        meta_data.truncate(meta_data.len() - 12);

        let (block_metas, bloom, min_key, max_key) = Self::decode_metadata(&meta_data)?;

        Ok(Self {
            file: File::open(path.as_ref())?,
            path: path.as_ref().to_path_buf(),
            block_metas,
            bloom,
            id,
            min_key,
            max_key,
        })
    }

    fn decode_metadata(data: &[u8]) -> Result<(Vec<BlockMeta>, BloomFilter, Bytes, Bytes)> {
        let mut buf = data;

        let num_blocks = buf.get_u32_le() as usize;
        let mut block_metas = Vec::with_capacity(num_blocks);

        for _ in 0..num_blocks {
            let offset = buf.get_u64_le();
            let length = buf.get_u32_le();
            let first_key_len = buf.get_u32_le() as usize;
            let first_key = Bytes::copy_from_slice(&buf[..first_key_len]);
            buf.advance(first_key_len);
            let last_key_len = buf.get_u32_le() as usize;
            let last_key = Bytes::copy_from_slice(&buf[..last_key_len]);
            buf.advance(last_key_len);

            block_metas.push(BlockMeta {
                offset,
                length,
                first_key,
                last_key,
            });
        }

        let bloom_num_hashes = buf.get_u32_le();
        let bloom_len = buf.get_u32_le() as usize;
        let bloom_bits = buf[..bloom_len].to_vec();
        buf.advance(bloom_len);

        let bloom = BloomFilter::from_bytes(bloom_bits, bloom_num_hashes);

        let min_key = block_metas.first()
            .map(|b| b.first_key.clone())
            .unwrap_or_default();
        let max_key = block_metas.last()
            .map(|b| b.last_key.clone())
            .unwrap_or_default();

        Ok((block_metas, bloom, min_key, max_key))
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        let block_idx = self.find_block(key);
        if block_idx >= self.block_metas.len() {
            return Ok(None);
        }

        let block = self.read_block(block_idx)?;
        Self::search_block(&block, key)
    }

    fn find_block(&self, key: &[u8]) -> usize {
        self.block_metas
            .binary_search_by(|meta| meta.first_key.as_ref().cmp(key))
            .unwrap_or_else(|idx| idx.saturating_sub(1))
    }

    fn read_block(&mut self, idx: usize) -> Result<Vec<u8>> {
        let meta = &self.block_metas[idx];
        self.file.seek(SeekFrom::Start(meta.offset))?;

        let mut block = vec![0u8; meta.length as usize];
        self.file.read_exact(&mut block)?;

        let checksum_start = block.len() - 4;
        let expected_checksum = u32::from_le_bytes(block[checksum_start..].try_into().unwrap());
        let actual_checksum = crc32fast::hash(&block[..checksum_start]);

        if expected_checksum != actual_checksum {
            return Err(Error::Corruption("Block checksum mismatch".into()));
        }

        block.truncate(checksum_start);
        Ok(block)
    }

    fn search_block(block: &[u8], search_key: &[u8]) -> Result<Option<Bytes>> {
        let mut buf = block;

        while buf.has_remaining() {
            let key_len = buf.get_u32_le() as usize;
            let key = &buf[..key_len];
            buf.advance(key_len);

            let value_len = buf.get_u32_le() as usize;
            let is_tombstone = value_len == u32::MAX as usize;

            if key == search_key {
                if is_tombstone {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(&buf[..value_len])));
            }

            if !is_tombstone {
                buf.advance(value_len);
            }
        }

        Ok(None)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct SSTableBuilder {
    writer: BufWriter<File>,
    path: PathBuf,
    block_metas: Vec<BlockMeta>,
    current_block: BytesMut,
    block_first_key: Option<Bytes>,
    block_offset: u64,
    bloom: BloomFilter,
    num_keys: usize,
}

impl SSTableBuilder {
    pub fn new(id: u64, dir: impl AsRef<Path>, estimated_keys: usize) -> Result<Self> {
        let path = dir.as_ref().join(format!("{:06}.sst", id));
        let file = File::create(&path)?;

        Ok(Self {
            writer: BufWriter::new(file),
            path,
            block_metas: Vec::new(),
            current_block: BytesMut::with_capacity(BLOCK_SIZE),
            block_first_key: None,
            block_offset: 0,
            bloom: BloomFilter::new(estimated_keys.max(1), 0.01),
            num_keys: 0,
        })
    }

    pub fn add(&mut self, key: &Key, value: Option<&Bytes>) -> Result<()> {
        let key_bytes = key.data();
        self.bloom.insert(key_bytes);

        if self.block_first_key.is_none() {
            self.block_first_key = Some(Bytes::copy_from_slice(key_bytes));
        }

        self.current_block.put_u32_le(key_bytes.len() as u32);
        self.current_block.put_slice(key_bytes);

        match value {
            Some(v) => {
                self.current_block.put_u32_le(v.len() as u32);
                self.current_block.put_slice(v);
            }
            None => {
                self.current_block.put_u32_le(u32::MAX);
            }
        }

        self.num_keys += 1;

        if self.current_block.len() >= BLOCK_SIZE {
            self.flush_block(Bytes::copy_from_slice(key_bytes))?;
        }

        Ok(())
    }

    fn flush_block(&mut self, last_key: Bytes) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let checksum = crc32fast::hash(&self.current_block);
        self.current_block.put_u32_le(checksum);

        let length = self.current_block.len() as u32;
        self.writer.write_all(&self.current_block)?;

        self.block_metas.push(BlockMeta {
            offset: self.block_offset,
            length,
            first_key: self.block_first_key.take().unwrap(),
            last_key,
        });

        self.block_offset += length as u64;
        self.current_block.clear();

        Ok(())
    }

    pub fn finish(mut self) -> Result<PathBuf> {
        if !self.current_block.is_empty() {
            let last_key = self.block_metas
                .last()
                .map(|m| m.last_key.clone())
                .unwrap_or_default();
            self.flush_block(last_key)?;
        }

        let meta_offset = self.block_offset;

        let mut meta_buf = BytesMut::new();
        meta_buf.put_u32_le(self.block_metas.len() as u32);

        for meta in &self.block_metas {
            meta_buf.put_u64_le(meta.offset);
            meta_buf.put_u32_le(meta.length);
            meta_buf.put_u32_le(meta.first_key.len() as u32);
            meta_buf.put_slice(&meta.first_key);
            meta_buf.put_u32_le(meta.last_key.len() as u32);
            meta_buf.put_slice(&meta.last_key);
        }

        meta_buf.put_u32_le(self.bloom.num_hashes());
        meta_buf.put_u32_le(self.bloom.bits().len() as u32);
        meta_buf.put_slice(self.bloom.bits());

        self.writer.write_all(&meta_buf)?;
        self.writer.write_all(&meta_offset.to_le_bytes())?;
        self.writer.write_all(&SSTABLE_MAGIC.to_le_bytes())?;
        self.writer.flush()?;

        Ok(self.path)
    }
}
