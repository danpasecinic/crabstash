use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabstash_common::simd::{bytes_equal, compare_bytes};
use crabstash_common::{Error, Key, Result};
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::bloom::BloomFilter;
use crate::cache::{BlockCache, BlockCacheKey};
use crate::iterator::StorageIterator;

const SSTABLE_MAGIC: u32 = 0x53535442;
const BLOCK_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionType {
    #[default]
    None,
    Lz4,
}

impl CompressionType {
    fn to_u8(self) -> u8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Lz4 => 1,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => CompressionType::Lz4,
            _ => CompressionType::None,
        }
    }
}

#[derive(Debug)]
pub struct BlockMeta {
    pub offset: u64,
    pub length: u32,
    pub uncompressed_length: u32,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

pub struct SSTable {
    file: Mutex<File>,
    path: PathBuf,
    block_metas: Vec<BlockMeta>,
    bloom: BloomFilter,
    compression: CompressionType,
    cache: Option<Arc<BlockCache>>,
    pub id: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
}

impl SSTable {
    pub fn open(id: u64, path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cache(id, path, None)
    }

    pub fn open_with_cache(
        id: u64,
        path: impl AsRef<Path>,
        cache: Option<Arc<BlockCache>>,
    ) -> Result<Self> {
        let mut file = File::open(path.as_ref())?;

        file.seek(SeekFrom::End(-4))?;
        let mut magic_buf = [0u8; 4];
        file.read_exact(&mut magic_buf)?;
        let magic = u32::from_le_bytes(magic_buf);
        if magic != SSTABLE_MAGIC {
            return Err(Error::Corruption("Invalid SSTable magic".into()).into());
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

        let (block_metas, bloom, compression, min_key, max_key) =
            Self::decode_metadata(&meta_data)?;

        Ok(Self {
            file: Mutex::new(File::open(path.as_ref())?),
            path: path.as_ref().to_path_buf(),
            block_metas,
            bloom,
            compression,
            cache,
            id,
            min_key,
            max_key,
        })
    }

    fn decode_metadata(
        data: &[u8],
    ) -> Result<(Vec<BlockMeta>, BloomFilter, CompressionType, Bytes, Bytes)> {
        let mut buf = data;

        let compression = CompressionType::from_u8(buf.get_u8());
        let num_blocks = buf.get_u32_le() as usize;
        let mut block_metas = Vec::with_capacity(num_blocks);

        for _ in 0..num_blocks {
            let offset = buf.get_u64_le();
            let length = buf.get_u32_le();
            let uncompressed_length = buf.get_u32_le();
            let first_key_len = buf.get_u32_le() as usize;
            let first_key = Bytes::copy_from_slice(&buf[..first_key_len]);
            buf.advance(first_key_len);
            let last_key_len = buf.get_u32_le() as usize;
            let last_key = Bytes::copy_from_slice(&buf[..last_key_len]);
            buf.advance(last_key_len);

            block_metas.push(BlockMeta {
                offset,
                length,
                uncompressed_length,
                first_key,
                last_key,
            });
        }

        let bloom_num_hashes = buf.get_u32_le();
        let bloom_len = buf.get_u32_le() as usize;
        let bloom_bits = buf[..bloom_len].to_vec();
        buf.advance(bloom_len);

        let bloom = BloomFilter::from_bytes(bloom_bits, bloom_num_hashes);

        let min_key = block_metas
            .first()
            .map(|b| b.first_key.clone())
            .unwrap_or_default();
        let max_key = block_metas
            .last()
            .map(|b| b.last_key.clone())
            .unwrap_or_default();

        Ok((block_metas, bloom, compression, min_key, max_key))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        let block_idx = self.find_block(key);
        if block_idx >= self.block_metas.len() {
            return Ok(None);
        }

        let block = self.read_block(block_idx)?;
        Self::search_block(&block[..], key)
    }

    pub fn cache(&self) -> Option<&Arc<BlockCache>> {
        self.cache.as_ref()
    }

    fn find_block(&self, key: &[u8]) -> usize {
        self.block_metas
            .binary_search_by(|meta| compare_bytes(meta.first_key.as_ref(), key))
            .unwrap_or_else(|idx| idx.saturating_sub(1))
    }

    fn read_block(&self, idx: usize) -> Result<Bytes> {
        let cache_key = BlockCacheKey::new(self.id, idx);

        if let Some(ref cache) = self.cache
            && let Some(block) = cache.get(&cache_key)
        {
            return Ok((*block).clone());
        }

        let block = self.read_block_from_disk(idx)?;

        if let Some(ref cache) = self.cache {
            cache.insert(cache_key, block.clone());
        }

        Ok(block)
    }

    fn read_block_from_disk(&self, idx: usize) -> Result<Bytes> {
        let meta = &self.block_metas[idx];

        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(meta.offset))?;

        let mut block = vec![0u8; meta.length as usize];
        file.read_exact(&mut block)?;
        drop(file);

        let checksum_start = block.len() - 4;
        let expected_checksum = u32::from_le_bytes(block[checksum_start..].try_into()?);
        let actual_checksum = crc32fast::hash(&block[..checksum_start]);

        if expected_checksum != actual_checksum {
            return Err(Error::Corruption("Block checksum mismatch".into()).into());
        }

        block.truncate(checksum_start);

        let decompressed = self.decompress_block(&block, meta.uncompressed_length as usize)?;
        Ok(Bytes::from(decompressed))
    }

    fn decompress_block(&self, data: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
        match self.compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Lz4 => {
                let mut output = vec![0u8; uncompressed_len];
                lz4_flex::decompress_into(data, &mut output)
                    .map_err(|e| Error::Corruption(format!("LZ4 decompression failed: {}", e)))?;
                Ok(output)
            }
        }
    }

    fn search_block(block: &[u8], search_key: &[u8]) -> Result<Option<Bytes>> {
        let mut buf = block;

        while buf.has_remaining() {
            let key_len = buf.get_u32_le() as usize;
            let key = &buf[..key_len];
            buf.advance(key_len);

            let value_len = buf.get_u32_le() as usize;
            let is_tombstone = value_len == u32::MAX as usize;

            if bytes_equal(key, search_key) {
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

    pub fn num_blocks(&self) -> usize {
        self.block_metas.len()
    }

    pub fn block_meta(&self, idx: usize) -> &BlockMeta {
        &self.block_metas[idx]
    }

    pub fn read_block_cached(&self, idx: usize) -> Result<Bytes> {
        let cache_key = BlockCacheKey::new(self.id, idx);

        if let Some(ref cache) = self.cache
            && let Some(block) = cache.get(&cache_key)
        {
            return Ok((*block).clone());
        }

        let meta = &self.block_metas[idx];
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(meta.offset))?;

        let mut block = vec![0u8; meta.length as usize];
        file.read_exact(&mut block)?;

        let checksum_start = block.len() - 4;
        let expected = u32::from_le_bytes(block[checksum_start..].try_into()?);
        let actual = crc32fast::hash(&block[..checksum_start]);

        if expected != actual {
            return Err(Error::Corruption("Block checksum mismatch".into()).into());
        }

        block.truncate(checksum_start);

        let decompressed = self.decompress_block(&block, meta.uncompressed_length as usize)?;
        let block = Bytes::from(decompressed);

        if let Some(ref cache) = self.cache {
            cache.insert(cache_key, block.clone());
        }

        Ok(block)
    }
}

pub struct SSTableIterator {
    table: Arc<SSTable>,
    block_idx: usize,
    block_data: Vec<u8>,
    block_offset: usize,
    current_key: Key,
    current_value: Option<Bytes>,
    valid: bool,
}

impl SSTableIterator {
    pub fn new(table: Arc<SSTable>) -> Result<Self> {
        let mut iter = Self {
            table,
            block_idx: 0,
            block_data: Vec::new(),
            block_offset: 0,
            current_key: Key::new(Bytes::new(), 0),
            current_value: None,
            valid: false,
        };
        iter.load_block(0)?;
        iter.parse_entry()?;
        Ok(iter)
    }

    pub fn seek_to_first(&mut self) -> Result<()> {
        self.block_idx = 0;
        self.load_block(0)?;
        self.parse_entry()
    }

    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        let block_idx = self
            .table
            .block_metas
            .binary_search_by(|meta| meta.first_key.as_ref().cmp(target))
            .unwrap_or_else(|idx| idx.saturating_sub(1));

        self.block_idx = block_idx;
        self.load_block(block_idx)?;
        self.parse_entry()?;

        while self.valid && self.current_key.data() < target {
            self.next()?;
        }
        Ok(())
    }

    fn load_block(&mut self, idx: usize) -> Result<()> {
        if idx >= self.table.block_metas.len() {
            self.valid = false;
            return Ok(());
        }

        let block = self.table.read_block_cached(idx)?;
        self.block_data = block.to_vec();
        self.block_offset = 0;
        self.block_idx = idx;
        Ok(())
    }

    fn parse_entry(&mut self) -> Result<()> {
        if self.block_offset >= self.block_data.len() {
            if self.block_idx + 1 >= self.table.block_metas.len() {
                self.valid = false;
                return Ok(());
            }
            self.load_block(self.block_idx + 1)?;
        }

        if self.block_data.is_empty() {
            self.valid = false;
            return Ok(());
        }

        let mut buf = &self.block_data[self.block_offset..];
        if !buf.has_remaining() {
            self.valid = false;
            return Ok(());
        }

        let key_len = buf.get_u32_le() as usize;
        let key_data = Bytes::copy_from_slice(&buf[..key_len]);
        buf.advance(key_len);

        let value_len = buf.get_u32_le() as usize;
        let is_tombstone = value_len == u32::MAX as usize;

        let value = if is_tombstone {
            None
        } else {
            let v = Bytes::copy_from_slice(&buf[..value_len]);
            buf.advance(value_len);
            Some(v)
        };

        let consumed = self.block_data.len() - self.block_offset - buf.len();
        self.block_offset += consumed;

        self.current_key = Key::new(key_data, 0);
        self.current_value = value;
        self.valid = true;
        Ok(())
    }
}

impl StorageIterator for SSTableIterator {
    fn key(&self) -> &Key {
        &self.current_key
    }

    fn value(&self) -> Option<&Bytes> {
        self.current_value.as_ref()
    }

    fn is_valid(&self) -> bool {
        self.valid
    }

    fn next(&mut self) -> Result<()> {
        self.parse_entry()
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
    compression: CompressionType,
    num_keys: usize,
}

impl SSTableBuilder {
    pub fn new(id: u64, dir: impl AsRef<Path>, estimated_keys: usize) -> Result<Self> {
        Self::new_with_compression(id, dir, estimated_keys, CompressionType::None)
    }

    pub fn new_with_compression(
        id: u64,
        dir: impl AsRef<Path>,
        estimated_keys: usize,
        compression: CompressionType,
    ) -> Result<Self> {
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
            compression,
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

        let uncompressed_length = self.current_block.len() as u32;

        let compressed = self.compress_block(&self.current_block);
        let mut output = BytesMut::from(compressed.as_slice());

        let checksum = crc32fast::hash(&output);
        output.put_u32_le(checksum);

        let length = output.len() as u32;
        self.writer.write_all(&output)?;

        self.block_metas.push(BlockMeta {
            offset: self.block_offset,
            length,
            uncompressed_length,
            first_key: self.block_first_key.take().unwrap(),
            last_key,
        });

        self.block_offset += length as u64;
        self.current_block.clear();

        Ok(())
    }

    fn compress_block(&self, data: &[u8]) -> Vec<u8> {
        match self.compression {
            CompressionType::None => data.to_vec(),
            CompressionType::Lz4 => lz4_flex::compress(data),
        }
    }

    pub fn finish(mut self) -> Result<PathBuf> {
        if !self.current_block.is_empty() {
            let last_key = self
                .block_metas
                .last()
                .map(|m| m.last_key.clone())
                .unwrap_or_default();
            self.flush_block(last_key)?;
        }

        let meta_offset = self.block_offset;

        let mut meta_buf = BytesMut::new();
        meta_buf.put_u8(self.compression.to_u8());
        meta_buf.put_u32_le(self.block_metas.len() as u32);

        for meta in &self.block_metas {
            meta_buf.put_u64_le(meta.offset);
            meta_buf.put_u32_le(meta.length);
            meta_buf.put_u32_le(meta.uncompressed_length);
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
