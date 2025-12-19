const SEED: u32 = 0xbc9f1d34;

pub struct BloomFilter {
    bits: Vec<u8>,
    num_hashes: u32,
}

impl BloomFilter {
    pub fn new(num_keys: usize, false_positive_rate: f64) -> Self {
        let bits_per_key = Self::optimal_bits_per_key(false_positive_rate);
        let num_bits = (num_keys * bits_per_key).max(64);
        let num_hashes = Self::optimal_num_hashes(bits_per_key);

        Self {
            bits: vec![0; num_bits.div_ceil(8)],
            num_hashes,
        }
    }

    pub fn from_bytes(data: Vec<u8>, num_hashes: u32) -> Self {
        Self {
            bits: data,
            num_hashes,
        }
    }

    fn optimal_bits_per_key(false_positive_rate: f64) -> usize {
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        (-false_positive_rate.ln() / ln2_squared).ceil() as usize
    }

    fn optimal_num_hashes(bits_per_key: usize) -> u32 {
        ((bits_per_key as f64) * std::f64::consts::LN_2).ceil() as u32
    }

    pub fn insert(&mut self, key: &[u8]) {
        let num_bits = self.bits.len() * 8;
        let mut h = Self::hash(key);

        let delta = h.rotate_left(15);
        for _ in 0..self.num_hashes {
            let bit_pos = (h as usize) % num_bits;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
            h = h.wrapping_add(delta);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let num_bits = self.bits.len() * 8;
        let mut h = Self::hash(key);

        let delta = h.rotate_left(15);
        for _ in 0..self.num_hashes {
            let bit_pos = (h as usize) % num_bits;
            if self.bits[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    fn hash(key: &[u8]) -> u32 {
        let mut h = SEED ^ (key.len() as u32);
        for chunk in key.chunks(4) {
            let mut k = [0u8; 4];
            k[..chunk.len()].copy_from_slice(chunk);
            let k = u32::from_le_bytes(k);
            h ^= k;
            h = h.wrapping_mul(0x5bd1e995);
            h ^= h >> 15;
        }
        h
    }

    pub fn bits(&self) -> &[u8] {
        &self.bits
    }

    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }
}
