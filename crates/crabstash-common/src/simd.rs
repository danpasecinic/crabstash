use std::cmp::Ordering;
use wide::u8x16;

const CHUNK_SIZE: usize = 16;
const ALL_EQUAL_MASK: u32 = 0xFFFF;

#[inline]
pub fn compare_bytes(a: &[u8], b: &[u8]) -> Ordering {
    let min_len = a.len().min(b.len());

    let mut offset = 0;
    while offset + CHUNK_SIZE <= min_len {
        let chunk_a = u8x16::new(load_chunk(&a[offset..]));
        let chunk_b = u8x16::new(load_chunk(&b[offset..]));

        let eq_mask = chunk_a.simd_eq(chunk_b);
        let bitmask = eq_mask.to_bitmask();

        if bitmask != ALL_EQUAL_MASK {
            let first_diff = (!bitmask).trailing_zeros() as usize;
            let idx = offset + first_diff;
            return a[idx].cmp(&b[idx]);
        }

        offset += CHUNK_SIZE;
    }

    for i in offset..min_len {
        match a[i].cmp(&b[i]) {
            Ordering::Equal => continue,
            ord => return ord,
        }
    }

    a.len().cmp(&b.len())
}

#[inline]
pub fn bytes_equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let len = a.len();
    let mut offset = 0;

    while offset + CHUNK_SIZE <= len {
        let chunk_a = u8x16::new(load_chunk(&a[offset..]));
        let chunk_b = u8x16::new(load_chunk(&b[offset..]));

        let eq_mask = chunk_a.simd_eq(chunk_b);
        if eq_mask.to_bitmask() != ALL_EQUAL_MASK {
            return false;
        }

        offset += CHUNK_SIZE;
    }

    a[offset..] == b[offset..]
}

#[inline(always)]
fn load_chunk(slice: &[u8]) -> [u8; CHUNK_SIZE] {
    let mut arr = [0u8; CHUNK_SIZE];
    arr.copy_from_slice(&slice[..CHUNK_SIZE]);
    arr
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_equal() {
        let a = b"hello world";
        let b = b"hello world";
        assert_eq!(compare_bytes(a, b), Ordering::Equal);
    }

    #[test]
    fn test_compare_less() {
        let a = b"hello";
        let b = b"world";
        assert_eq!(compare_bytes(a, b), Ordering::Less);
    }

    #[test]
    fn test_compare_greater() {
        let a = b"world";
        let b = b"hello";
        assert_eq!(compare_bytes(a, b), Ordering::Greater);
    }

    #[test]
    fn test_compare_prefix() {
        let a = b"hello";
        let b = b"hello world";
        assert_eq!(compare_bytes(a, b), Ordering::Less);
    }

    #[test]
    fn test_compare_long_strings() {
        let a = b"this is a very long string that exceeds sixteen bytes";
        let b = b"this is a very long string that exceeds sixteen bytes";
        assert_eq!(compare_bytes(a, b), Ordering::Equal);
    }

    #[test]
    fn test_compare_long_strings_diff() {
        let a = b"this is a very long string that exceeds sixteen bytes AAA";
        let b = b"this is a very long string that exceeds sixteen bytes BBB";
        assert_eq!(compare_bytes(a, b), Ordering::Less);
    }

    #[test]
    fn test_bytes_equal() {
        assert!(bytes_equal(b"hello", b"hello"));
        assert!(!bytes_equal(b"hello", b"world"));
        assert!(!bytes_equal(b"hello", b"hello world"));
    }

    #[test]
    fn test_bytes_equal_long() {
        let a = b"this is a very long string for testing simd equality";
        let b = b"this is a very long string for testing simd equality";
        assert!(bytes_equal(a, b));
    }
}
