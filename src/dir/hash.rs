//! Directory hash algorithms for HTree indexing
//!
//! Implements the hash functions used by ext4's directory indexing (HTree):
//! - Half MD4
//! - TEA (Tiny Encryption Algorithm)
//! - Legacy hash
//!
//! These algorithms correspond to lwext4's ext4_hash.c

use crate::error::{Error, ErrorKind, Result};

/// Hash version constants
///
/// 对应 lwext4 的 EXT2_HTREE_* 常量
pub const EXT2_HTREE_LEGACY: u8 = 0;
pub const EXT2_HTREE_HALF_MD4: u8 = 1;
pub const EXT2_HTREE_TEA: u8 = 2;
pub const EXT2_HTREE_LEGACY_UNSIGNED: u8 = 3;
pub const EXT2_HTREE_HALF_MD4_UNSIGNED: u8 = 4;
pub const EXT2_HTREE_TEA_UNSIGNED: u8 = 5;

/// End-of-file marker for HTree
pub const EXT2_HTREE_EOF: u32 = 0x7FFFFFFF;

/// Hash information structure
///
/// 对应 lwext4 的 `struct ext4_hash_info`
#[derive(Debug, Clone)]
pub struct HashInfo {
    pub hash: u32,
    pub minor_hash: u32,
    pub hash_version: u8,
    pub seed: Option<[u32; 4]>,
}

/// MD4 F function
#[inline(always)]
fn md4_f(x: u32, y: u32, z: u32) -> u32 {
    (x & y) | (!x & z)
}

/// MD4 G function
#[inline(always)]
fn md4_g(x: u32, y: u32, z: u32) -> u32 {
    (x & y) | (x & z) | (y & z)
}

/// MD4 H function
#[inline(always)]
fn md4_h(x: u32, y: u32, z: u32) -> u32 {
    x ^ y ^ z
}

/// Rotate left
#[inline(always)]
fn rotate_left(x: u32, n: u32) -> u32 {
    (x << n) | (x >> (32 - n))
}

/// MD4 Round 1 transformation
macro_rules! ff {
    ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr) => {
        $a = $a.wrapping_add(md4_f($b, $c, $d)).wrapping_add($x);
        $a = rotate_left($a, $s);
    };
}

/// MD4 Round 2 transformation
macro_rules! gg {
    ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr) => {
        $a = $a
            .wrapping_add(md4_g($b, $c, $d))
            .wrapping_add($x)
            .wrapping_add(0x5A827999);
        $a = rotate_left($a, $s);
    };
}

/// MD4 Round 3 transformation
macro_rules! hh {
    ($a:expr, $b:expr, $c:expr, $d:expr, $x:expr, $s:expr) => {
        $a = $a
            .wrapping_add(md4_h($b, $c, $d))
            .wrapping_add($x)
            .wrapping_add(0x6ED9EBA1);
        $a = rotate_left($a, $s);
    };
}

/// Half MD4 hash transformation
///
/// 对应 lwext4 的 `ext2_half_md4()`
///
/// This is a simplified MD4 algorithm used by Linux for directory indexing.
fn half_md4(hash: &mut [u32; 4], data: &[u32; 8]) {
    let mut a = hash[0];
    let mut b = hash[1];
    let mut c = hash[2];
    let mut d = hash[3];

    // Round 1
    ff!(a, b, c, d, data[0], 3);
    ff!(d, a, b, c, data[1], 7);
    ff!(c, d, a, b, data[2], 11);
    ff!(b, c, d, a, data[3], 19);
    ff!(a, b, c, d, data[4], 3);
    ff!(d, a, b, c, data[5], 7);
    ff!(c, d, a, b, data[6], 11);
    ff!(b, c, d, a, data[7], 19);

    // Round 2
    gg!(a, b, c, d, data[1], 3);
    gg!(d, a, b, c, data[3], 5);
    gg!(c, d, a, b, data[5], 9);
    gg!(b, c, d, a, data[7], 13);
    gg!(a, b, c, d, data[0], 3);
    gg!(d, a, b, c, data[2], 5);
    gg!(c, d, a, b, data[4], 9);
    gg!(b, c, d, a, data[6], 13);

    // Round 3
    hh!(a, b, c, d, data[3], 3);
    hh!(d, a, b, c, data[7], 9);
    hh!(c, d, a, b, data[2], 11);
    hh!(b, c, d, a, data[6], 15);
    hh!(a, b, c, d, data[1], 3);
    hh!(d, a, b, c, data[5], 9);
    hh!(c, d, a, b, data[0], 11);
    hh!(b, c, d, a, data[4], 15);

    hash[0] = hash[0].wrapping_add(a);
    hash[1] = hash[1].wrapping_add(b);
    hash[2] = hash[2].wrapping_add(c);
    hash[3] = hash[3].wrapping_add(d);
}

/// TEA (Tiny Encryption Algorithm) hash transformation
///
/// 对应 lwext4 的 `ext2_tea()`
fn tea(hash: &mut [u32; 4], data: &[u32; 8]) {
    const TEA_DELTA: u32 = 0x9E3779B9;
    let mut x = hash[0];
    let mut y = hash[1];
    let mut sum = TEA_DELTA;

    for _ in 0..16 {
        x = x.wrapping_add(
            ((y << 4).wrapping_add(data[0]))
                ^ (y.wrapping_add(sum))
                ^ ((y >> 5).wrapping_add(data[1])),
        );
        y = y.wrapping_add(
            ((x << 4).wrapping_add(data[2]))
                ^ (x.wrapping_add(sum))
                ^ ((x >> 5).wrapping_add(data[3])),
        );
        sum = sum.wrapping_add(TEA_DELTA);
    }

    hash[0] = hash[0].wrapping_add(x);
    hash[1] = hash[1].wrapping_add(y);
}

/// Legacy hash algorithm
///
/// 对应 lwext4 的 `ext2_legacy_hash()`
fn legacy_hash(name: &[u8], unsigned_char: bool) -> u32 {
    let mut h1 = 0x12A3FE2D_u32;
    let mut h2 = 0x37ABE8F9_u32;
    const MULTI: u32 = 0x6D22F5;

    for &byte in name {
        let val = if unsigned_char {
            byte as u32
        } else {
            (byte as i8) as i32 as u32
        };

        let h0 = h2.wrapping_add(h1 ^ val.wrapping_mul(MULTI));
        let h0 = if h0 & 0x80000000 != 0 {
            h0.wrapping_sub(0x7FFFFFFF)
        } else {
            h0
        };

        h2 = h1;
        h1 = h0;
    }

    h1 << 1
}

/// Prepare hash buffer from string
///
/// 对应 lwext4 的 `ext2_prep_hashbuf()`
///
/// Converts string into u32 array with padding
fn prep_hashbuf(src: &[u8], dst: &mut [u32], unsigned_char: bool) {
    let slen = src.len();
    let padding = (slen as u32) | ((slen as u32) << 8) | ((slen as u32) << 16) | ((slen as u32) << 24);

    let len = slen.min(dst.len() * 4);
    let mut buf_val = padding;

    for (i, &byte) in src.iter().enumerate().take(len) {
        let buf_byte = if unsigned_char {
            byte as u32
        } else {
            (byte as i8) as i32 as u32
        };

        if i % 4 == 0 {
            buf_val = padding;
        }

        buf_val = (buf_val << 8) | buf_byte;

        if i % 4 == 3 {
            dst[i / 4] = buf_val;
            buf_val = padding;
        }
    }

    // Write remaining partial word
    if len % 4 != 0 {
        dst[len / 4] = buf_val;
    }

    // Fill rest with padding
    for item in &mut dst[len.div_ceil(4)..] {
        *item = padding;
    }
}

/// Compute directory entry name hash
///
/// 对应 lwext4 的 `ext2_htree_hash()`
///
/// # Parameters
///
/// * `name` - Entry name
/// * `hash_seed` - Hash seed from superblock (optional)
/// * `hash_version` - Hash algorithm version from superblock
///
/// # Returns
///
/// `(major_hash, minor_hash)` tuple
///
/// # Errors
///
/// Returns error if name length is invalid (0 or > 255)
pub fn htree_hash(
    name: &[u8],
    hash_seed: Option<&[u32; 4]>,
    hash_version: u8,
) -> Result<(u32, u32)> {
    if name.is_empty() || name.len() > 255 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Name length must be 1-255",
        ));
    }

    // Initialize hash state
    let mut hash = [0x67452301_u32, 0xEFCDAB89, 0x98BADCFE, 0x10325476];

    // Apply seed if provided
    if let Some(seed) = hash_seed {
        hash.copy_from_slice(seed);
    }

    let unsigned_char = matches!(
        hash_version,
        EXT2_HTREE_TEA_UNSIGNED | EXT2_HTREE_LEGACY_UNSIGNED | EXT2_HTREE_HALF_MD4_UNSIGNED
    );

    let (major, minor) = match hash_version {
        EXT2_HTREE_TEA | EXT2_HTREE_TEA_UNSIGNED => {
            let mut pos = 0;
            while pos < name.len() {
                let mut data = [0u32; 8];
                let chunk_len = (name.len() - pos).min(16);
                prep_hashbuf(&name[pos..pos + chunk_len], &mut data[..4], unsigned_char);
                tea(&mut hash, &data);
                pos += 16;
            }
            (hash[0], hash[1])
        }

        EXT2_HTREE_LEGACY | EXT2_HTREE_LEGACY_UNSIGNED => {
            let major = legacy_hash(name, unsigned_char);
            (major, 0)
        }

        EXT2_HTREE_HALF_MD4 | EXT2_HTREE_HALF_MD4_UNSIGNED => {
            let mut pos = 0;
            while pos < name.len() {
                let mut data = [0u32; 8];
                let chunk_len = (name.len() - pos).min(32);
                prep_hashbuf(&name[pos..pos + chunk_len], &mut data, unsigned_char);
                half_md4(&mut hash, &data);
                pos += 32;
            }
            (hash[0], hash[1])
        }

        _ => {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Unknown hash version",
            ));
        }
    };

    Ok((major, minor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_basic() {
        // Test with simple name
        let name = b"test";
        let result = htree_hash(name, None, EXT2_HTREE_HALF_MD4);
        assert!(result.is_ok());
    }

    #[test]
    fn test_hash_invalid_length() {
        // Empty name
        assert!(htree_hash(b"", None, EXT2_HTREE_HALF_MD4).is_err());

        // Name too long
        let long_name = [b'a'; 256];
        assert!(htree_hash(&long_name, None, EXT2_HTREE_HALF_MD4).is_err());
    }

    #[test]
    fn test_hash_versions() {
        let name = b"example";

        // Test all hash versions
        for &version in &[
            EXT2_HTREE_LEGACY,
            EXT2_HTREE_HALF_MD4,
            EXT2_HTREE_TEA,
            EXT2_HTREE_LEGACY_UNSIGNED,
            EXT2_HTREE_HALF_MD4_UNSIGNED,
            EXT2_HTREE_TEA_UNSIGNED,
        ] {
            let result = htree_hash(name, None, version);
            assert!(result.is_ok(), "Hash version {} failed", version);
        }
    }
}
