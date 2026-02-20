//! Bitmap 操作实现
//!
//! 对应 lwext4 的 `ext4_balloc.c` 和 `ext4_ialloc.c` 中的位图操作

use crate::error::{Error, ErrorKind, Result};

/// 测试位图中某一位是否被设置
///
/// 对应 lwext4 的 `ext4_bmap_bit_get()`
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `index` - 位索引（从 0 开始）
///
/// # 返回
///
/// 如果位被设置返回 true，否则返回 false
pub fn test_bit(bitmap: &[u8], index: u32) -> bool {
    let byte_index = (index / 8) as usize;
    let bit_offset = (index % 8) as u8;

    if byte_index >= bitmap.len() {
        return false;
    }

    (bitmap[byte_index] & (1 << bit_offset)) != 0
}

/// 设置位图中的某一位
///
/// 对应 lwext4 的 `ext4_bmap_bit_set()`
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `index` - 位索引（从 0 开始）
///
/// # 返回
///
/// 成功返回 ()，如果索引超出范围返回错误
pub fn set_bit(bitmap: &mut [u8], index: u32) -> Result<()> {
    let byte_index = (index / 8) as usize;
    let bit_offset = (index % 8) as u8;

    if byte_index >= bitmap.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Bitmap index out of range",
        ));
    }

    bitmap[byte_index] |= 1 << bit_offset;
    Ok(())
}

/// 清除位图中的某一位
///
/// 对应 lwext4 的 `ext4_bmap_bit_clr()`
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `index` - 位索引（从 0 开始）
///
/// # 返回
///
/// 成功返回 ()，如果索引超出范围返回错误
pub fn clear_bit(bitmap: &mut [u8], index: u32) -> Result<()> {
    let byte_index = (index / 8) as usize;
    let bit_offset = (index % 8) as u8;

    if byte_index >= bitmap.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Bitmap index out of range",
        ));
    }

    bitmap[byte_index] &= !(1 << bit_offset);
    Ok(())
}

/// 在位图中查找第一个空闲位（值为 0 的位）
///
/// 对应 lwext4 的 `ext4_bmap_bit_find_clr()`
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `start` - 开始搜索的位置（从 0 开始）
/// * `end` - 结束位置（不包含）
///
/// # 返回
///
/// 成功返回第一个空闲位的索引，如果没有找到返回 None
pub fn find_first_zero(bitmap: &[u8], start: u32, end: u32) -> Option<u32> {
    let max_bits = (bitmap.len() * 8) as u32;
    let end = end.min(max_bits);

    (start..end).find(|&i| !test_bit(bitmap, i))
}

/// 在位图中查找第一个被设置的位（值为 1 的位）
///
/// 对应 lwext4 的 `ext4_bmap_bit_find_set()`
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `start` - 开始搜索的位置（从 0 开始）
/// * `end` - 结束位置（不包含）
///
/// # 返回
///
/// 成功返回第一个被设置位的索引，如果没有找到返回 None
pub fn find_first_one(bitmap: &[u8], start: u32, end: u32) -> Option<u32> {
    let max_bits = (bitmap.len() * 8) as u32;
    let end = end.min(max_bits);

    (start..end).find(|&i| test_bit(bitmap, i))
}

/// 统计位图中从 start 到 end 范围内被设置的位数
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `start` - 开始位置（从 0 开始）
/// * `end` - 结束位置（不包含）
///
/// # 返回
///
/// 被设置的位数
pub fn count_ones(bitmap: &[u8], start: u32, end: u32) -> u32 {
    let max_bits = (bitmap.len() * 8) as u32;
    let end = end.min(max_bits);
    let mut count = 0;

    for i in start..end {
        if test_bit(bitmap, i) {
            count += 1;
        }
    }

    count
}

/// 统计位图中从 start 到 end 范围内空闲的位数
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `start` - 开始位置（从 0 开始）
/// * `end` - 结束位置（不包含）
///
/// # 返回
///
/// 空闲的位数
pub fn count_zeros(bitmap: &[u8], start: u32, end: u32) -> u32 {
    let max_bits = (bitmap.len() * 8) as u32;
    let end = end.min(max_bits);
    (end - start) - count_ones(bitmap, start, end)
}

/// 批量设置位图中的连续位
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `start` - 开始位置
/// * `count` - 要设置的位数
///
/// # 返回
///
/// 成功返回 ()，如果超出范围返回错误
pub fn set_bits(bitmap: &mut [u8], start: u32, count: u32) -> Result<()> {
    for i in 0..count {
        set_bit(bitmap, start + i)?;
    }
    Ok(())
}

/// 批量清除位图中的连续位
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `start` - 开始位置
/// * `count` - 要清除的位数
///
/// # 返回
///
/// 成功返回 ()，如果超出范围返回错误
pub fn clear_bits(bitmap: &mut [u8], start: u32, count: u32) -> Result<()> {
    for i in 0..count {
        clear_bit(bitmap, start + i)?;
    }
    Ok(())
}

/// 查找位图中连续的 N 个空闲位
///
/// # 参数
///
/// * `bitmap` - 位图数据
/// * `start` - 开始搜索的位置
/// * `end` - 结束位置（不包含）
/// * `count` - 需要的连续空闲位数
///
/// # 返回
///
/// 成功返回第一个连续空闲段的起始索引，如果没有找到返回 None
pub fn find_consecutive_zeros(bitmap: &[u8], start: u32, end: u32, count: u32) -> Option<u32> {
    if count == 0 {
        return Some(start);
    }

    let max_bits = (bitmap.len() * 8) as u32;
    let end = end.min(max_bits);

    if start + count > end {
        return None;
    }

    let mut consecutive = 0;
    let mut candidate_start = start;

    for i in start..end {
        if !test_bit(bitmap, i) {
            if consecutive == 0 {
                candidate_start = i;
            }
            consecutive += 1;

            if consecutive == count {
                return Some(candidate_start);
            }
        } else {
            consecutive = 0;
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_operations() {
        let mut bitmap = [0u8; 4]; // 32 bits

        // 测试 set_bit
        assert!(!test_bit(&bitmap, 0));
        set_bit(&mut bitmap, 0).unwrap();
        assert!(test_bit(&bitmap, 0));

        set_bit(&mut bitmap, 7).unwrap();
        assert!(test_bit(&bitmap, 7));

        set_bit(&mut bitmap, 15).unwrap();
        assert!(test_bit(&bitmap, 15));

        // 测试 clear_bit
        clear_bit(&mut bitmap, 0).unwrap();
        assert!(!test_bit(&bitmap, 0));
        assert!(test_bit(&bitmap, 7));
    }

    #[test]
    fn test_find_first_zero() {
        let mut bitmap = [0xFFu8; 4]; // 全部设置为 1

        // 清除第 10 位
        clear_bit(&mut bitmap, 10).unwrap();

        let result = find_first_zero(&bitmap, 0, 32);
        assert_eq!(result, Some(10));

        // 清除第 5 位
        clear_bit(&mut bitmap, 5).unwrap();
        let result = find_first_zero(&bitmap, 0, 32);
        assert_eq!(result, Some(5)); // 应该找到更早的那个
    }

    #[test]
    fn test_find_first_one() {
        let mut bitmap = [0u8; 4]; // 全部清零

        // 设置第 10 位
        set_bit(&mut bitmap, 10).unwrap();

        let result = find_first_one(&bitmap, 0, 32);
        assert_eq!(result, Some(10));

        // 设置第 5 位
        set_bit(&mut bitmap, 5).unwrap();
        let result = find_first_one(&bitmap, 0, 32);
        assert_eq!(result, Some(5));
    }

    #[test]
    fn test_count_ones_zeros() {
        let mut bitmap = [0u8; 4]; // 32 bits

        assert_eq!(count_zeros(&bitmap, 0, 32), 32);
        assert_eq!(count_ones(&bitmap, 0, 32), 0);

        // 设置几个位
        set_bit(&mut bitmap, 0).unwrap();
        set_bit(&mut bitmap, 5).unwrap();
        set_bit(&mut bitmap, 10).unwrap();

        assert_eq!(count_ones(&bitmap, 0, 32), 3);
        assert_eq!(count_zeros(&bitmap, 0, 32), 29);
    }

    #[test]
    fn test_set_clear_bits() {
        let mut bitmap = [0u8; 4];

        // 批量设置
        set_bits(&mut bitmap, 5, 5).unwrap();
        for i in 5..10 {
            assert!(test_bit(&bitmap, i));
        }
        assert!(!test_bit(&bitmap, 4));
        assert!(!test_bit(&bitmap, 10));

        // 批量清除
        clear_bits(&mut bitmap, 7, 3).unwrap();
        assert!(test_bit(&bitmap, 5));
        assert!(test_bit(&bitmap, 6));
        assert!(!test_bit(&bitmap, 7));
        assert!(!test_bit(&bitmap, 8));
        assert!(!test_bit(&bitmap, 9));
    }

    #[test]
    fn test_find_consecutive_zeros() {
        let mut bitmap = [0u8; 4]; // 32 bits, 全为 0

        // 整个位图都是 0，应该能找到任意长度的连续 0
        assert_eq!(find_consecutive_zeros(&bitmap, 0, 32, 5), Some(0));
        assert_eq!(find_consecutive_zeros(&bitmap, 0, 32, 10), Some(0));

        // 设置一些位，创建间隙
        set_bits(&mut bitmap, 5, 3).unwrap(); // 位 5,6,7 被设置

        // 在位 5 之前应该能找到 5 个连续的 0（0-4）
        assert_eq!(find_consecutive_zeros(&bitmap, 0, 32, 5), Some(0));

        // 位 8 之后应该有足够的空间
        assert_eq!(find_consecutive_zeros(&bitmap, 8, 32, 10), Some(8));

        // 设置更多位
        set_bits(&mut bitmap, 0, 5).unwrap(); // 位 0-4 被设置

        // 现在 0-7 都被设置了，应该从 8 开始找
        assert_eq!(find_consecutive_zeros(&bitmap, 0, 32, 5), Some(8));
    }

    #[test]
    fn test_out_of_range() {
        let mut bitmap = [0u8; 4]; // 32 bits

        // 超出范围的索引
        assert!(set_bit(&mut bitmap, 32).is_err());
        assert!(clear_bit(&mut bitmap, 32).is_err());

        // find_* 函数应该正确处理超出范围的情况
        assert_eq!(find_first_zero(&bitmap, 0, 100), Some(0)); // end 会被限制到 32
        assert_eq!(find_first_zero(&bitmap, 32, 100), None); // start 已经超出
    }
}
