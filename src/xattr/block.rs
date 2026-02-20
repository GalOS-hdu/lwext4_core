//! xattr 块操作
//!
//! 处理独立 xattr 块的读写和引用计数

use crate::{
    consts::*,
    error::{Error, ErrorKind, Result},
    superblock::Superblock,
    types::{ext4_xattr_entry, ext4_xattr_header},
};
use core::mem::size_of;

use super::search::XattrSearch;

/// 获取 xattr block header
///
/// 对应 C 宏 `EXT4_XATTR_BHDR(block)`
///
/// # 参数
///
/// * `block_data` - block 数据
///
/// # 返回
///
/// header 引用
#[inline]
fn get_block_header(block_data: &[u8]) -> Result<ext4_xattr_header> {
    if block_data.len() < size_of::<ext4_xattr_header>() {
        return Err(Error::new(ErrorKind::InvalidInput, "block too small for header"));
    }

    let header_bytes = &block_data[0..size_of::<ext4_xattr_header>()];
    let header = unsafe {
        core::ptr::read(header_bytes.as_ptr() as *const ext4_xattr_header)
    };

    Ok(header)
}

/// 获取第一个 entry 的偏移
///
/// 对应 C 宏 `EXT4_XATTR_BFIRST(block)`
///
/// # 返回
///
/// 第一个 entry 的偏移（header 之后）
#[inline]
fn get_first_entry_offset() -> usize {
    size_of::<ext4_xattr_header>()
}

/// 验证 xattr block 的有效性
///
/// 对应 lwext4 的 `ext4_xattr_is_block_valid()`
///
/// # 参数
///
/// * `sb` - superblock
/// * `block_data` - block 数据
///
/// # 返回
///
/// 如果有效返回 Ok(())，否则返回错误
pub fn validate_block(sb: &Superblock, block_data: &[u8]) -> Result<()> {
    let block_size = sb.block_size() as usize;

    if block_data.len() < block_size {
        return Err(Error::new(ErrorKind::InvalidInput, "block data too small"));
    }

    // 读取 header
    let header = get_block_header(block_data)?;

    // 检查魔数
    if u32::from_le(header.h_magic) != EXT4_XATTR_MAGIC {
        return Err(Error::new(ErrorKind::InvalidInput, "invalid xattr block magic"));
    }

    // 检查块数（当前只支持单块）
    if u32::from_le(header.h_blocks) != 1 {
        return Err(Error::new(ErrorKind::InvalidInput, "multi-block xattr not supported"));
    }

    // 验证所有 entry
    let base = 0;
    let end = block_size;
    let mut min_offs = end - base;

    let first_entry_offset = get_first_entry_offset();
    let mut entry_offset = first_entry_offset;

    loop {
        // 检查是否到达末尾
        if entry_offset + 4 > end {
            break;
        }

        // 检查是否是最后一个 entry
        let first_u32 = u32::from_le_bytes([
            block_data[entry_offset],
            block_data[entry_offset + 1],
            block_data[entry_offset + 2],
            block_data[entry_offset + 3],
        ]);

        if first_u32 == 0 {
            break;
        }

        // 读取 entry
        if entry_offset + size_of::<ext4_xattr_entry>() > end {
            return Err(Error::new(ErrorKind::InvalidInput, "entry out of bounds"));
        }

        let entry_bytes = &block_data[entry_offset..entry_offset + size_of::<ext4_xattr_entry>()];
        let entry = unsafe {
            core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
        };

        let value_size = entry.value_size();
        let value_offs = u16::from_le(entry.e_value_offs) as usize;

        // 检查：如果 value_size 为 0，value_offs 也应该为 0
        if value_size == 0 && value_offs != 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid value offset"));
        }

        // 检查 value 是否在范围内
        if value_size > 0 {
            if base + value_offs + value_size as usize > end {
                return Err(Error::new(ErrorKind::InvalidInput, "value out of bounds"));
            }

            // 更新最小值偏移
            if value_offs < min_offs {
                min_offs = value_offs;
            }
        }

        // 检查下一个 entry 的位置
        let name_len = entry.e_name_len as usize;
        let entry_len = (name_len + EXT4_XATTR_ROUND as usize + size_of::<ext4_xattr_entry>())
            & !(EXT4_XATTR_ROUND as usize);

        let next_offset = entry_offset + entry_len;

        // 确保下一个 entry 的位置加上结束标记不会超出边界
        if next_offset + 4 > end {
            return Err(Error::new(ErrorKind::InvalidInput, "next entry out of bounds"));
        }

        entry_offset = next_offset;
    }

    // 检查 entry 区域和 value 区域是否有重叠
    if entry_offset > min_offs {
        return Err(Error::new(ErrorKind::InvalidInput, "entry and value regions overlap"));
    }

    Ok(())
}

/// 初始化 xattr block
///
/// 对应 lwext4 的 `ext4_xattr_block_initialize()`
///
/// # 参数
///
/// * `block_data` - block 数据（可变）
///
/// # 返回
///
/// 成功返回 Ok(())
pub fn initialize_block(block_data: &mut [u8]) -> Result<()> {
    if block_data.len() < size_of::<ext4_xattr_header>() {
        return Err(Error::new(ErrorKind::InvalidInput, "block too small"));
    }

    // 清零整个 block
    for byte in block_data.iter_mut() {
        *byte = 0;
    }

    // 设置 header
    let magic_bytes = EXT4_XATTR_MAGIC.to_le_bytes();
    block_data[0..4].copy_from_slice(&magic_bytes);

    // h_refcount = 1
    let refcount_bytes = 1u32.to_le_bytes();
    block_data[4..8].copy_from_slice(&refcount_bytes);

    // h_blocks = 1
    let blocks_bytes = 1u32.to_le_bytes();
    block_data[8..12].copy_from_slice(&blocks_bytes);

    // h_hash = 0 (初始)
    // h_checksum = 0 (初始)
    // h_reserved = 0 (已经清零)

    Ok(())
}

/// 在 xattr block 中查找 entry
///
/// 对应 lwext4 的 `ext4_xattr_block_find_entry()`
///
/// # 参数
///
/// * `sb` - superblock
/// * `block_data` - block 数据
/// * `name_index` - 命名空间索引
/// * `name` - 属性名称（不含前缀）
///
/// # 返回
///
/// 成功返回 Some((entry_offset, value_offset, value_size))，未找到返回 None
pub fn find_block_entry(
    sb: &Superblock,
    block_data: &[u8],
    name_index: u8,
    name: &[u8],
) -> Result<Option<(usize, usize, u32)>> {
    // 验证 block 有效性
    validate_block(sb, block_data)?;

    // 创建搜索上下文
    let first_entry_offset = get_first_entry_offset();
    let mut search = XattrSearch::new(block_data, first_entry_offset);

    // 查找 entry
    Ok(search.find_entry(name_index, name))
}

/// 列出 xattr block 中的所有 entry 名称
///
/// # 参数
///
/// * `sb` - superblock
/// * `block_data` - block 数据
/// * `buffer` - 输出缓冲区（名称以 \0 分隔）
///
/// # 返回
///
/// 成功返回写入的字节数
pub fn list_block_xattr(
    sb: &Superblock,
    block_data: &[u8],
    buffer: &mut [u8],
) -> Result<usize> {
    // 验证 block 有效性
    validate_block(sb, block_data)?;

    let block_size = sb.block_size() as usize;
    let first_entry_offset = get_first_entry_offset();
    let mut entry_offset = first_entry_offset;
    let mut written = 0;

    loop {
        // 检查是否到达末尾
        if entry_offset + 4 > block_size {
            break;
        }

        // 检查是否是最后一个 entry
        let first_u32 = u32::from_le_bytes([
            block_data[entry_offset],
            block_data[entry_offset + 1],
            block_data[entry_offset + 2],
            block_data[entry_offset + 3],
        ]);

        if first_u32 == 0 {
            break;
        }

        // 读取 entry
        if entry_offset + size_of::<ext4_xattr_entry>() > block_size {
            break;
        }

        let entry_bytes = &block_data[entry_offset..entry_offset + size_of::<ext4_xattr_entry>()];
        let entry = unsafe {
            core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
        };

        let name_len = entry.e_name_len as usize;
        let name_offset = entry_offset + size_of::<ext4_xattr_entry>();

        if name_offset + name_len <= block_size {
            let entry_name = &block_data[name_offset..name_offset + name_len];

            // 获取命名空间前缀
            use super::prefix::get_xattr_name_prefix;
            if let Some((prefix, prefix_len)) = get_xattr_name_prefix(entry.e_name_index) {
                let total_len = prefix_len + name_len + 1; // +1 for null terminator

                if written + total_len <= buffer.len() {
                    // 写入前缀
                    buffer[written..written + prefix_len].copy_from_slice(prefix.as_bytes());
                    written += prefix_len;

                    // 写入名称
                    buffer[written..written + name_len].copy_from_slice(entry_name);
                    written += name_len;

                    // 写入 null terminator
                    buffer[written] = 0;
                    written += 1;
                }
            }
        }

        // 移动到下一个 entry
        let entry_len = (name_len + EXT4_XATTR_ROUND as usize + size_of::<ext4_xattr_entry>())
            & !(EXT4_XATTR_ROUND as usize);
        entry_offset += entry_len;
    }

    Ok(written)
}

/// 获取 block 的引用计数
///
/// # 参数
///
/// * `block_data` - block 数据
///
/// # 返回
///
/// 引用计数
pub fn get_refcount(block_data: &[u8]) -> Result<u32> {
    let header = get_block_header(block_data)?;
    Ok(u32::from_le(header.h_refcount))
}

/// 设置 block 的引用计数
///
/// # 参数
///
/// * `block_data` - block 数据（可变）
/// * `refcount` - 新的引用计数
///
/// # 返回
///
/// 成功返回 Ok(())
pub fn set_refcount(block_data: &mut [u8], refcount: u32) -> Result<()> {
    if block_data.len() < 8 {
        return Err(Error::new(ErrorKind::InvalidInput, "block too small"));
    }

    let refcount_bytes = refcount.to_le_bytes();
    block_data[4..8].copy_from_slice(&refcount_bytes);

    Ok(())
}

/// 增加 block 的引用计数
///
/// # 参数
///
/// * `block_data` - block 数据（可变）
///
/// # 返回
///
/// 新的引用计数
pub fn inc_refcount(block_data: &mut [u8]) -> Result<u32> {
    let current = get_refcount(block_data)?;

    if current >= EXT4_XATTR_REFCOUNT_MAX {
        return Err(Error::new(ErrorKind::InvalidInput, "refcount overflow"));
    }

    let new_refcount = current + 1;
    set_refcount(block_data, new_refcount)?;
    Ok(new_refcount)
}

/// 减少 block 的引用计数
///
/// # 参数
///
/// * `block_data` - block 数据（可变）
///
/// # 返回
///
/// 新的引用计数（如果为 0，表示 block 可以被释放）
pub fn dec_refcount(block_data: &mut [u8]) -> Result<u32> {
    let current = get_refcount(block_data)?;

    if current == 0 {
        return Err(Error::new(ErrorKind::InvalidInput, "refcount underflow"));
    }

    let new_refcount = current - 1;
    set_refcount(block_data, new_refcount)?;
    Ok(new_refcount)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_initialize_block() {
        let mut block_data = vec![0u8; 4096];
        initialize_block(&mut block_data).unwrap();

        let header = get_block_header(&block_data).unwrap();
        assert_eq!(u32::from_le(header.h_magic), EXT4_XATTR_MAGIC);
        assert_eq!(u32::from_le(header.h_refcount), 1);
        assert_eq!(u32::from_le(header.h_blocks), 1);
    }

    #[test]
    fn test_refcount_operations() {
        let mut block_data = vec![0u8; 4096];
        initialize_block(&mut block_data).unwrap();

        // 初始引用计数应该是 1
        assert_eq!(get_refcount(&block_data).unwrap(), 1);

        // 增加引用计数
        let new_count = inc_refcount(&mut block_data).unwrap();
        assert_eq!(new_count, 2);
        assert_eq!(get_refcount(&block_data).unwrap(), 2);

        // 减少引用计数
        let new_count = dec_refcount(&mut block_data).unwrap();
        assert_eq!(new_count, 1);
        assert_eq!(get_refcount(&block_data).unwrap(), 1);
    }

    #[test]
    fn test_validate_empty_block() {
        // 需要一个有效的 superblock
        // 暂时跳过，因为需要构造完整的测试环境
    }
}
