//! Extent 树完整性验证
//!
//! 对应 lwext4 的 `ext4_ext_check()` 功能
//!
//! 这个模块提供 extent 树结构的完整性验证，包括：
//! - 魔数检查
//! - 深度验证
//! - 条目数量合理性检查
//! - CRC32C 校验和验证（如果启用）


use crate::{
    error::{Error, ErrorKind, Result},
    extent::checksum::{compute_checksum, extent_tail_offset, get_extent_tail},
    fs::InodeRef,
    superblock::Superblock,
    types::ext4_extent_header,
    BlockDevice,
};

/// Extent magic number
const EXT4_EXTENT_MAGIC: u16 = 0xF30A;

/// Extent 树完整性检查错误信息
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtentCheckError {
    /// 无效的魔数
    InvalidMagic,
    /// 深度不匹配
    UnexpectedDepth,
    /// 无效的 max_entries_count（为0）
    InvalidMaxEntries,
    /// entries_count 超过 max_entries_count
    InvalidEntriesCount,
    /// 校验和不匹配
    ChecksumMismatch,
}

impl ExtentCheckError {
    /// 获取错误描述
    pub fn message(&self) -> &'static str {
        match self {
            Self::InvalidMagic => "invalid magic",
            Self::UnexpectedDepth => "unexpected depth",
            Self::InvalidMaxEntries => "invalid max entries (zero)",
            Self::InvalidEntriesCount => "entries count exceeds max",
            Self::ChecksumMismatch => "checksum mismatch",
        }
    }
}

/// 验证 extent 块的结构完整性
///
/// 对应 lwext4 的 `ext4_ext_check()`
///
/// # 参数
///
/// * `sb` - superblock 引用
/// * `inode_num` - inode 编号（用于校验和验证）
/// * `inode_gen` - inode generation（用于校验和验证）
/// * `block_data` - extent 块数据
/// * `expected_depth` - 预期的树深度
/// * `pblock` - 物理块号（用于错误日志）
///
/// # 返回
///
/// 成功返回 `Ok(())`，失败返回包含详细错误信息的 `Err`
///
/// # 检查项目
///
/// 1. 魔数检查：header.magic == 0xF30A
/// 2. 深度检查：header.depth == expected_depth
/// 3. 最大条目数检查：header.max != 0
/// 4. 条目数检查：header.entries <= header.max
/// 5. 校验和检查：如果启用 METADATA_CSUM 特性
pub fn check_extent_block(
    sb: &Superblock,
    inode_num: u32,
    inode_gen: u32,
    block_data: &[u8],
    expected_depth: u16,
    _pblock: u64,
) -> Result<()> {
    // 解析 extent header
    let header_ptr = block_data.as_ptr() as *const ext4_extent_header;
    let header = unsafe { &*header_ptr };

    // 1. 检查魔数
    let magic = u16::from_le(header.magic);
    if magic != EXT4_EXTENT_MAGIC {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "bad extent block: invalid magic",
        ));
    }

    // 2. 检查深度
    let depth = u16::from_le(header.depth);
    if depth != expected_depth {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "bad extent block: unexpected depth",
        ));
    }

    // 3. 检查 max_entries_count
    let max_entries = u16::from_le(header.max);
    if max_entries == 0 {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "bad extent block: invalid max entries",
        ));
    }

    // 4. 检查 entries_count
    let entries = u16::from_le(header.entries);
    if entries > max_entries {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "bad extent block: entries count exceeds max",
        ));
    }

    // 5. 检查校验和（如果启用了 METADATA_CSUM）
    if sb.has_ro_compat_feature(crate::consts::EXT4_FEATURE_RO_COMPAT_METADATA_CSUM) {
        // 获取存储的校验和
        let tail_offset = extent_tail_offset(header);
        if tail_offset <= block_data.len() {
            let stored_checksum = unsafe {
                let tail = get_extent_tail(block_data);
                u32::from_le(tail.checksum)
            };

            // 计算校验和
            let computed_checksum = compute_checksum(sb, inode_num, inode_gen, block_data);

            if stored_checksum != computed_checksum {
                // 注意：lwext4 只是打印警告，不返回错误
                // 我们这里也采用相同策略，打印日志但不失败
                #[cfg(feature = "std")]
                eprintln!(
                    "Warning: Extent block checksum failed. Block: 0x{:x}",
                    pblock
                );

                // 如果需要严格校验，可以取消注释以下代码：
                // return Err(Error::new(
                //     ErrorKind::Corrupted,
                //     "extent block checksum mismatch",
                // ));
            }
        }
    }

    Ok(())
}

/// 验证 inode 中的 extent 树（depth=0）
///
/// 这是 `check_extent_block` 的便捷包装，用于检查存储在 inode 中的 extent 树
///
/// # 参数
///
/// * `inode_ref` - inode 引用
/// * `sb` - superblock 引用
///
/// # 返回
///
/// 成功返回 `Ok(())`
pub fn check_inode_extent<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &Superblock,
) -> Result<()> {
    let inode_num = inode_ref.inode_num();

    let (inode_gen, depth, block_data_vec) = inode_ref.with_inode(|inode| {
        let inode_gen = u32::from_le(inode.generation);

        // 将 inode.blocks 转换为字节切片并复制
        let block_data = inode.extent_root_data();

        // 读取 depth
        let header_ptr = block_data.as_ptr() as *const ext4_extent_header;
        let header = unsafe { &*header_ptr };
        let depth = u16::from_le(header.depth);

        (inode_gen, depth, block_data.to_vec())
    })?;

    // 在闭包外部执行验证
    check_extent_block(sb, inode_num, inode_gen, &block_data_vec, depth, 0)
}

/// 快速检查 extent header 的基本有效性
///
/// 这是一个轻量级的检查，只验证最基本的字段，
/// 不涉及校验和计算，适合频繁调用的场景
///
/// # 参数
///
/// * `header` - extent header 引用
///
/// # 返回
///
/// 如果基本检查通过返回 `Ok(())`
pub fn quick_check_header(header: &ext4_extent_header) -> Result<()> {
    // 1. 检查魔数
    let magic = u16::from_le(header.magic);
    if magic != EXT4_EXTENT_MAGIC {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "invalid extent magic number",
        ));
    }

    // 2. 检查 max_entries_count
    let max_entries = u16::from_le(header.max);
    if max_entries == 0 {
        return Err(Error::new(ErrorKind::Corrupted, "invalid max entries"));
    }

    // 3. 检查 entries_count
    let entries = u16::from_le(header.entries);
    if entries > max_entries {
        return Err(Error::new(ErrorKind::Corrupted, "invalid entries count"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ext4_sblock;
    use alloc::vec;

    #[test]
    fn test_quick_check_valid_header() {
        let header = ext4_extent_header {
            magic: EXT4_EXTENT_MAGIC.to_le(),
            entries: 2u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        assert!(quick_check_header(&header).is_ok());
    }

    #[test]
    fn test_quick_check_invalid_magic() {
        let header = ext4_extent_header {
            magic: 0xBEEF_u16.to_le(),
            entries: 2u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        assert!(quick_check_header(&header).is_err());
    }

    #[test]
    fn test_quick_check_zero_max() {
        let header = ext4_extent_header {
            magic: EXT4_EXTENT_MAGIC.to_le(),
            entries: 0u16.to_le(),
            max: 0u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        assert!(quick_check_header(&header).is_err());
    }

    #[test]
    fn test_quick_check_entries_exceed_max() {
        let header = ext4_extent_header {
            magic: EXT4_EXTENT_MAGIC.to_le(),
            entries: 5u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        assert!(quick_check_header(&header).is_err());
    }

    #[test]
    fn test_check_extent_block_valid() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.feature_ro_compat = 0u32.to_le(); // 不启用 METADATA_CSUM

        let superblock = Superblock::new(sb);

        let mut block_data = vec![0u8; 4096];

        // 初始化有效的 extent header
        let header = ext4_extent_header {
            magic: EXT4_EXTENT_MAGIC.to_le(),
            entries: 0u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        unsafe {
            let header_ptr = block_data.as_mut_ptr() as *mut ext4_extent_header;
            *header_ptr = header;
        }

        assert!(check_extent_block(&superblock, 1, 0, &block_data, 0, 1000).is_ok());
    }

    #[test]
    fn test_check_extent_block_invalid_magic() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.feature_ro_compat = 0u32.to_le();

        let superblock = Superblock::new(sb);

        let mut block_data = vec![0u8; 4096];

        // 初始化无效魔数的 header
        let header = ext4_extent_header {
            magic: 0xDEAD_u16.to_le(),
            entries: 0u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        unsafe {
            let header_ptr = block_data.as_mut_ptr() as *mut ext4_extent_header;
            *header_ptr = header;
        }

        assert!(check_extent_block(&superblock, 1, 0, &block_data, 0, 1000).is_err());
    }

    #[test]
    fn test_check_extent_block_depth_mismatch() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.feature_ro_compat = 0u32.to_le();

        let superblock = Superblock::new(sb);

        let mut block_data = vec![0u8; 4096];

        let header = ext4_extent_header {
            magic: EXT4_EXTENT_MAGIC.to_le(),
            entries: 0u16.to_le(),
            max: 4u16.to_le(),
            depth: 1u16.to_le(), // depth = 1
            generation: 0u32.to_le(),
        };

        unsafe {
            let header_ptr = block_data.as_mut_ptr() as *mut ext4_extent_header;
            *header_ptr = header;
        }

        // 期望 depth = 0，但实际是 1
        assert!(check_extent_block(&superblock, 1, 0, &block_data, 0, 1000).is_err());
    }
}
