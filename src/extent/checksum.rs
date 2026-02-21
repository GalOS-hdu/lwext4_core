//! Extent 块校验和计算
//!
//! 对应 lwext4 的 `ext4_ext_block_csum()` 和 `ext4_extent_block_csum_set()` 功能
//!
//! ## 校验和算法
//!
//! Extent 块的 CRC32C 校验和计算包括：
//! 1. 文件系统 UUID
//! 2. Inode 编号
//! 3. Inode generation
//! 4. Extent 块内容（header + entries，不包括 tail 的 checksum 字段）

use crate::{
    consts::EXT4_FEATURE_RO_COMPAT_METADATA_CSUM,
    error::Result,
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_extent_header, ext4_extent_tail},
    BlockDevice,
    crc::EXT4_CRC32_INIT,
};

/// 计算 extent tail 的偏移量
///
/// Tail 位于所有 extent/index 条目之后
///
/// # 参数
///
/// * `header` - extent header 引用
///
/// # 返回
///
/// Tail 在块中的字节偏移量
#[inline]
pub fn extent_tail_offset(header: &ext4_extent_header) -> usize {
    core::mem::size_of::<ext4_extent_header>()
        + (core::mem::size_of::<crate::types::ext4_extent>()
            * u16::from_le(header.max) as usize)
}

/// 获取 extent tail 的可变引用
///
/// # 参数
///
/// * `block_data` - extent 块数据（完整块）
///
/// # 返回
///
/// Extent tail 的可变引用
///
/// # Safety
///
/// 调用者必须确保 block_data 包含有效的 extent 块
#[inline]
pub unsafe fn get_extent_tail_mut(block_data: &mut [u8]) -> &mut ext4_extent_tail {
    let header_ptr = block_data.as_ptr() as *const ext4_extent_header;
    let header = unsafe { &*header_ptr };
    let offset = extent_tail_offset(header);

    let tail_ptr = unsafe { block_data.as_mut_ptr().add(offset) as *mut ext4_extent_tail };
    unsafe { &mut *tail_ptr }
}

/// 获取 extent tail 的引用
///
/// # 参数
///
/// * `block_data` - extent 块数据（完整块）
///
/// # 返回
///
/// Extent tail 的引用
///
/// # Safety
///
/// 调用者必须确保 block_data 包含有效的 extent 块
#[inline]
pub unsafe fn get_extent_tail(block_data: &[u8]) -> &ext4_extent_tail {
    let header_ptr = block_data.as_ptr() as *const ext4_extent_header;
    let header = unsafe { &*header_ptr };
    let offset = extent_tail_offset(header);

    let tail_ptr = unsafe { block_data.as_ptr().add(offset) as *const ext4_extent_tail };
    unsafe { &*tail_ptr }
}

/// 计算 extent 块的 CRC32C 校验和
///
/// 对应 lwext4 的 `ext4_ext_block_csum()`
///
/// # 参数
///
/// * `sb` - superblock 引用
/// * `inode_num` - inode 编号
/// * `inode_gen` - inode generation
/// * `block_data` - extent 块数据
///
/// # 返回
///
/// 32 位 CRC32C 校验和
pub fn compute_checksum(
    sb: &Superblock,
    inode_num: u32,
    inode_gen: u32,
    block_data: &[u8],
) -> u32 {
    // 检查是否启用了 METADATA_CSUM 特性
    if !sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM) {
        return 0;
    }

    // 1. 计算 fs uuid 的 CRC
    let mut crc = crate::crc::crc32c_append(EXT4_CRC32_INIT, &sb.inner().uuid);

    // 2. 计算 inode number 的 CRC
    let inode_num_bytes = inode_num.to_le_bytes();
    crc = crate::crc::crc32c_append(crc, &inode_num_bytes);

    // 3. 计算 inode generation 的 CRC
    let inode_gen_bytes = inode_gen.to_le_bytes();
    crc = crate::crc::crc32c_append(crc, &inode_gen_bytes);

    // 4. 计算 extent 块的 CRC（到 tail 之前）
    let header_ptr = block_data.as_ptr() as *const ext4_extent_header;
    let header = unsafe { &*header_ptr };
    let tail_offset = extent_tail_offset(header);

    if tail_offset <= block_data.len() {
        crc = crate::crc::crc32c_append(crc, &block_data[..tail_offset]);
    }

    crc
}

/// 设置 extent 块的校验和
///
/// 对应 lwext4 的 `ext4_extent_block_csum_set()`
///
/// # 参数
///
/// * `sb` - superblock 引用
/// * `inode_num` - inode 编号
/// * `inode_gen` - inode generation
/// * `block_data` - 可变 extent 块数据
pub fn set_checksum(
    sb: &Superblock,
    inode_num: u32,
    inode_gen: u32,
    block_data: &mut [u8],
) {
    let checksum = compute_checksum(sb, inode_num, inode_gen, block_data);

    unsafe {
        let tail = get_extent_tail_mut(block_data);
        tail.checksum = checksum.to_le();
    }
}

/// 验证 extent 块的校验和
///
/// # 参数
///
/// * `sb` - superblock 引用
/// * `inode_num` - inode 编号
/// * `inode_gen` - inode generation
/// * `block_data` - extent 块数据
///
/// # 返回
///
/// 如果校验和正确或不需要校验返回 `true`
pub fn verify_checksum(
    sb: &Superblock,
    inode_num: u32,
    inode_gen: u32,
    block_data: &[u8],
) -> bool {
    // 检查是否启用了 METADATA_CSUM 特性
    if !sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM) {
        return true;
    }

    let stored = unsafe {
        let tail = get_extent_tail(block_data);
        u32::from_le(tail.checksum)
    };

    let computed = compute_checksum(sb, inode_num, inode_gen, block_data);

    stored == computed
}

/// 为 inode 中的 extent 树设置校验和
///
/// 用于 depth=0 的情况，extent 树存储在 inode 的 blocks 字段中
///
/// # 参数
///
/// * `inode_ref` - inode 引用
/// * `sb` - superblock 引用
pub fn set_inode_extent_checksum<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &Superblock,
) -> Result<()> {
    // 检查是否启用了 METADATA_CSUM 特性
    if !sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM) {
        return Ok(());
    }

    let inode_num = inode_ref.inode_num();

    inode_ref.with_inode_mut(|inode| {
        let inode_gen = u32::from_le(inode.generation);

        // 将 inode.blocks ([u32; 15]) 转换为字节切片 [u8; 60]
        let block_data = inode.extent_root_data_mut();

        set_checksum(sb, inode_num, inode_gen, block_data);
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ext4_extent, ext4_sblock};
    use alloc::vec;

    #[test]
    fn test_tail_offset_calculation() {
        let mut header = ext4_extent_header {
            magic: 0xF30A_u16.to_le(),
            entries: 0u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        let offset = extent_tail_offset(&header);

        // header: 12 bytes
        // 4 extents: 4 * 12 = 48 bytes
        // total: 60 bytes
        assert_eq!(offset, 12 + 48);

        header.max = 2u16.to_le();
        let offset2 = extent_tail_offset(&header);
        assert_eq!(offset2, 12 + 24);
    }

    #[test]
    fn test_checksum_without_feature() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.feature_ro_compat = 0u32.to_le();

        let superblock = Superblock::new(sb);

        let mut block_data = vec![0u8; 4096];

        // 初始化 extent header
        let header = ext4_extent_header {
            magic: 0xF30A_u16.to_le(),
            entries: 0u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        unsafe {
            let header_ptr = block_data.as_mut_ptr() as *mut ext4_extent_header;
            *header_ptr = header;
        }

        // 未启用特性，校验和应该为 0
        let csum = compute_checksum(&superblock, 1, 0, &block_data);
        assert_eq!(csum, 0);

        // 验证应该总是通过
        assert!(verify_checksum(&superblock, 1, 0, &block_data));
    }

    #[test]
    fn test_checksum_with_feature() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.feature_ro_compat = EXT4_FEATURE_RO_COMPAT_METADATA_CSUM.to_le();

        // 设置 UUID
        sb.uuid = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        let superblock = Superblock::new(sb);

        let mut block_data = vec![0u8; 4096];

        // 初始化 extent header
        let header = ext4_extent_header {
            magic: 0xF30A_u16.to_le(),
            entries: 0u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        unsafe {
            let header_ptr = block_data.as_mut_ptr() as *mut ext4_extent_header;
            *header_ptr = header;
        }

        // 设置校验和
        set_checksum(&superblock, 1, 0, &mut block_data);

        // 验证应该通过
        assert!(verify_checksum(&superblock, 1, 0, &block_data));
    }

    #[test]
    fn test_checksum_corruption() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.feature_ro_compat = EXT4_FEATURE_RO_COMPAT_METADATA_CSUM.to_le();
        sb.uuid = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        let superblock = Superblock::new(sb);

        let mut block_data = vec![0u8; 4096];

        // 初始化 extent header
        let header = ext4_extent_header {
            magic: 0xF30A_u16.to_le(),
            entries: 1u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        unsafe {
            let header_ptr = block_data.as_mut_ptr() as *mut ext4_extent_header;
            *header_ptr = header;

            // 添加一个 extent
            let extent_ptr = block_data.as_mut_ptr().add(12) as *mut ext4_extent;
            *extent_ptr = ext4_extent {
                block: 0u32.to_le(),
                len: 10u16.to_le(),
                start_hi: 0u16.to_le(),
                start_lo: 1000u32.to_le(),
            };
        }

        // 设置校验和
        set_checksum(&superblock, 1, 0, &mut block_data);

        // 验证应该通过
        assert!(verify_checksum(&superblock, 1, 0, &block_data));

        // 修改 extent 数据（模拟损坏）
        block_data[12] = 0xFF;

        // 现在验证应该失败
        assert!(!verify_checksum(&superblock, 1, 0, &block_data));
    }
}
