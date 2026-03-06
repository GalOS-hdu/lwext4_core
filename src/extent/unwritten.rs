//! Unwritten Extent 支持
//!
//! Unwritten extent（未初始化的 extent）用于：
//! 1. 预分配空间（fallocate）
//! 2. 延迟分配
//! 3. 避免不必要的零填充
//!
//! ## Unwritten Extent 标志
//!
//! Extent 的长度字段（`len`）使用最高位（MSB，位15）来标记是否为 unwritten：
//! - `len & 0x8000 == 0` → 已初始化（written）
//! - `len & 0x8000 != 0` → 未初始化（unwritten）
//!
//! ## 长度限制
//!
//! - 已初始化 extent 最大长度：32768 (0x8000)
//! - 未初始化 extent 最大长度：32767 (0x7FFF)
//!
//! ## 主要操作
//!
//! - `mark_initialized()` - 将 unwritten 转为 written
//! - `mark_unwritten()` - 将 written 转为 unwritten
//! - `split_extent_at()` - 在指定位置分裂 extent
//! - `convert_to_initialized()` - 转换部分 unwritten extent 为 initialized
//! - `zero_unwritten_range()` - 零填充未写入区域

use crate::{
    block::BlockDevice,
    error::{Error, ErrorKind, Result},
    extent::insert::insert_extent_simple,
    fs::InodeRef,
    superblock::Superblock,
    types::ext4_extent,
};

/// 已初始化 extent 的最大长度（2^15 = 32768）
pub const EXT_INIT_MAX_LEN: u16 = 1 << 15;

/// 未初始化 extent 的最大长度（32767）
pub const EXT_UNWRITTEN_MAX_LEN: u16 = EXT_INIT_MAX_LEN - 1;

/// Extent 分裂标志：标记第一部分为 unwritten
pub(crate) const EXT4_EXT_MARK_UNWRIT1: u32 = 0x02;

/// Extent 分裂标志：标记第二部分为 unwritten
pub(crate) const EXT4_EXT_MARK_UNWRIT2: u32 = 0x04;

/// 标记 extent 为已初始化（written）
///
/// 清除长度字段的 MSB（位15）
///
/// # 参数
///
/// * `extent` - 要修改的 extent
pub fn mark_initialized(extent: &mut ext4_extent) {
    let actual_len = get_actual_len(extent);
    extent.len = actual_len.to_le();
}

/// 标记 extent 为未初始化（unwritten）
///
/// 设置长度字段的 MSB（位15）
///
/// # 参数
///
/// * `extent` - 要修改的 extent
pub fn mark_unwritten(extent: &mut ext4_extent) {
    let len = u16::from_le(extent.len);
    extent.len = (len | EXT_INIT_MAX_LEN).to_le();
}

/// 检查 extent 是否为 unwritten
///
/// # 参数
///
/// * `extent` - 要检查的 extent
///
/// # 返回
///
/// 如果是 unwritten 返回 true
pub fn is_unwritten(extent: &ext4_extent) -> bool {
    // 注意：len = 0x8000 被视为已初始化 extent
    u16::from_le(extent.len) > EXT_INIT_MAX_LEN
}

/// 获取 extent 的实际长度（去除 unwritten 标志位）
///
/// # 参数
///
/// * `extent` - extent 引用
///
/// # 返回
///
/// 实际块数量
pub fn get_actual_len(extent: &ext4_extent) -> u16 {
    let len = u16::from_le(extent.len);
    if len <= EXT_INIT_MAX_LEN {
        len
    } else {
        len - EXT_INIT_MAX_LEN
    }
}

/// 设置 extent 的物理块号
///
/// 将 48 位物理块号分解为 start_lo (32位) 和 start_hi (16位)
///
/// # 参数
///
/// * `extent` - extent 引用
/// * `pblock` - 物理块号
pub fn store_pblock(extent: &mut ext4_extent, pblock: u64) {
    extent.start_lo = (pblock as u32).to_le();
    extent.start_hi = ((pblock >> 32) as u16).to_le();
}

/// 获取 extent 的物理块号
///
/// 合并 start_lo 和 start_hi 为 48 位物理块号
///
/// # 参数
///
/// * `extent` - extent 引用
///
/// # 返回
///
/// 物理块号
pub fn get_pblock(extent: &ext4_extent) -> u64 {
    (u32::from_le(extent.start_lo) as u64) | ((u16::from_le(extent.start_hi) as u64) << 32)
}

/// 在指定逻辑块位置分裂 extent
///
/// 对应 lwext4 的 `ext4_ext_split_extent_at()`
///
/// 这个函数将一个 extent 在指定位置分裂成两个 extent，
/// 并根据 split_flag 设置每部分的 unwritten 状态
///
/// # 参数
///
/// * `inode_ref` - inode 引用
/// * `sb` - superblock
/// * `extent_idx` - 要分裂的 extent 在数组中的索引
/// * `split` - 分裂点的逻辑块号
/// * `split_flag` - 分裂标志（EXT4_EXT_MARK_UNWRIT1/2）
///
/// # 返回
///
/// 成功返回 Ok(())
///
/// # 限制
///
/// 此函数仅支持深度 0 的 extent 树。
/// 对于多层树，请使用 `unwritten_multilevel::split_extent_at_multilevel`。
pub fn split_extent_at<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    _sb: &mut Superblock,
    extent_idx: usize,
    split: u32,
    split_flag: u32,
) -> Result<()> {
    // 读取原始 extent（包括 unwritten 状态，用于回滚）
    let (ee_block, ee_len, ee_start, original_was_unwritten) = inode_ref.with_inode(|inode| {
        let header = inode.extent_header();
        let entries = u16::from_le(header.entries) as usize;

        if extent_idx >= entries {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "extent index out of bounds",
            ));
        }

        let extent_ptr = unsafe {
            (inode.extent_header() as *const crate::types::ext4_extent_header).add(1) as *const ext4_extent
        };
        let extent = unsafe { &*extent_ptr.add(extent_idx) };

        let ee_block = u32::from_le(extent.block);
        let ee_len = get_actual_len(extent);
        let ee_start = get_pblock(extent);
        let was_unwritten = is_unwritten(extent);

        Ok((ee_block, ee_len, ee_start, was_unwritten))
    })??;

    // 计算新块的物理起始位置
    let newblock = ee_start + (split - ee_block) as u64;

    // Case 1: 分裂点正好是 extent 起始位置
    if split == ee_block {
        // 只需要改变状态，不需要分裂
        inode_ref.with_inode_mut(|inode| {
            let header_ptr = inode.extent_header_mut() as *mut crate::types::ext4_extent_header;
            let extent_ptr = unsafe {
                (header_ptr.add(1) as *mut ext4_extent).add(extent_idx)
            };
            let extent = unsafe { &mut *extent_ptr };

            if split_flag & EXT4_EXT_MARK_UNWRIT2 != 0 {
                mark_unwritten(extent);
            } else {
                mark_initialized(extent);
            }
        })?;

        inode_ref.mark_dirty()?;
        return Ok(());
    }

    // Case 2: 需要真正分裂 extent

    // 第一步：修改原 extent 的长度
    inode_ref.with_inode_mut(|inode| {
        let header_ptr = inode.extent_header_mut() as *mut crate::types::ext4_extent_header;
        let extent_ptr = unsafe {
            (header_ptr.add(1) as *mut ext4_extent).add(extent_idx)
        };
        let extent = unsafe { &mut *extent_ptr };

        // 缩短原 extent
        extent.len = ((split - ee_block) as u16).to_le();

        // 如果需要标记第一部分为 unwritten
        if split_flag & EXT4_EXT_MARK_UNWRIT1 != 0 {
            mark_unwritten(extent);
        }
    })?;

    // 第二步：创建新 extent（分裂后的第二部分）
    let mut new_extent = ext4_extent {
        block: split.to_le(),
        len: (ee_len - (split - ee_block) as u16).to_le(),
        start_lo: 0,
        start_hi: 0,
    };

    store_pblock(&mut new_extent, newblock);

    // 如果需要标记第二部分为 unwritten
    if split_flag & EXT4_EXT_MARK_UNWRIT2 != 0 {
        mark_unwritten(&mut new_extent);
    }

    // 第三步：插入新 extent
    // 注意：如果插入失败，需要完整恢复原 extent 状态
    if let Err(e) = insert_extent_simple(inode_ref, &new_extent) {
        // ✅ 完整回滚：恢复长度 + unwritten 状态
        inode_ref.with_inode_mut(|inode| {
            let header_ptr = inode.extent_header_mut() as *mut crate::types::ext4_extent_header;
            let extent_ptr = unsafe {
                (header_ptr.add(1) as *mut ext4_extent).add(extent_idx)
            };
            let extent = unsafe { &mut *extent_ptr };

            // 恢复原始长度
            extent.len = ee_len.to_le();

            // 恢复原始 unwritten 状态
            if original_was_unwritten {
                mark_unwritten(extent);
            } else {
                mark_initialized(extent);
            }
        })?;

        return Err(e);
    }

    inode_ref.mark_dirty()?;
    Ok(())
}

/// 将 unwritten extent 的指定范围转换为 initialized
///
/// 对应 lwext4 的 `ext4_ext_convert_to_initialized()`
///
/// 根据转换范围的位置，可能需要将一个 extent 分裂成 1-3 个部分：
/// - 如果转换范围在开头：分裂成 [initialized][unwritten]
/// - 如果转换范围在结尾：分裂成 [unwritten][initialized]
/// - 如果转换范围在中间：分裂成 [unwritten][initialized][unwritten]
///
/// # 参数
///
/// * `inode_ref` - inode 引用
/// * `sb` - superblock
/// * `extent_idx` - extent 在数组中的索引
/// * `split` - 转换范围的起始逻辑块号
/// * `blocks` - 转换的块数量
///
/// # 返回
///
/// 成功返回 Ok(())
pub fn convert_to_initialized<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    extent_idx: usize,
    split: u32,
    blocks: u32,
) -> Result<()> {
    // 读取 extent 信息
    let (ee_block, ee_len) = inode_ref.with_inode(|inode| {
        let header = inode.extent_header();
        let entries = u16::from_le(header.entries) as usize;

        if extent_idx >= entries {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "extent index out of bounds",
            ));
        }

        let extent_ptr = unsafe {
            (inode.extent_header() as *const crate::types::ext4_extent_header).add(1) as *const ext4_extent
        };
        let extent = unsafe { &*extent_ptr.add(extent_idx) };

        let ee_block = u32::from_le(extent.block);
        let ee_len = get_actual_len(extent);

        Ok((ee_block, ee_len as u32))
    })??;

    // 确保 split 在 extent 范围内
    if split < ee_block {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "split point before extent start",
        ));
    }

    // Case 1: 转换范围在结尾 [unwritten][initialized]
    if split + blocks == ee_block + ee_len {
        return split_extent_at(inode_ref, sb, extent_idx, split, EXT4_EXT_MARK_UNWRIT1);
    }

    // Case 2: 转换范围在开头 [initialized][unwritten]
    if ee_block == split {
        return split_extent_at(
            inode_ref,
            sb,
            extent_idx,
            split + blocks,
            EXT4_EXT_MARK_UNWRIT2,
        );
    }

    // Case 3: 转换范围在中间 [unwritten][initialized][unwritten]
    // 需要两次分裂

    // 第一次分裂：split + blocks 处，将后半部分标spport记为 unwritten
    split_extent_at(
        inode_ref,
        sb,
        extent_idx,
        split + blocks,
        EXT4_EXT_MARK_UNWRIT1 | EXT4_EXT_MARK_UNWRIT2,
    )?;

    // 第二次分裂：split 处，将前半部分标记为 unwritten
    // 注意：第一次分裂后，extent_idx 位置的 extent 仍然是我们要分裂的那个
    split_extent_at(inode_ref, sb, extent_idx, split, EXT4_EXT_MARK_UNWRIT1)?;

    Ok(())
}

/// 零填充 unwritten extent 的指定范围
///
/// 对应 lwext4 的 `ext4_ext_zero_unwritten_range()`
///
/// 这个函数将 unwritten extent 的指定物理块范围填充为零，
/// 为后续的写入操作做准备
///
/// # 参数
///
/// * `bdev` - 块设备
/// * `pblock` - 起始物理块号
/// * `blocks_count` - 块数量
///
/// # 返回
///
/// 成功返回 Ok(())
///
/// # 注意
///
/// 这个操作比较耗时，应该只在必要时使用
pub fn zero_unwritten_range<D: BlockDevice>(
    _bdev: &mut D,
    _pblock: u64,
    _blocks_count: u32,
) -> Result<()> {
    // TODO: 实现块零填充
    // 当前简化实现，实际使用时需要：
    // 1. 读取每个块
    // 2. 填充为零
    // 3. 写回块设备

    // 暂时返回 Unsupported
    Err(Error::new(
        ErrorKind::Unsupported,
        "zero_unwritten_range not yet implemented",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_functions() {
        let mut extent = ext4_extent {
            block: 0u32.to_le(),
            len: 100u16.to_le(),
            start_lo: 1000u32.to_le(),
            start_hi: 0u16.to_le(),
        };

        // 初始应该是 initialized
        assert!(!is_unwritten(&extent));
        assert_eq!(get_actual_len(&extent), 100);

        // 标记为 unwritten
        mark_unwritten(&mut extent);
        assert!(is_unwritten(&extent));
        assert_eq!(get_actual_len(&extent), 100); // 实际长度不变

        // 标记回 initialized
        mark_initialized(&mut extent);
        assert!(!is_unwritten(&extent));
        assert_eq!(get_actual_len(&extent), 100);
    }

    #[test]
    fn test_pblock_functions() {
        let mut extent = ext4_extent {
            block: 0u32.to_le(),
            len: 10u16.to_le(),
            start_lo: 0,
            start_hi: 0,
        };

        // 测试 32 位物理块号
        store_pblock(&mut extent, 12345);
        assert_eq!(get_pblock(&extent), 12345);

        // 测试 48 位物理块号
        let large_pblock = (1u64 << 40) + 67890;
        store_pblock(&mut extent, large_pblock);
        assert_eq!(get_pblock(&extent), large_pblock);
    }

    #[test]
    fn test_actual_len() {
        let mut extent = ext4_extent {
            block: 0u32.to_le(),
            len: 100u16.to_le(),
            start_lo: 0,
            start_hi: 0,
        };

        // initialized extent
        assert_eq!(get_actual_len(&extent), 100);

        // unwritten extent
        mark_unwritten(&mut extent);
        assert_eq!(get_actual_len(&extent), 100); // 实际长度应该相同

        // 边界情况：最大长度
        extent.len = EXT_INIT_MAX_LEN.to_le();
        assert_eq!(get_actual_len(&extent), EXT_INIT_MAX_LEN);
        assert!(!is_unwritten(&extent)); // 0x8000 被视为 initialized

        extent.len = (EXT_INIT_MAX_LEN + 1).to_le();
        assert_eq!(get_actual_len(&extent), 1);
        assert!(is_unwritten(&extent));
    }

    #[test]
    fn test_max_lengths() {
        assert_eq!(EXT_INIT_MAX_LEN, 32768);
        assert_eq!(EXT_UNWRITTEN_MAX_LEN, 32767);
    }
}
