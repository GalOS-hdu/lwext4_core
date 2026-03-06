//! Extent 自动合并优化
//!
//! 实现 extent 的自动合并功能，减少文件碎片，提升性能
//!
//! ## 主要功能
//!
//! - 检测相邻 extent 是否可以合并
//! - 在插入 extent 时自动尝试合并
//! - 支持向前合并、向后合并和双向合并
//!
//! ## 合并条件
//!
//! 两个 extent 可以合并当且仅当：
//! 1. 逻辑块连续（extent1.end + 1 == extent2.start）
//! 2. 物理块连续（pblock1.end + 1 == pblock2.start）
//! 3. Unwritten 状态相同
//! 4. 合并后长度不超过最大值（32768块）
//!
//! ## 对应 lwext4
//!
//! - `ext4_ext_can_prepend()` - 检查是否可以向前合并
//! - `ext4_ext_can_append()` - 检查是否可以向后合并

use crate::{
    block::BlockDevice,
    error::Result,
    fs::InodeRef,
    types::ext4_extent,
};

use super::{
    helpers::ext4_ext_store_pblock,
    unwritten::is_unwritten,
    path::ExtentNodeType,
};

use alloc::vec::Vec;

/// Extent 合并方向
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeDirection {
    /// 不能合并
    None,
    /// 向前合并（与前一个 extent 合并）
    Prepend,
    /// 向后合并（与后一个 extent 合并）
    Append,
    /// 双向合并（与前后两个 extent 合并）
    Both,
}

/// Extent 最大长度（不包括 unwritten 标志位）
const EXT4_EXT_MAX_LEN: u32 = 0x7FFF;

/// 检查是否可以向前合并
///
/// 检查新的 extent 是否可以与前一个 extent 合并
///
/// # 参数
///
/// * `prev` - 前一个 extent
/// * `new_lblock` - 新 extent 的逻辑块起始
/// * `new_pblock` - 新 extent 的物理块起始
/// * `new_len` - 新 extent 的长度
/// * `new_is_unwritten` - 新 extent 是否为 unwritten
///
/// # 返回
///
/// 如果可以合并返回 true
///
/// # 对应 lwext4
///
/// `ext4_ext_can_prepend()`
pub fn can_prepend(
    prev: &ext4_extent,
    new_lblock: u32,
    new_pblock: u64,
    new_len: u32,
    new_is_unwritten: bool,
) -> bool {
    let prev_lblock = prev.logical_block();
    let prev_len = prev.actual_len() as u32;
    let prev_pblock = prev.physical_block();
    let prev_unwritten = is_unwritten(prev);

    // 检查逻辑块是否连续
    if new_lblock != prev_lblock + prev_len {
        return false;
    }

    // 检查物理块是否连续
    if new_pblock != prev_pblock + prev_len as u64 {
        return false;
    }

    // 检查合并后长度是否超过最大值
    if prev_len + new_len > EXT4_EXT_MAX_LEN {
        return false;
    }

    // unwritten extent 和 initialized extent 不能合并
    // 只有相同状态的 extent 可以合并
    if prev_unwritten != new_is_unwritten {
        return false;
    }

    true
}

/// 检查是否可以向后合并
///
/// 检查新的 extent 是否可以与后一个 extent 合并
///
/// # 参数
///
/// * `next` - 后一个 extent
/// * `new_lblock` - 新 extent 的逻辑块起始
/// * `new_pblock` - 新 extent 的物理块起始
/// * `new_len` - 新 extent 的长度
/// * `new_is_unwritten` - 新 extent 是否为 unwritten
///
/// # 返回
///
/// 如果可以合并返回 true
///
/// # 对应 lwext4
///
/// `ext4_ext_can_append()`
pub fn can_append(
    next: &ext4_extent,
    new_lblock: u32,
    new_pblock: u64,
    new_len: u32,
    new_is_unwritten: bool,
) -> bool {
    let next_lblock = next.logical_block();
    let next_pblock = next.physical_block();
    let next_unwritten = is_unwritten(next);

    // 检查逻辑块是否连续
    if new_lblock + new_len != next_lblock {
        return false;
    }

    // 检查物理块是否连续
    if new_pblock + new_len as u64 != next_pblock {
        return false;
    }

    // 检查合并后长度是否超过最大值
    let next_len = next.actual_len() as u32;
    if new_len + next_len > EXT4_EXT_MAX_LEN {
        return false;
    }

    // unwritten extent 和 initialized extent 不能合并
    // 只有相同状态的 extent 可以合并
    if next_unwritten != new_is_unwritten {
        return false;
    }

    true
}

/// 检查新 extent 可以与哪些相邻 extent 合并
///
/// # 参数
///
/// * `extents` - extent 数组
/// * `insert_pos` - 新 extent 应该插入的位置
/// * `new_lblock` - 新 extent 的逻辑块起始
/// * `new_pblock` - 新 extent 的物理块起始
/// * `new_len` - 新 extent 的长度
/// * `new_is_unwritten` - 新 extent 是否为 unwritten
///
/// # 返回
///
/// 合并方向
pub fn check_merge_direction(
    extents: &[ext4_extent],
    insert_pos: usize,
    new_lblock: u32,
    new_pblock: u64,
    new_len: u32,
    new_is_unwritten: bool,
) -> MergeDirection {
    let can_prepend = if insert_pos > 0 {
        can_prepend(&extents[insert_pos - 1], new_lblock, new_pblock, new_len, new_is_unwritten)
    } else {
        false
    };

    let can_append = if insert_pos < extents.len() {
        can_append(&extents[insert_pos], new_lblock, new_pblock, new_len, new_is_unwritten)
    } else {
        false
    };

    match (can_prepend, can_append) {
        (true, true) => MergeDirection::Both,
        (true, false) => MergeDirection::Prepend,
        (false, true) => MergeDirection::Append,
        (false, false) => MergeDirection::None,
    }
}

/// 执行 extent 合并
///
/// # 参数
///
/// * `extents` - extent 数组（会被修改）
/// * `insert_pos` - 新 extent 应该插入的位置
/// * `new_lblock` - 新 extent 的逻辑块起始
/// * `new_pblock` - 新 extent 的物理块起始
/// * `new_len` - 新 extent 的长度
/// * `new_is_unwritten` - 新 extent 是否为 unwritten
/// * `direction` - 合并方向
///
/// # 返回
///
/// 新的 extent 数组
pub fn merge_extents(
    extents: &mut Vec<ext4_extent>,
    insert_pos: usize,
    new_lblock: u32,
    new_pblock: u64,
    new_len: u32,
    new_is_unwritten: bool,
    direction: MergeDirection,
) -> Result<()> {
    use super::unwritten::{mark_unwritten, mark_initialized};

    match direction {
        MergeDirection::None => {
            // 不合并，直接插入
            let mut new_extent = ext4_extent {
                block: new_lblock.to_le(),
                len: (new_len as u16).to_le(),
                ..ext4_extent::default()
            };
            ext4_ext_store_pblock(&mut new_extent, new_pblock);

            // 设置 unwritten 标志
            if new_is_unwritten {
                mark_unwritten(&mut new_extent);
            } else {
                mark_initialized(&mut new_extent);
            }

            extents.insert(insert_pos, new_extent);
        }

        MergeDirection::Prepend => {
            // 向前合并：扩展前一个 extent
            let prev_idx = insert_pos - 1;
            let prev = &mut extents[prev_idx];
            let new_total_len = prev.actual_len() as u32 + new_len;
            prev.len = (new_total_len as u16).to_le();
        }

        MergeDirection::Append => {
            // 向后合并：扩展后一个 extent 并调整起始位置
            let next = &mut extents[insert_pos];
            let old_next_len = next.actual_len() as u32;
            let new_total_len = new_len + old_next_len;

            next.block = new_lblock.to_le();
            next.len = (new_total_len as u16).to_le();
            ext4_ext_store_pblock(next, new_pblock);
        }

        MergeDirection::Both => {
            // 双向合并：扩展前一个 extent，删除后一个 extent
            let prev_idx = insert_pos - 1;
            let next_idx = insert_pos;

            let next_len = extents[next_idx].actual_len() as u32;
            let prev = &mut extents[prev_idx];
            let new_total_len = prev.actual_len() as u32 + new_len + next_len;
            prev.len = (new_total_len as u16).to_le();

            // 删除后一个 extent
            extents.remove(next_idx);
        }
    }

    Ok(())
}

/// 在叶子节点中尝试合并并插入 extent
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `block_addr` - 叶子节点所在的块地址（0 表示在 inode 中）
/// * `node_type` - 节点类型
/// * `block_size` - 块大小
/// * `new_lblock` - 新 extent 的逻辑块起始
/// * `new_pblock` - 新 extent 的物理块起始
/// * `new_len` - 新 extent 的长度
/// * `new_is_unwritten` - 新 extent 是否为 unwritten
///
/// # 返回
///
/// 成功返回 true，失败（节点满）返回 false
pub fn try_merge_and_insert<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    block_addr: u64,
    node_type: ExtentNodeType,
    block_size: u32,
    new_lblock: u32,
    new_pblock: u64,
    new_len: u32,
    new_is_unwritten: bool,
) -> Result<bool> {
    if node_type == ExtentNodeType::Root {
        try_merge_and_insert_root(inode_ref, new_lblock, new_pblock, new_len, new_is_unwritten)
    } else {
        try_merge_and_insert_leaf_block(
            inode_ref.bdev(),
            block_addr,
            block_size,
            new_lblock,
            new_pblock,
            new_len,
            new_is_unwritten,
        )
    }
}

/// 在根节点（inode）中尝试合并并插入 extent
fn try_merge_and_insert_root<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    new_lblock: u32,
    new_pblock: u64,
    new_len: u32,
    new_is_unwritten: bool,
) -> Result<bool> {
    use super::split::{read_extents_from_inode, write_extents_to_inode};

    // 读取当前的 extent 数组
    let (mut extents, header) = read_extents_from_inode(inode_ref)?;

    // 检查是否有空间
    let entries = header.entries_count();
    let max_entries = header.max_entries();

    // 找到插入位置
    let insert_pos = extents
        .iter()
        .position(|e| e.logical_block() > new_lblock)
        .unwrap_or(extents.len());

    // 检查合并方向
    let direction = check_merge_direction(&extents, insert_pos, new_lblock, new_pblock, new_len, new_is_unwritten);

    // 计算合并后的 extent 数量
    let new_count = match direction {
        MergeDirection::None => entries + 1,
        MergeDirection::Prepend | MergeDirection::Append => entries,
        MergeDirection::Both => entries - 1,
    };

    // 检查是否有空间
    if new_count > max_entries {
        return Ok(false);
    }

    // 执行合并
    merge_extents(&mut extents, insert_pos, new_lblock, new_pblock, new_len, new_is_unwritten, direction)?;

    // 写回
    let mut new_header = header;
    new_header.entries = (extents.len() as u16).to_le();
    write_extents_to_inode(inode_ref, &new_header, &extents)?;

    Ok(true)
}

/// 在叶子块中尝试合并并插入 extent
fn try_merge_and_insert_leaf_block<D: BlockDevice>(
    bdev: &mut crate::block::BlockDev<D>,
    block_addr: u64,
    block_size: u32,
    new_lblock: u32,
    new_pblock: u64,
    new_len: u32,
    new_is_unwritten: bool,
) -> Result<bool> {
    use super::split::{read_extents_from_block, write_extents_to_block};

    // 读取当前的 extent 数组
    let (mut extents, header) = read_extents_from_block(bdev, block_addr, block_size)?;

    // 检查是否有空间
    let entries = header.entries_count();
    let max_entries = header.max_entries();

    // 找到插入位置
    let insert_pos = extents
        .iter()
        .position(|e| e.logical_block() > new_lblock)
        .unwrap_or(extents.len());

    // 检查合并方向
    let direction = check_merge_direction(&extents, insert_pos, new_lblock, new_pblock, new_len, new_is_unwritten);

    // 计算合并后的 extent 数量
    let new_count = match direction {
        MergeDirection::None => entries + 1,
        MergeDirection::Prepend | MergeDirection::Append => entries,
        MergeDirection::Both => entries - 1,
    };

    // 检查是否有空间
    if new_count > max_entries {
        return Ok(false);
    }

    // 执行合并
    merge_extents(&mut extents, insert_pos, new_lblock, new_pblock, new_len, new_is_unwritten, direction)?;

    // 写回
    let mut new_header = header;
    new_header.entries = (extents.len() as u16).to_le();
    write_extents_to_block(bdev, block_addr, block_size, &new_header, &extents)?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_prepend() {
        use super::super::unwritten::mark_unwritten;

        let mut prev = ext4_extent::default();
        prev.block = 0u32.to_le();
        prev.len = 10u16.to_le();
        prev.start_lo = 1000u32.to_le();
        prev.start_hi = 0u16.to_le();

        // 可以合并：连续的逻辑块和物理块（initialized extent）
        assert!(can_prepend(&prev, 10, 1010, 5, false));

        // 不能合并：逻辑块不连续
        assert!(!can_prepend(&prev, 11, 1010, 5, false));

        // 不能合并：物理块不连续
        assert!(!can_prepend(&prev, 10, 1011, 5, false));

        // 不能合并：长度超过最大值
        assert!(!can_prepend(&prev, 10, 1010, EXT4_EXT_MAX_LEN, false));

        // 不能合并：状态不同（prev 是 initialized，new 是 unwritten）
        assert!(!can_prepend(&prev, 10, 1010, 5, true));

        // 可以合并：两者都是 unwritten
        mark_unwritten(&mut prev);
        assert!(can_prepend(&prev, 10, 1010, 5, true));

        // 不能合并：状态不同（prev 是 unwritten，new 是 initialized）
        assert!(!can_prepend(&prev, 10, 1010, 5, false));
    }

    #[test]
    fn test_can_append() {
        use super::super::unwritten::mark_unwritten;

        let mut next = ext4_extent::default();
        next.block = 20u32.to_le();
        next.len = 10u16.to_le();
        next.start_lo = 2000u32.to_le();
        next.start_hi = 0u16.to_le();

        // 可以合并：连续的逻辑块和物理块（initialized extent）
        assert!(can_append(&next, 10, 1990, 10, false));

        // 不能合并：逻辑块不连续
        assert!(!can_append(&next, 10, 1990, 9, false));

        // 不能合并：物理块不连续
        assert!(!can_append(&next, 10, 1991, 10, false));

        // 不能合并：长度超过最大值
        assert!(!can_append(&next, 10, 1990, EXT4_EXT_MAX_LEN, false));

        // 不能合并：状态不同（next 是 initialized，new 是 unwritten）
        assert!(!can_append(&next, 10, 1990, 10, true));

        // 可以合并：两者都是 unwritten
        mark_unwritten(&mut next);
        assert!(can_append(&next, 10, 1990, 10, true));

        // 不能合并：状态不同（next 是 unwritten，new 是 initialized）
        assert!(!can_append(&next, 10, 1990, 10, false));
    }
}
