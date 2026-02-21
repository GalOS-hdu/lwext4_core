//! Extent 空间移除功能（多层树支持）
//!
//! 实现 extent 树的空间删除，支持任意深度的树结构
//!
//! ## 主要功能
//!
//! - 删除指定范围的 extent
//! - 支持完全删除、部分截断、中间分裂
//! - 自动释放物理块
//! - 支持多层 extent 树
//!
//! ## 对应 lwext4
//!
//! - `ext4_extent_remove_space()` - 主删除函数
//!
//! ## 实现策略
//!
//! 1. 遍历 extent 树，找到所有与删除范围重叠的 extent
//! 2. 对每个 extent 执行删除/截断/分裂操作
//! 3. 释放对应的物理块
//! 4. 更新树结构（暂不实现节点合并）

use crate::{
    balloc::BlockAllocator,
    block::{Block, BlockDevice},
    error::Result,
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_extent, ext4_extent_header},
};

use super::{
    helpers::ext4_ext_store_pblock,
    write::ExtentNodeType,
};

use alloc::vec::Vec;

/// Extent 删除操作类型
#[derive(Debug, Clone, Copy)]
enum RemoveOp {
    /// 完全删除这个 extent
    Delete {
        /// Extent 在叶子节点中的索引
        index: usize,
        /// 起始物理块
        start_pblock: u64,
        /// 块数量
        count: u32,
    },

    /// 截断 extent 的开头
    TruncateStart {
        /// Extent 在叶子节点中的索引
        index: usize,
        /// 新的起始逻辑块
        new_start_lblock: u32,
        /// 新的起始物理块
        new_start_pblock: u64,
        /// 新的长度
        new_len: u16,
        /// 释放的起始物理块
        free_pblock: u64,
        /// 释放的块数量
        free_count: u32,
    },

    /// 截断 extent 的结尾
    TruncateEnd {
        /// Extent 在叶子节点中的索引
        index: usize,
        /// 新的长度
        new_len: u16,
        /// 释放的起始物理块
        free_pblock: u64,
        /// 释放的块数量
        free_count: u32,
    },

    /// 在 extent 中间删除（分裂成两个）
    SplitMiddle {
        /// Extent 在叶子节点中的索引
        index: usize,
        /// 左侧 extent 的新长度
        left_len: u16,
        /// 右侧 extent 的逻辑块起始
        right_start_lblock: u32,
        /// 右侧 extent 的物理块起始
        right_start_pblock: u64,
        /// 右侧 extent 的长度
        right_len: u16,
        /// 释放的起始物理块
        free_pblock: u64,
        /// 释放的块数量
        free_count: u32,
    },
}

/// 叶子节点的删除操作信息
#[derive(Debug)]
struct LeafRemoveInfo {
    /// 叶子节点所在的块地址（0 表示在 inode 中）
    block_addr: u64,
    /// 节点类型
    node_type: ExtentNodeType,
    /// 要执行的删除操作列表
    operations: Vec<RemoveOp>,
}

/// 移除 extent 空间（多层树支持）
///
/// 对应 lwext4 的 `ext4_extent_remove_space()`
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `allocator` - 块分配器
/// * `from` - 起始逻辑块号
/// * `to` - 结束逻辑块号（包含，u32::MAX 表示到文件末尾）
///
/// # 实现逻辑
///
/// 1. 遍历 extent 树的所有叶子节点
/// 2. 对每个叶子节点，找出所有与删除范围重叠的 extent
/// 3. 计算每个 extent 的删除操作（删除、截断、分裂）
/// 4. 执行所有删除操作
/// 5. 释放对应的物理块
///
/// # 当前限制
///
/// - ⚠️ 不会删除空的叶子节点
/// - ⚠️ 不会减少树的深度
/// - ⚠️ 不会合并相邻节点
/// - ⚠️ 不会重新平衡树
///
/// 这些优化功能可以在后续版本中添加。
pub fn remove_space_multilevel<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    from: u32,
    to: u32,
) -> Result<()> {
    let block_size = sb.block_size();

    // 1. 遍历树，收集所有需要执行的删除操作
    let leaf_ops = collect_remove_operations(inode_ref, block_size, from, to)?;

    // 2. 对每个叶子节点执行删除操作
    for leaf_info in leaf_ops {
        execute_leaf_operations(
            inode_ref,
            sb,
            allocator,
            &leaf_info,
            block_size,
        )?;
    }

    Ok(())
}

/// 遍历树，收集所有需要执行的删除操作
fn collect_remove_operations<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    block_size: u32,
    from: u32,
    to: u32,
) -> Result<Vec<LeafRemoveInfo>> {
    let mut result = Vec::new();

    // 读取根节点信息
    let (_depth, root_is_leaf) = inode_ref.with_inode(|inode| -> Result<(u16, bool)> {
        let header = inode.extent_header();

        let depth = header.depth();
        let is_leaf = header.is_leaf();

        Ok((depth, is_leaf))
    })??;

    // 如果根节点就是叶子（深度 0）
    if root_is_leaf {
        let ops = collect_leaf_operations_from_root(inode_ref, from, to)?;
        if !ops.is_empty() {
            result.push(LeafRemoveInfo {
                block_addr: 0,
                node_type: ExtentNodeType::Root,
                operations: ops,
            });
        }
        return Ok(result);
    }

    // 多层树：需要遍历所有叶子节点
    // 使用递归或迭代方式遍历
    traverse_tree_for_remove(
        inode_ref,
        block_size,
        &mut result,
        from,
        to,
    )?;

    Ok(result)
}

/// 从根节点（inode）收集删除操作
fn collect_leaf_operations_from_root<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    from: u32,
    to: u32,
) -> Result<Vec<RemoveOp>> {
    inode_ref.with_inode(|inode| {
        let data = inode.extent_root_data();

        collect_leaf_operations(data, from, to)
    })?
}

/// 从叶子节点数据中收集删除操作
fn collect_leaf_operations(
    node_data: &[u8],
    from: u32,
    to: u32,
) -> Result<Vec<RemoveOp>> {
    let mut operations = Vec::new();

    let header = unsafe {
        *(node_data.as_ptr() as *const ext4_extent_header)
    };

    let entries = header.entries_count();
    let header_size = core::mem::size_of::<ext4_extent_header>();
    let extent_size = core::mem::size_of::<ext4_extent>();

    // 遍历所有 extent
    for i in 0..entries as usize {
        let offset = header_size + i * extent_size;
        if offset + extent_size > node_data.len() {
            break;
        }

        let extent = unsafe {
            *(node_data.as_ptr().add(offset) as *const ext4_extent)
        };

        let ee_block = extent.logical_block();
        let ee_len = extent.actual_len() as u32;
        let ee_start = extent.physical_block();
        let ee_end = ee_block + ee_len - 1;

        // 检查是否与删除范围重叠
        if ee_end < from || ee_block > to {
            // 不重叠，跳过
            continue;
        }

        // 计算删除操作类型
        let op = if ee_block >= from && ee_end <= to {
            // 情况 1: 完全删除
            RemoveOp::Delete {
                index: i,
                start_pblock: ee_start,
                count: ee_len,
            }
        } else if from <= ee_block && to >= ee_block && to < ee_end {
            // 情况 2: 删除开头，保留结尾
            let delete_len = (to - ee_block + 1) as u32;
            let new_start_lblock = to + 1;
            let new_start_pblock = ee_start + delete_len as u64;
            let new_len = (ee_len - delete_len) as u16;

            RemoveOp::TruncateStart {
                index: i,
                new_start_lblock,
                new_start_pblock,
                new_len,
                free_pblock: ee_start,
                free_count: delete_len,
            }
        } else if from > ee_block && from <= ee_end && to >= ee_end {
            // 情况 3: 删除结尾，保留开头
            let new_len = (from - ee_block) as u16;
            let free_pblock = ee_start + new_len as u64;
            let free_count = ee_len - new_len as u32;

            RemoveOp::TruncateEnd {
                index: i,
                new_len,
                free_pblock,
                free_count,
            }
        } else if from > ee_block && to < ee_end {
            // 情况 4: 删除中间，分裂成两个
            let left_len = (from - ee_block) as u16;
            let delete_len = to - from + 1;
            let right_start_lblock = to + 1;
            let right_start_pblock = ee_start + (from - ee_block) as u64 + delete_len as u64;
            let right_len = (ee_end - to) as u16;

            RemoveOp::SplitMiddle {
                index: i,
                left_len,
                right_start_lblock,
                right_start_pblock,
                right_len,
                free_pblock: ee_start + left_len as u64,
                free_count: delete_len,
            }
        } else {
            // 不应该到达这里
            continue;
        };

        operations.push(op);
    }

    Ok(operations)
}

/// 遍历树收集删除操作（多层树）
fn traverse_tree_for_remove<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    block_size: u32,
    result: &mut Vec<LeafRemoveInfo>,
    from: u32,
    to: u32,
) -> Result<()> {
    // 使用 DFS 遍历整个树
    // 从根节点开始
    let root_data = inode_ref.with_inode(|inode| -> Result<Vec<u8>> {
        let data = inode.extent_root_data();
        Ok(data.to_vec())
    })??;

    traverse_node_for_remove(
        inode_ref.bdev(),
        &root_data,
        0, // root block_addr = 0
        block_size,
        result,
        from,
        to,
    )?;

    Ok(())
}

/// 递归遍历节点收集删除操作
fn traverse_node_for_remove<D: BlockDevice>(
    bdev: &mut crate::block::BlockDev<D>,
    node_data: &[u8],
    node_block_addr: u64,
    block_size: u32,
    result: &mut Vec<LeafRemoveInfo>,
    from: u32,
    to: u32,
) -> Result<()> {
    let header = unsafe {
        *(node_data.as_ptr() as *const ext4_extent_header)
    };

    if header.is_leaf() {
        // 这是叶子节点，收集删除操作
        let ops = collect_leaf_operations(node_data, from, to)?;
        if !ops.is_empty() {
            let node_type = if node_block_addr == 0 {
                ExtentNodeType::Root
            } else {
                ExtentNodeType::Leaf
            };

            result.push(LeafRemoveInfo {
                block_addr: node_block_addr,
                node_type,
                operations: ops,
            });
        }
    } else {
        // 这是索引节点，递归遍历子节点
        let entries = header.entries_count();
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let idx_size = core::mem::size_of::<crate::types::ext4_extent_idx>();

        for i in 0..entries as usize {
            let offset = header_size + i * idx_size;
            if offset + idx_size > node_data.len() {
                break;
            }

            let idx = unsafe {
                *(node_data.as_ptr().add(offset) as *const crate::types::ext4_extent_idx)
            };

            let idx_block = idx.logical_block();
            let child_pblock = idx.leaf_block();

            // 检查这个子树是否可能包含需要删除的范围
            // 获取下一个索引的起始块（如果有的话）
            let next_idx_block = if i + 1 < entries as usize {
                let next_offset = header_size + (i + 1) * idx_size;
                let next_idx = unsafe {
                    *(node_data.as_ptr().add(next_offset) as *const crate::types::ext4_extent_idx)
                };
                next_idx.logical_block()
            } else {
                u32::MAX
            };

            // 如果删除范围与这个子树的范围有重叠
            if idx_block <= to && from < next_idx_block {
                // 读取子节点
                let child_data = {
                    let mut block = Block::get(bdev, child_pblock)?;
                    block.with_data(|data| -> Result<Vec<u8>> {
                        Ok(data[0..block_size as usize].to_vec())
                    })?
                }?;

                // 递归处理子节点
                traverse_node_for_remove(
                    bdev,
                    &child_data,
                    child_pblock,
                    block_size,
                    result,
                    from,
                    to,
                )?;
            }
        }
    }

    Ok(())
}

/// 执行叶子节点的删除操作
fn execute_leaf_operations<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    _allocator: &mut BlockAllocator,
    leaf_info: &LeafRemoveInfo,
    block_size: u32,
) -> Result<()> {
    // 1. 先释放所有物理块
    for op in &leaf_info.operations {
        match op {
            RemoveOp::Delete { start_pblock, count, .. } => {
                crate::balloc::free_blocks(inode_ref.bdev(), sb, *start_pblock, *count)?;
            }
            RemoveOp::TruncateStart { free_pblock, free_count, .. } => {
                crate::balloc::free_blocks(inode_ref.bdev(), sb, *free_pblock, *free_count)?;
            }
            RemoveOp::TruncateEnd { free_pblock, free_count, .. } => {
                crate::balloc::free_blocks(inode_ref.bdev(), sb, *free_pblock, *free_count)?;
            }
            RemoveOp::SplitMiddle { free_pblock, free_count, .. } => {
                crate::balloc::free_blocks(inode_ref.bdev(), sb, *free_pblock, *free_count)?;
            }
        }
    }

    // 2. 更新叶子节点的 extent 数组
    if leaf_info.node_type == ExtentNodeType::Root {
        update_root_extents(inode_ref, &leaf_info.operations)?;
    } else {
        update_leaf_block_extents(
            inode_ref.bdev(),
            leaf_info.block_addr,
            block_size,
            &leaf_info.operations,
        )?;
    }

    Ok(())
}

/// 更新根节点（inode）中的 extent
fn update_root_extents<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    operations: &[RemoveOp],
) -> Result<()> {
    inode_ref.with_inode_mut(|inode| {
        let data = inode.extent_root_data_mut();

        update_extent_array(data, operations)
    })??;

    inode_ref.mark_dirty()?;
    Ok(())
}

/// 更新块中的 extent 数组
fn update_leaf_block_extents<D: BlockDevice>(
    bdev: &mut crate::block::BlockDev<D>,
    block_addr: u64,
    block_size: u32,
    operations: &[RemoveOp],
) -> Result<()> {
    let mut block = Block::get(bdev, block_addr)?;

    block.with_data_mut(|data| {
        update_extent_array(&mut data[0..block_size as usize], operations)
    })??;

    // Block 会在 drop 时自动标记为 dirty
    Ok(())
}

/// 更新 extent 数组（核心逻辑）
fn update_extent_array(
    node_data: &mut [u8],
    operations: &[RemoveOp],
) -> Result<()> {
    let header = unsafe {
        &mut *(node_data.as_mut_ptr() as *mut ext4_extent_header)
    };

    let header_size = core::mem::size_of::<ext4_extent_header>();
    let extent_size = core::mem::size_of::<ext4_extent>();
    let entries = header.entries_count();

    // 创建新的 extent 数组
    let mut new_extents = Vec::new();

    // 遍历原有的 extent
    for i in 0..entries as usize {
        let offset = header_size + i * extent_size;
        let extent = unsafe {
            *(node_data.as_ptr().add(offset) as *const ext4_extent)
        };

        // 查找这个 extent 是否有对应的操作
        let op = operations.iter().find(|op| {
            match op {
                RemoveOp::Delete { index, .. } => *index == i,
                RemoveOp::TruncateStart { index, .. } => *index == i,
                RemoveOp::TruncateEnd { index, .. } => *index == i,
                RemoveOp::SplitMiddle { index, .. } => *index == i,
            }
        });

        match op {
            Some(RemoveOp::Delete { .. }) => {
                // 完全删除，不添加到新数组
            }
            Some(RemoveOp::TruncateStart {
                new_start_lblock,
                new_start_pblock,
                new_len,
                ..
            }) => {
                // 截断开头，创建新的 extent
                let mut new_extent = extent;
                new_extent.block = new_start_lblock.to_le();
                new_extent.len = new_len.to_le();
                ext4_ext_store_pblock(&mut new_extent, *new_start_pblock);
                new_extents.push(new_extent);
            }
            Some(RemoveOp::TruncateEnd { new_len, .. }) => {
                // 截断结尾，更新长度
                let mut new_extent = extent;
                new_extent.len = new_len.to_le();
                new_extents.push(new_extent);
            }
            Some(RemoveOp::SplitMiddle {
                left_len,
                right_start_lblock,
                right_start_pblock,
                right_len,
                ..
            }) => {
                // 分裂成两个
                // 左侧
                let mut left_extent = extent;
                left_extent.len = left_len.to_le();
                new_extents.push(left_extent);

                // 右侧
                let mut right_extent = extent;
                right_extent.block = right_start_lblock.to_le();
                right_extent.len = right_len.to_le();
                ext4_ext_store_pblock(&mut right_extent, *right_start_pblock);
                new_extents.push(right_extent);
            }
            None => {
                // 没有操作，保留原 extent
                new_extents.push(extent);
            }
        }
    }

    // 写入新的 extent 数组
    header.entries = (new_extents.len() as u16).to_le();

    for (i, extent) in new_extents.iter().enumerate() {
        let offset = header_size + i * extent_size;
        unsafe {
            *(node_data.as_mut_ptr().add(offset) as *mut ext4_extent) = *extent;
        }
    }

    // 清零剩余空间
    let used_size = header_size + new_extents.len() * extent_size;
    let old_used_size = header_size + entries as usize * extent_size;
    if used_size < old_used_size {
        node_data[used_size..old_used_size].fill(0);
    }

    Ok(())
}
