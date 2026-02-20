//! balloc 与文件系统的集成
//!
//! 提供与 InodeRef 集成的块分配/释放函数，自动更新 inode blocks 计数
//! issue: 实际并未被使用

use crate::{
    block::{BlockDev, BlockDevice},
    error::Result,
    fs::InodeRef,
    superblock::Superblock,
};

use super::{alloc::*, free::*};

/// 释放单个块（带 inode 更新）
///
/// 对应 lwext4 的 `ext4_balloc_free_block()` 完整版本
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `inode_ref` - inode 引用（会自动更新 blocks 计数）
/// * `baddr` - 要释放的块地址
///
/// # 返回
///
/// 成功返回 ()
pub fn free_block_with_inode<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &mut Superblock,
    inode_ref: &mut InodeRef<D>,
    baddr: u64,
) -> Result<()> {
    // 先释放块
    free_block(bdev, sb, baddr)?;

    // 更新 inode blocks 计数
    inode_ref.sub_blocks(1)?;

    Ok(())
}

/// 释放多个块（带 inode 更新）
///
/// 对应 lwext4 的 `ext4_balloc_free_blocks()` 完整版本
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `inode_ref` - inode 引用（会自动更新 blocks 计数）
/// * `first` - 第一个要释放的块地址
/// * `count` - 要释放的块数量
///
/// # 返回
///
/// 成功返回 ()
pub fn free_blocks_with_inode<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &mut Superblock,
    inode_ref: &mut InodeRef<D>,
    first: u64,
    count: u32,
) -> Result<()> {
    // 先释放块
    free_blocks(bdev, sb, first, count)?;

    // 更新 inode blocks 计数
    inode_ref.sub_blocks(count)?;

    Ok(())
}

/// 分配块（带 inode 更新）
///
/// 使用块分配器分配块，并自动更新 inode blocks 计数
///
/// # 参数
///
/// * `allocator` - 块分配器
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `inode_ref` - inode 引用（会自动更新 blocks 计数）
/// * `goal` - 目标块地址（提示）
///
/// # 返回
///
/// 成功返回分配的块地址
pub fn alloc_block_with_inode<D: BlockDevice>(
    allocator: &mut BlockAllocator,
    bdev: &mut BlockDev<D>,
    sb: &mut Superblock,
    inode_ref: &mut InodeRef<D>,
    goal: u64,
) -> Result<u64> {
    // 分配块
    let baddr = allocator.alloc_block(bdev, sb, goal)?;

    // 更新 inode blocks 计数
    inode_ref.add_blocks(1)?;

    Ok(baddr)
}

/// 尝试分配特定块（带 inode 更新）
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `inode_ref` - inode 引用（会自动更新 blocks 计数）
/// * `baddr` - 要尝试分配的块地址
///
/// # 返回
///
/// 成功返回 true（块已分配），false（块已被占用）
pub fn try_alloc_block_with_inode<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &mut Superblock,
    inode_ref: &mut InodeRef<D>,
    baddr: u64,
) -> Result<bool> {
    // 尝试分配块
    let allocated = try_alloc_block(bdev, sb, baddr)?;

    // 如果分配成功，更新 inode blocks 计数
    if allocated {
        inode_ref.add_blocks(1)?;
    }

    Ok(allocated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_balloc_fs_integration_api() {
        // 这些测试需要实际的块设备和 ext4 文件系统
        // 主要是验证 API 的设计和编译
    }
}
