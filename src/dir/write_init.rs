//! 目录初始化操作
//!
//! 提供新目录的创建和初始化功能，包括普通目录和 HTree 索引目录。
//!
//! ## 功能
//!
//! - `append_new_block` - 分配新块并添加目录项
//! - `dir_init` - 初始化普通目录（创建 . 和 .. 条目）
//! - `dx_init` - 初始化 HTree 索引目录

use crate::{
    block::{Block, BlockDevice},
    consts::*,
    dir::checksum,
    error::Result,
    fs::InodeRef,
    superblock::Superblock,
    types::ext4_dir_entry_tail,
};

use super::write::{write_entry, update_dir_block_checksum, EXT4_DE_DIR};

/// 分配新的目录块并添加条目
///
/// 对应 lwext4 中目录空间不足时分配新块的逻辑
///
/// # 实现步骤
///
/// 1. 计算下一个逻辑块号
/// 2. 通过 extent::get_blocks() 分配物理块并更新 extent tree
/// 3. 初始化新块为空目录块
/// 4. 在新块中插入条目
/// 5. 更新 inode size
pub fn append_new_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    name: &str,
    child_inode: u32,
    file_type: u8,
    _required_len: u16,
) -> Result<()> {

    let block_size = sb.block_size();
    let has_csum = sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);

    // 计算下一个逻辑块号
    let current_size = inode_ref.size()?;
    let logical_block = (current_size / block_size as u64) as u32;

    // 使用 extent::get_blocks() 分配新块并更新 extent tree
    use crate::extent::get_blocks;
    use crate::balloc::BlockAllocator;

    let mut allocator = BlockAllocator::new();

    log::info!("[append_new_block] Allocating logical block {} for inode {}",
               logical_block, inode_ref.index());

    let (new_block_addr, _count) = get_blocks(inode_ref, sb, &mut allocator, logical_block, 1, true)?;

    log::info!("[append_new_block] Allocated physical block {new_block_addr} for logical block {logical_block}");

    // 初始化新块
    let uuid = sb.inner().uuid;
    let dir_inode = inode_ref.index();
    let inode_generation = inode_ref.generation()?;

    let bdev = inode_ref.bdev();
    let mut block = Block::get_noread(bdev, new_block_addr)?;

    block.with_data_mut(|data| {
        // 清零整个块
        data.fill(0);

        // 计算可用空间
        let entry_space = if has_csum {
            block_size as usize - core::mem::size_of::<ext4_dir_entry_tail>()
        } else {
            block_size as usize
        };

        // 创建单个条目，占据整个空间
        write_entry(data, 0, name, child_inode, file_type, entry_space as u16);

        // 如果需要校验和，初始化尾部
        if has_csum {
            let tail_offset = block_size as usize - core::mem::size_of::<ext4_dir_entry_tail>();
            let tail = unsafe {
                &mut *(data[tail_offset..].as_mut_ptr() as *mut ext4_dir_entry_tail)
            };
            checksum::init_entry_tail(tail);

            // 更新校验和
            update_dir_block_checksum(
                has_csum,
                &uuid,
                dir_inode,
                inode_generation,
                data,
                block_size as usize,
            );
        }
    })?;

    drop(block);

    // 更新 inode size
    let new_size = (logical_block as u64 + 1) * block_size as u64;
    inode_ref.set_size(new_size)?;

    Ok(())
}

/// 初始化新目录（创建 . 和 .. 条目）
///
/// 对应 lwext4 中创建新目录时调用 `ext4_dir_add_entry()` 两次
///
/// # 前提条件
///
/// - 目录至少有一个块已分配
/// - inode size >= block_size
///
/// # 实现说明
///
/// 在目录的第一个块中创建：
/// - `.` 条目（指向自己）
/// - `..` 条目（指向父目录）
///
/// issue: 默认block1已分配， 需要检查是否需要优化当前函数以移除默认条件
pub fn dir_init<D: BlockDevice>(
    dir_inode_ref: &mut InodeRef<D>,
    parent_inode: u32,
) -> Result<()> {
    let block_size = dir_inode_ref.sb().block_size();
    let has_csum = dir_inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);

    // 获取或分配第一个块（新目录需要创建块）
    let block_addr = dir_inode_ref.get_inode_dblk_idx(0, true)?;

    // 提取需要的数据
    let uuid = dir_inode_ref.sb().inner().uuid;
    let dir_inode = dir_inode_ref.index();
    let inode_generation = dir_inode_ref.generation()?;

    let bdev = dir_inode_ref.bdev();
    let mut block = Block::get_noread(bdev, block_addr)?;

    block.with_data_mut(|data| {
        // 清零整个块
        data.fill(0);

        // 计算可用空间
        let entry_space = if has_csum {
            block_size as usize - core::mem::size_of::<ext4_dir_entry_tail>()
        } else {
            block_size as usize
        };

        // 1. 创建 "." 条目（长度 12 字节）
        let dot_len = 12_u16;
        write_entry(data, 0, ".", dir_inode, EXT4_DE_DIR, dot_len);

        // 2. 创建 ".." 条目（占据剩余空间）
        let dotdot_offset = dot_len as usize;
        let dotdot_len = (entry_space - dot_len as usize) as u16;
        write_entry(data, dotdot_offset, "..", parent_inode, EXT4_DE_DIR, dotdot_len);

        // 3. 如果需要校验和，初始化尾部
        if has_csum {
            let tail_offset = block_size as usize - core::mem::size_of::<ext4_dir_entry_tail>();
            let tail = unsafe {
                &mut *(data[tail_offset..].as_mut_ptr() as *mut ext4_dir_entry_tail)
            };
            checksum::init_entry_tail(tail);

            // 更新校验和
            update_dir_block_checksum(
                has_csum,
                &uuid,
                dir_inode,
                inode_generation,
                data,
                block_size as usize,
            );
        }
    })?;

    drop(block);

    // 更新目录 inode 的 size（一个块）
    dir_inode_ref.set_size(block_size as u64)?;

    Ok(())
}

/// 初始化 HTree 索引目录
///
/// 对应 lwext4 的 `ext4_dir_dx_init()`
///
/// # 前提条件
///
/// - 目录至少有一个块已分配
/// - 文件系统支持 DIR_INDEX 特性
///
/// # 实现说明
///
/// 在块 0 创建 HTree 根节点结构，包括：
/// - `.` 和 `..` 条目（作为 dot entries）
/// - 根节点信息（hash 版本、间接层级等）
/// - 索引条目数组
///
/// issue: 1.初始化逻辑不完整 2.还没有被实际应用到mkdir的逻辑中 3.简化实现，默认block1已分配
pub fn dx_init<D: BlockDevice>(
    dir_inode_ref: &mut InodeRef<D>,
    parent_inode: u32,
) -> Result<()> {

    let block_size = dir_inode_ref.sb().block_size();
    let has_csum = dir_inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);

    // 获取或分配第一个块（根块）
    let block_addr = dir_inode_ref.get_inode_dblk_idx(0, true)?;

    // 提取需要的数据
    let uuid = dir_inode_ref.sb().inner().uuid;
    let dir_inode = dir_inode_ref.index();
    let inode_generation = dir_inode_ref.generation()?;
    let hash_version = dir_inode_ref.sb().inner().def_hash_version;

    let bdev = dir_inode_ref.bdev();
    let mut block = Block::get_noread(bdev, block_addr)?;

    block.with_data_mut(|data| {
        // 清零整个块
        data.fill(0);

        // 1. 创建 . 和 .. 条目（作为特殊的 dot entries）
        write_entry(data, 0, ".", dir_inode, EXT4_DE_DIR, 12);

        let dotdot_len = block_size - 12;
        write_entry(data, 12, "..", parent_inode, EXT4_DE_DIR, dotdot_len as u16);

        // 2. 初始化 HTree 根信息
        let root_info_offset = 12 + 12;

        // hash_version (1 byte) at offset
        data[root_info_offset] = hash_version;
        // info_length (1 byte) = 8
        data[root_info_offset + 1] = 8;
        // indirect_levels (1 byte) = 0
        data[root_info_offset + 2] = 0;
        // unused (1 byte) = 0
        data[root_info_offset + 3] = 0;

        // 3. 设置索引条目限制和计数
        let entries_offset = root_info_offset + 8;

        // 计算可用空间
        let entry_space = if has_csum {
            block_size as usize - entries_offset - core::mem::size_of::<ext4_dir_entry_tail>()
        } else {
            block_size as usize - entries_offset
        };

        // 每个索引条目 8 字节
        let max_entries = (entry_space / 8) as u16;

        // count_limit 结构：limit(2) + count(2)
        let limit_offset = entries_offset;
        data[limit_offset..limit_offset + 2].copy_from_slice(&max_entries.to_le_bytes());
        // count = 1
        data[limit_offset + 2..limit_offset + 4].copy_from_slice(&1_u16.to_le_bytes());

        // 4. 添加第一个索引条目（hash=0, block=1）
        let first_entry_offset = entries_offset + 4;
        data[first_entry_offset..first_entry_offset + 4].copy_from_slice(&0_u32.to_le_bytes());
        data[first_entry_offset + 4..first_entry_offset + 8].copy_from_slice(&1_u32.to_le_bytes());

        // 5. 如果需要校验和，初始化尾部
        if has_csum {
            let tail_offset = block_size as usize - core::mem::size_of::<ext4_dir_entry_tail>();
            let tail = unsafe {
                &mut *(data[tail_offset..].as_mut_ptr() as *mut ext4_dir_entry_tail)
            };
            checksum::init_entry_tail(tail);

            update_dir_block_checksum(
                has_csum,
                &uuid,
                dir_inode,
                inode_generation,
                data,
                block_size as usize,
            );
        }
    })?;

    drop(block);

    // 更新目录 inode 的 size（一个块）
    dir_inode_ref.set_size(block_size as u64)?;

    Ok(())
}
