//! 目录写操作
//!
//! 提供目录项的添加、删除等写操作功能
//!
//! 对应 lwext4 的 ext4_dir.c 中的写操作部分
//!
//! ## 功能
//!
//! - ✅ 普通目录添加条目（支持分配新块）
//! - ✅ HTree 目录添加条目（支持叶子块分裂）
//! - ✅ HTree 叶子块分裂
//! - ✅ 删除目录条目
//! - ✅ 目录校验和更新
//!
//! ## 限制
//!
//! - ⚠️ HTree 索引块满时不支持递归分裂（部分实现）
//! - ⚠️ HTree 根节点分裂未完全集成（功能已实现）
//! - ❌ 不支持内联数据（inline data）目录
//!
//! ## 使用示例
//!
//! ```rust,ignore
//! use lwext4_core::dir::write::*;
//!
//! // 添加目录项
//! add_entry(&mut inode_ref, "newfile.txt", child_inode, EXT4_DE_REG_FILE)?;
//!
//! // 删除目录项
//! remove_entry(&mut inode_ref, "oldfile.txt")?;
//! ```

use crate::{
    block::{Block, BlockDevice},
    consts::*,
    dir::{checksum, htree},
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_dir_entry, ext4_dir_entry_tail},
};

/// 目录项类型常量
pub const EXT4_DE_UNKNOWN: u8 = 0;
pub const EXT4_DE_REG_FILE: u8 = 1;
pub const EXT4_DE_DIR: u8 = 2;
pub const EXT4_DE_CHRDEV: u8 = 3;
pub const EXT4_DE_BLKDEV: u8 = 4;
pub const EXT4_DE_FIFO: u8 = 5;
pub const EXT4_DE_SOCK: u8 = 6;
pub const EXT4_DE_SYMLINK: u8 = 7;

/// 向目录添加新条目
///
/// 对应 lwext4 的 `ext4_dir_add_entry()`
///
/// # 参数
///
/// * `inode_ref` - 目录 inode 引用
/// * `sb` - 可变 superblock 引用（用于可能的块分配）
/// * `name` - 条目名称
/// * `child_inode` - 子 inode 编号
/// * `file_type` - 文件类型（EXT4_DE_* 常量）
///
/// # 返回
///
/// 成功返回 Ok(())
///
/// # 注意
///
/// - 对于普通目录，如果空间不足会自动分配新块
/// - 对于 HTree 目录，如果叶子块满了会返回 NoSpace 错误
pub fn add_entry<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    name: &str,
    child_inode: u32,
    file_type: u8,
) -> Result<()> {
    // 检查名称长度
    if name.is_empty() || name.len() > 255 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Directory entry name too long or empty",
        ));
    }

    // 检查是否是 HTree 索引目录
    let is_htree = htree::is_indexed(inode_ref)?;

    if is_htree {
        // HTree 目录
        add_entry_htree(inode_ref, sb, name, child_inode, file_type)
    } else {
        // 普通目录
        add_entry_linear(inode_ref, sb, name, child_inode, file_type)
    }
}

/// 向普通目录（线性扫描）添加条目
///
/// 对应 lwext4 的线性目录处理部分
fn add_entry_linear<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    name: &str,
    child_inode: u32,
    file_type: u8,
) -> Result<()> {
    // 计算所需的目录项长度（8字节对齐）
    let name_len = name.len();
    let required_len = calculate_entry_len(name_len as u8);

    // 遍历目录的所有块，查找空闲空间
    let mut block_idx = 0_u32;
    loop {

        // 尝试获取当前块
        let block_addr = match inode_ref.get_inode_dblk_idx(block_idx, false) {
            Ok(addr) => addr,
            Err(_) => {
                // 没有更多块了，需要分配新块
                return append_new_block(
                    inode_ref,
                    sb,
                    name,
                    child_inode,
                    file_type,
                    required_len,
                );
            }
        };

        {
            // 在获取 bdev 之前提取所有需要的数据（不保留引用）
            let has_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
            let block_size = inode_ref.sb().block_size() as usize;
            let uuid = inode_ref.sb().inner().uuid;
            let inode_index = inode_ref.index();
            let inode_generation = inode_ref.generation()?;

            let bdev = inode_ref.bdev();
            let mut block = Block::get(bdev, block_addr)?;

            // 在当前块中查找空闲空间并更新校验和
            let insert_result = block.with_data_mut(|data| {
                let result = find_and_insert_entry(
                    data,
                    name,
                    child_inode,
                    file_type,
                    required_len,
                );

                if result {
                    // 成功插入，更新校验和（如果需要）
                    update_dir_block_checksum(
                        has_csum,
                        &uuid,
                        inode_index,
                        inode_generation,
                        data,
                        block_size,
                    );
                }

                result
            })?;

            drop(block);

            if insert_result {
                // 标记块为脏（需要通过 transaction）
                // 注意：这里假设 block 的 Drop 会自动处理
                return Ok(());
            }
        }

        // 当前块没有空间，尝试下一块
        block_idx += 1;
    }
}

/// Handle leaf block split and retry insertion
///
/// Called when the target leaf block is full
fn handle_leaf_split<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    hash_info: &htree::HTreeHashInfo,
    path: &htree::HTreePath,
    old_block_addr: u64,
    name: &str,
    child_inode: u32,
    file_type: u8,
) -> Result<()> {
    // Split the leaf block
    let (new_logical_block, split_hash) = htree::split_leaf_block(
        inode_ref,
        sb,
        old_block_addr,
        hash_info,
    )?;

    // Insert index entry pointing to the new block
    // The parent is the last index block in the path
    if let Some(parent_info) = path.index_blocks.last() {
        // Check if parent has space for the new entry
        if parent_info.entry_count >= parent_info.entry_limit {
            // Parent is full, need to split index block
            // For now, return an error (TODO: implement recursive index split)
            return Err(Error::new(
                ErrorKind::NoSpace,
                "Index block is full, recursive split not yet implemented",
            ));
        }

        // Insert the new index entry at position_idx + 1
        // (right after where we found the original leaf)
        let insert_position = parent_info.position_idx + 1;

        insert_index_entry_at(
            inode_ref,
            parent_info.block_addr,
            insert_position,
            split_hash,
            new_logical_block,
        )?;
    } else {
        // No parent in path means this is a root-only tree (indirect_levels == 0)
        // In this case, we need to do a root split which grows the tree
        // For now, return an error (TODO: implement root growth)
        // issue: if this means inline data?
        return Err(Error::new(
            ErrorKind::NoSpace,
            "Root split not yet implemented in add_entry",
        ));
    }

    // Retry the insertion: decide which block to use based on hash
    let target_block = if hash_info.hash >= split_hash {
        // Insert into new block
        new_logical_block
    } else {
        // Insert into old block
        path.leaf_block
    };

    // Get the target block address
    let target_block_addr = inode_ref.get_inode_dblk_idx(target_block, false)?;

    // Prepare data for checksum
    let has_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
    let block_size = inode_ref.sb().block_size() as usize;
    let uuid = inode_ref.sb().inner().uuid;
    let inode_index = inode_ref.index();
    let inode_generation = inode_ref.generation()?;
    let required_len = calculate_entry_len(name.len() as u8);

    // Try to insert into the target block
    let bdev = inode_ref.bdev();
    let mut block = Block::get(bdev, target_block_addr)?;

    let insert_result = block.with_data_mut(|data| {
        let result = find_and_insert_entry(
            data,
            name,
            child_inode,
            file_type,
            required_len,
        );

        if result {
            update_dir_block_checksum(
                has_csum,
                &uuid,
                inode_index,
                inode_generation,
                data,
                block_size,
            );
        }

        result
    })?;

    drop(block);

    if !insert_result {
        return Err(Error::new(
            ErrorKind::NoSpace,
            "Failed to insert entry after split",
        ));
    }

    Ok(())
}

/// Insert an index entry into an index block at a specific position
///
/// Wrapper around htree module's internal function
fn insert_index_entry_at<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    index_block_addr: u64,
    insert_position: usize,
    hash: u32,
    logical_block: u32,
) -> Result<()> {
    let has_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
    let block_size = inode_ref.sb().block_size() as usize;

    let bdev = inode_ref.bdev();
    let mut block = Block::get(bdev, index_block_addr)?;

    block.with_data_mut(|data| {
        // Determine entries starting position
        let is_root = {
            let fake_entry = unsafe { &*(data.as_ptr() as *const crate::types::ext4_fake_dir_entry) };
            // Root block has dot entries
            u16::from_le(fake_entry.entry_len) != block_size as u16
        };

        let entries_offset = if is_root {
            // Root: skip dot entries (2*12) + root info (8)
            2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>()
                + core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>()
        } else {
            // Non-root: skip fake entry
            core::mem::size_of::<crate::types::ext4_fake_dir_entry>()
        };

        // Read climit
        let climit_ptr = unsafe {
            data.as_mut_ptr().add(entries_offset) as *mut crate::types::ext4_dir_idx_climit
        };
        let climit = unsafe { &mut *climit_ptr };
        let count = u16::from_le(climit.count);

        // Calculate insertion position
        let entry_size = core::mem::size_of::<crate::types::ext4_dir_idx_entry>();
        let insert_offset = entries_offset + core::mem::size_of::<crate::types::ext4_dir_idx_climit>() + entry_size * insert_position;
        let old_entry_ptr = unsafe { data.as_ptr().add(insert_offset) };
        let new_entry_ptr = unsafe { data.as_mut_ptr().add(insert_offset + entry_size) };

        // Move subsequent entries to make room
        let bytes_to_move = entry_size * (count as usize - insert_position);
        if bytes_to_move > 0 {
            unsafe {
                core::ptr::copy(
                    old_entry_ptr,
                    new_entry_ptr,
                    bytes_to_move
                );
            }
        }

        // Write new entry
        let new_entry = unsafe {
            &mut *(data.as_mut_ptr().add(insert_offset) as *mut crate::types::ext4_dir_idx_entry)
        };
        new_entry.hash = hash.to_le();
        new_entry.block = logical_block.to_le();

        // Update count
        climit.count = (count + 1).to_le();

        // Update checksum if needed
        // TODO: Implement index block checksum properly
        if has_csum {
            // For now, just a placeholder
        }
    })?;

    Ok(())
}

/// 向 HTree 索引目录添加条目
///
/// 对应 lwext4 的 `ext4_dir_dx_add_entry()`
///
/// 支持叶子块分裂。当叶子块满时自动分裂并重试插入。
///
/// ⚠️ **部分限制**：索引块满时不支持递归分裂（返回错误）
fn add_entry_htree<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    name: &str,
    child_inode: u32,
    file_type: u8,
) -> Result<()> {
    // 计算哈希值
    let hash_info = htree::init_hash_info(inode_ref, name)?;

    // 找到目标叶子块及其路径
    let path = htree::get_leaf_with_path(inode_ref, &hash_info)?;
    let leaf_block_idx = path.leaf_block;

    // 获取物理块地址
    let block_addr = inode_ref.get_inode_dblk_idx(leaf_block_idx, false)?;

    // 计算所需长度
    let required_len = calculate_entry_len(name.len() as u8);

    // 在获取 bdev 之前提取所有需要的数据
    let has_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
    let block_size = inode_ref.sb().block_size() as usize;
    let uuid = inode_ref.sb().inner().uuid;
    let inode_index = inode_ref.index();
    let inode_generation = inode_ref.generation()?;

    let bdev = inode_ref.bdev();
    let mut block = Block::get(bdev, block_addr)?;

    // 在叶子块中插入条目并更新校验和
    let insert_result = block.with_data_mut(|data| {
        let result = find_and_insert_entry(
            data,
            name,
            child_inode,
            file_type,
            required_len,
        );

        if result {
            // 成功插入，更新校验和
            update_dir_block_checksum(
                has_csum,
                &uuid,
                inode_index,
                inode_generation,
                data,
                block_size,
            );
        }

        result
    })?;

    drop(block);

    if !insert_result {
        // 叶子块满了，需要分裂
        handle_leaf_split(
            inode_ref,
            sb,
            &hash_info,
            &path,
            block_addr,
            name,
            child_inode,
            file_type,
        )?;
    }

    Ok(())
}

/// 在块中查找空闲空间并插入条目
///
/// # 返回
///
/// 成功插入返回 true，空间不足返回 false
fn find_and_insert_entry(
    data: &mut [u8],
    name: &str,
    child_inode: u32,
    file_type: u8,
    required_len: u16,
) -> bool {
    let mut offset = 0;
    let mut entries_checked = 0;

    log::trace!(
        "[find_and_insert_entry] START: name='{}', required_len={}, block_size={}",
        name,
        required_len,
        data.len()
    );

    while offset < data.len() {
        entries_checked += 1;
        if offset + core::mem::size_of::<ext4_dir_entry>() > data.len() {
            break;
        }

        let entry = unsafe {
            &*(data[offset..].as_ptr() as *const ext4_dir_entry)
        };

        let rec_len = u16::from_le(entry.rec_len);

        if rec_len == 0 {
            break;
        }

        let entry_inode = u32::from_le(entry.inode);
        let actual_len = if entry_inode != 0 {
            calculate_entry_len(entry.name_len)
        } else {
            0
        };

        // 使用 checked_sub 避免下溢，如果 actual_len > rec_len 则跳过该条目
        let free_space = match rec_len.checked_sub(actual_len) {
            Some(space) => space,
            None => {
                // actual_len > rec_len，这个条目可能损坏，跳过
                offset += rec_len as usize;
                continue;
            }
        };

        // 检查是否有足够的空闲空间
        if free_space >= required_len {
            log::trace!(
                "[find_and_insert_entry] FOUND SPACE: offset={offset}, rec_len={rec_len}, actual_len={actual_len}, free_space={free_space}, required_len={required_len}, entry_inode={entry_inode}, entries_checked={entries_checked}"
            );
            // 找到合适的位置
            if entry_inode != 0 && actual_len > 0 {
                // 分裂现有条目
                split_entry_and_insert(
                    data,
                    offset,
                    actual_len,
                    name,
                    child_inode,
                    file_type,
                    required_len,
                );
            } else {
                // 直接使用空闲条目
                write_entry(
                    data,
                    offset,
                    name,
                    child_inode,
                    file_type,
                    rec_len,
                );
            }
            return true;
        }

        offset += rec_len as usize;
    }

    log::trace!(
        "[find_and_insert_entry] NO SPACE: name='{name}', entries_checked={entries_checked}, final_offset={offset}"
    );
    false
}

/// 分裂现有条目并插入新条目
fn split_entry_and_insert(
    data: &mut [u8],
    offset: usize,
    actual_len: u16,
    name: &str,
    child_inode: u32,
    file_type: u8,
    _required_len: u16,
) {
    // 获取原条目的 rec_len
    let old_entry = unsafe {
        &mut *(data[offset..].as_mut_ptr() as *mut ext4_dir_entry)
    };
    let total_len = u16::from_le(old_entry.rec_len);

    // 更新原条目的 rec_len 为实际长度
    old_entry.rec_len = actual_len.to_le();

    // 在原条目后面写入新条目
    let new_offset = offset + actual_len as usize;
    let new_rec_len = total_len - actual_len;

    write_entry(
        data,
        new_offset,
        name,
        child_inode,
        file_type,
        new_rec_len,
    );
}

/// 写入目录项
fn write_entry(
    data: &mut [u8],
    offset: usize,
    name: &str,
    inode: u32,
    file_type: u8,
    rec_len: u16,
) {
    let entry = unsafe {
        &mut *(data[offset..].as_mut_ptr() as *mut ext4_dir_entry)
    };

    entry.inode = inode.to_le();
    entry.rec_len = rec_len.to_le();
    entry.name_len = name.len() as u8;
    entry.file_type = file_type;

    // 写入名称
    let name_bytes = name.as_bytes();
    let name_offset = offset + core::mem::size_of::<ext4_dir_entry>();
    data[name_offset..name_offset + name_bytes.len()].copy_from_slice(name_bytes);
}

/// 分配新的目录块并添加条目
///
/// ⚠️ **重要**: 此函数需要可变的 Superblock 引用用于块分配
///
/// # 参数
///
/// * `inode_ref` - 目录 inode 引用
/// * `sb` - 可变 superblock 引用（用于块分配）
/// * `name` - 条目名称
/// * `child_inode` - 子 inode 编号
/// * `file_type` - 文件类型
/// * `required_len` - 所需空间长度
///
/// # 实现步骤
///
/// 1. 计算下一个逻辑块号
/// 2. 分配新的物理块
/// 3. 初始化新块为空目录块
/// 4. 在新块中插入条目
/// 5. 更新 inode size
///
/// # 注意
///
/// ⚠️ **TODO**: 当前实现简化版本，不支持 extent 树扩展
/// 完整的实现需要支持 extent 树的插入和分裂操作
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
    // 这会自动处理：
    // 1. 分配物理块
    // 2. 更新 extent tree（添加新的 extent 或扩展现在 extent）
    // 3. 更新 inode 的 blocks 计数
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
/// # 参数
///
/// * `dir_inode_ref` - 目录自身的 inode 引用
/// * `parent_inode` - 父目录的 inode 编号
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
/// issue: 默认block1已分配， 需要检查是否需要优化当前函数以移除默认条件， 或者将该逻辑分发到其他函数， 但要能够确保目录至少有一个块已分配
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
/// # 参数
///
/// * `dir_inode_ref` - 目录自身的 inode 引用
/// * `parent_inode` - 父目录的 inode 编号
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
/// ⚠️ **简化实现**：不自动分配第一个叶子块（块 1）
/// 叶子块应由调用者在创建目录后立即分配
/// issue: 1.初始化逻辑不完整 2.这个函数还没有被实际应用到mkdir的逻辑中， 也就是根本还没有被调用过 3.简化实现， 默认block1已经分配， 亟待后续优化
pub fn dx_init<D: BlockDevice>(
    dir_inode_ref: &mut InodeRef<D>,
    parent_inode: u32,
) -> Result<()> {

    let block_size = dir_inode_ref.sb().block_size();
    let has_csum = dir_inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);

    // 获取或分配第一个块（根块）（新目录需要创建块）
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
        // . 条目：12 字节
        write_entry(data, 0, ".", dir_inode, EXT4_DE_DIR, 12);

        // .. 条目：占据到索引信息之前的空间
        let dotdot_len = block_size - 12;
        write_entry(data, 12, "..", parent_inode, EXT4_DE_DIR, dotdot_len as u16);

        // 2. 初始化 HTree 根信息
        // 根信息位于 . 和 .. 之后
        // 每个 dot entry 是 12 字节（见 lwext4）
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
        let entries_offset = root_info_offset + 8; // info_length = 8

        // 计算可用空间
        let entry_space = if has_csum {
            block_size as usize - entries_offset - core::mem::size_of::<ext4_dir_entry_tail>()
        } else {
            block_size as usize - entries_offset
        };

        // 每个索引条目 8 字节（hash(4) + block(4)）
        let max_entries = (entry_space / 8) as u16;

        // count_limit 结构（4 字节）：limit(2) + count(2)
        let limit_offset = entries_offset;
        // limit
        data[limit_offset..limit_offset + 2].copy_from_slice(&max_entries.to_le_bytes());
        // count = 1 (将有一个指向块 1 的索引条目，需要调用者后续添加)
        data[limit_offset + 2..limit_offset + 4].copy_from_slice(&1_u16.to_le_bytes());

        // 4. 添加第一个索引条目（hash=0, block=1）
        // 注意：这假设块 1 会被分配，但我们不在这里分配它
        let first_entry_offset = entries_offset + 4; // 跳过 count_limit
        // hash (4 bytes) = 0
        data[first_entry_offset..first_entry_offset + 4].copy_from_slice(&0_u32.to_le_bytes());
        // block (4 bytes) = 1
        data[first_entry_offset + 4..first_entry_offset + 8].copy_from_slice(&1_u32.to_le_bytes());

        // 5. 如果需要校验和，初始化尾部
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

/// 计算目录项所需长度（8字节对齐）
fn calculate_entry_len(name_len: u8) -> u16 {
    let base_len = core::mem::size_of::<ext4_dir_entry>() + name_len as usize;
    // 4字节对齐（与 lwext4 C 实现一致）
    ((base_len + 3) & !3) as u16
}

/// 更新目录块校验和（不需要 InodeRef 的版本）
///
/// 这个版本接受提前提取的标量数据，避免与 bdev() 的可变借用冲突
pub(super) fn update_dir_block_checksum(
    has_csum: bool,
    uuid: &[u8; 16],
    inode_index: u32,
    inode_generation: u32,
    data: &mut [u8],
    block_size: usize,
) {
    if !has_csum {
        return;
    }

    // 手动计算校验和（不使用 InodeRef）
    #[cfg(feature = "metadata-csum")]
    {
        const EXT4_CRC32_INIT: u32 = 0xFFFFFFFF;

        // 计算尾部偏移量
        let tail_offset = block_size - core::mem::size_of::<ext4_dir_entry_tail>();

        // 1. 计算 UUID 的校验和
        let mut csum = crate::crc::crc32c_append(EXT4_CRC32_INIT, uuid);

        // 2. 计算 inode 号的校验和
        let ino_index = inode_index.to_le_bytes();
        csum = crate::crc::crc32c_append(csum, &ino_index);

        // 3. 计算 inode generation 的校验和
        let ino_gen = inode_generation.to_le_bytes();
        csum = crate::crc::crc32c_append(csum, &ino_gen);

        // 4. 计算目录项数据的校验和（不包含尾部）
        let dirent_data = &data[..tail_offset];
        csum = crate::crc::crc32c_append(csum, dirent_data);

        // 5. 设置校验和到尾部
        if let Some(tail) = checksum::get_tail_mut(data, block_size) {
            tail.set_checksum(csum);
        }
    }

    #[cfg(not(feature = "metadata-csum"))]
    {
        // 无操作
        let _ = (uuid, inode_index, inode_generation, data, block_size);
    }
}

/// 删除目录条目
///
/// 对应 lwext4 的 `ext4_dir_remove_entry()`
///
/// # 参数
///
/// * `inode_ref` - 目录 inode 引用
/// * `name` - 要删除的条目名称
///
/// # 返回
///
/// 成功返回 Ok(())，条目不存在返回 NotFound 错误
/// issue: 这里直接采用遍历所有逻辑块，然后从逻辑块中查找匹配目录项， 有待优化, 应该向lwext4的是实现， 使用上hashinfo
pub fn remove_entry<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
) -> Result<()> {
    // 遍历目录块查找条目
    let mut block_idx = 0_u32;
    loop {
        let block_addr = match inode_ref.get_inode_dblk_idx(block_idx, false) {
            Ok(addr) => addr,
            Err(_) => {
                // 遍历完所有块，没找到
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "Directory entry not found",
                ));
            }
        };

        // 在获取 bdev 之前提取所有需要的数据
        let has_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
        let block_size = inode_ref.sb().block_size() as usize;
        let uuid = inode_ref.sb().inner().uuid;
        let inode_index = inode_ref.index();
        let inode_generation = inode_ref.generation()?;

        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, block_addr)?;

        let found = block.with_data_mut(|data| {
            let result = remove_entry_from_block(data, name);

            if result {
                // 删除成功，更新校验和
                update_dir_block_checksum(
                    has_csum,
                    &uuid,
                    inode_index,
                    inode_generation,
                    data,
                    block_size,
                );
            }

            result
        })?;

        drop(block);

        if found {
            return Ok(());
        }

        block_idx += 1;
    }
}

/// 从块中删除条目
///
/// # 返回
///
/// 找到并删除返回 true，未找到返回 false
fn remove_entry_from_block(data: &mut [u8], name: &str) -> bool {
    let mut prev_offset: Option<usize> = None;
    let mut offset = 0;

    while offset < data.len() {
        if offset + core::mem::size_of::<ext4_dir_entry>() > data.len() {
            break;
        }

        let entry = unsafe {
            &*(data[offset..].as_ptr() as *const ext4_dir_entry)
        };

        let rec_len = u16::from_le(entry.rec_len);
        if rec_len == 0 {
            break;
        }

        let entry_inode = u32::from_le(entry.inode);

        if entry_inode != 0 {
            // 读取名称
            let name_offset = offset + core::mem::size_of::<ext4_dir_entry>();
            let entry_name_len = entry.name_len as usize;

            if name_offset + entry_name_len <= data.len() {
                let entry_name = &data[name_offset..name_offset + entry_name_len];

                if entry_name == name.as_bytes() {
                    // 找到了，删除它
                    if let Some(prev_off) = prev_offset {
                        // 合并到前一个条目
                        let prev_entry = unsafe {
                            &mut *(data[prev_off..].as_mut_ptr() as *mut ext4_dir_entry)
                        };
                        let prev_rec_len = u16::from_le(prev_entry.rec_len);
                        prev_entry.rec_len = (prev_rec_len + rec_len).to_le();
                    } else {
                        // 这是第一个条目，标记为删除（inode = 0）
                        let entry_mut = unsafe {
                            &mut *(data[offset..].as_mut_ptr() as *mut ext4_dir_entry)
                        };
                        entry_mut.inode = 0_u32.to_le();
                    }

                    return true;
                }
            }
        }

        prev_offset = Some(offset);
        offset += rec_len as usize;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_entry_len() {
        // 基础大小：8 字节（ext4_dir_entry: u32+u16+u8+u8），4字节对齐
        // 与 lwext4 C 实现一致: required_len = sizeof(ext4_fake_dir_entry) + name_len, 4字节对齐
        // 名称 "a" (1) -> 8 + 1 = 9 -> 对齐到 12
        assert_eq!(calculate_entry_len(1), 12);

        // 名称 "ab" (2) -> 8 + 2 = 10 -> 对齐到 12
        assert_eq!(calculate_entry_len(2), 12);

        // 名称 "abcd" (4) -> 8 + 4 = 12 -> 已对齐 12
        assert_eq!(calculate_entry_len(4), 12);

        // 名称 "abcde" (5) -> 8 + 5 = 13 -> 对齐到 16
        assert_eq!(calculate_entry_len(5), 16);

        // 名称 "abcdefgh" (8) -> 8 + 8 = 16 -> 已对齐 16
        assert_eq!(calculate_entry_len(8), 16);
    }

    #[test]
    fn test_dir_entry_constants() {
        assert_eq!(EXT4_DE_REG_FILE, 1);
        assert_eq!(EXT4_DE_DIR, 2);
        assert_eq!(EXT4_DE_SYMLINK, 7);
    }
}
