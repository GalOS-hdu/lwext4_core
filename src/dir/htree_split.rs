//! HTree 分裂与插入操作
//!
//! 实现 ext4 HTree 目录索引的叶子块分裂和索引块分裂。
//!
//! 对应 lwext4 的 `ext4_dir_idx.c` 中 split 相关函数。

use crate::{
    balloc::BlockAllocator,
    block::{Block, BlockDevice},
    bytes::{read_struct, read_struct_at, write_struct_at},
    consts::*,
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_dir_en, ext4_dir_entry_tail, ext4_dir_idx_climit, ext4_dir_idx_entry, ext4_fake_dir_entry},
};
use alloc::vec::Vec;

use super::checksum::init_entry_tail;
use super::htree::{root_entries_offset, non_root_entries_offset, HTreeHashInfo, HTreePath};

/// Directory entry with hash for sorting
///
/// 对应 lwext4 的 `struct ext4_dx_sort_entry`
#[derive(Clone)]
struct DirEntrySortEntry {
    /// Hash value of the entry name
    hash: u32,
    /// Entry inode number
    inode: u32,
    /// Entry name length
    name_len: u8,
    /// Entry file type
    file_type: u8,
    /// Entry name (max 255 bytes)
    name: [u8; 255],
}

impl DirEntrySortEntry {
    /// Calculate the aligned record length for this entry
    fn record_len(&self) -> u16 {
        let len = 8 + self.name_len as u16;
        // Align to 4 bytes
        if len % 4 != 0 {
            len + (4 - len % 4)
        } else {
            len
        }
    }
}

/// Index block split result
pub struct IndexSplitResult {
    /// New index block logical number
    pub new_logical_block: u32,
    /// Split hash value
    pub split_hash: u32,
    /// Whether this is a root split (tree grew taller)
    pub is_root_split: bool,
}

/// Split a full HTree leaf block into two blocks
///
/// 对应 lwext4 的 `ext4_dir_dx_split_data()`
///
/// # 算法流程
///
/// 1. 读取旧块中所有目录项
/// 2. 计算每个目录项的哈希值
/// 3. 按哈希值排序
/// 4. 找到 50% 容量的分割点
/// 5. 确保相同哈希的条目不被分开
/// 6. 分配新块
/// 7. 将前半部分写回旧块，后半部分写入新块
/// 8. 返回新块的逻辑块号和分割哈希值
pub fn split_leaf_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    old_block_addr: u64,
    hash_info: &HTreeHashInfo,
) -> Result<(u32, u32)> {
    use super::hash::htree_hash;

    let block_size = sb.block_size() as usize;
    let has_csum = sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);

    // 1. 读取旧块中所有目录项
    let mut entries = Vec::new();

    {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, old_block_addr)?;

        block.with_data(|data| {
            let mut offset = 0;

            while offset < block_size {
                if offset + 8 > block_size {
                    break;
                }

                let de: ext4_dir_en = read_struct_at(data, offset)?;
                let rec_len = u16::from_le(de.rec_len) as usize;

                if rec_len < 8 || offset + rec_len > block_size {
                    break;
                }

                let inode = u32::from_le(de.inode);
                if inode != 0 && de.name_len > 0 {
                    // 计算哈希值
                    let name_len = de.name_len as usize;
                    let name_slice = &data[offset + 8..offset + 8 + name_len];

                    let (hash, _minor_hash) = htree_hash(
                        name_slice,
                        hash_info.seed.as_ref(),
                        hash_info.hash_version
                    )?;

                    let mut entry = DirEntrySortEntry {
                        hash,
                        inode,
                        name_len: de.name_len,
                        file_type: de.file_type,
                        name: [0; 255],
                    };
                    entry.name[..name_len].copy_from_slice(name_slice);

                    entries.push(entry);
                }

                offset += rec_len;
            }

            Ok::<(), Error>(())
        })??;
    }

    if entries.is_empty() {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "No valid entries in leaf block"
        ));
    }

    // 2. 按哈希值排序
    entries.sort_by_key(|e| e.hash);

    // 3. 找到分割点（按 50% 容量）
    let tail_size = if has_csum {
        core::mem::size_of::<ext4_dir_entry_tail>()
    } else {
        0
    };
    let usable_size = block_size - tail_size;
    let target_size = usable_size / 2;

    let mut current_size = 0_usize;
    let mut split_idx = 0_usize;
    let mut split_hash = 0_u32;

    for (i, entry) in entries.iter().enumerate() {
        let rec_len = entry.record_len() as usize;
        if current_size + rec_len > target_size {
            split_idx = i;
            split_hash = entry.hash;
            break;
        }
        current_size += rec_len;
    }

    if split_idx == 0 {
        split_idx = entries.len() / 2;
        if split_idx == 0 {
            split_idx = 1;
        }
        split_hash = entries[split_idx].hash;
    }

    // 4. 确保相同哈希的条目不被分开
    let mut continued = false;
    if split_idx > 0 && split_hash == entries[split_idx - 1].hash {
        // 需要跳过所有相同哈希的条目
        while split_idx < entries.len() && entries[split_idx].hash == split_hash {
            split_idx += 1;
        }
        if split_idx < entries.len() {
            split_hash = entries[split_idx].hash;
        }
        continued = true;
    }

    // 5. 分配新块
    let mut allocator = BlockAllocator::new();
    let goal = old_block_addr;

    let new_block_addr = {
        let bdev = inode_ref.bdev();
        allocator.alloc_block(bdev, sb, goal)?
    };
    inode_ref.add_blocks(1)?;

    // 计算新块的逻辑块号
    let current_size = inode_ref.size()?;
    let new_logical_block = (current_size / block_size as u64) as u32;

    // 6. 写入两个块
    write_sorted_entries(
        inode_ref,
        old_block_addr,
        &entries[..split_idx],
        block_size,
        has_csum
    )?;

    write_sorted_entries(
        inode_ref,
        new_block_addr,
        &entries[split_idx..],
        block_size,
        has_csum
    )?;

    // 7. 更新 inode size
    let new_size = (new_logical_block as u64 + 1) * block_size as u64;
    inode_ref.set_size(new_size)?;

    // 8. 返回分割哈希值（如果continued，则+1）
    let final_split_hash = if continued {
        split_hash.wrapping_add(1)
    } else {
        split_hash
    };

    Ok((new_logical_block, final_split_hash))
}

/// Write sorted directory entries to a block
fn write_sorted_entries<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    block_addr: u64,
    entries: &[DirEntrySortEntry],
    block_size: usize,
    has_csum: bool,
) -> Result<()> {
    use crate::types::ext4_dir_en;
    use super::write::update_dir_block_checksum;

    let tail_size = if has_csum {
        core::mem::size_of::<ext4_dir_entry_tail>()
    } else {
        0
    };
    let usable_size = block_size - tail_size;

    let uuid = inode_ref.sb().inner().uuid;
    let dir_inode = inode_ref.index();
    let inode_generation = inode_ref.generation()?;

    let bdev = inode_ref.bdev();
    let mut block = Block::get_noread(bdev, block_addr)?;

    block.with_data_mut(|data| {
        data.fill(0);

        let mut offset = 0_usize;
        for (i, entry) in entries.iter().enumerate() {
            if offset >= usable_size {
                break;
            }

            let rec_len = if i == entries.len() - 1 {
                // 最后一个条目占据剩余空间
                (usable_size - offset) as u16
            } else {
                entry.record_len()
            };

            if offset + rec_len as usize > usable_size {
                break;
            }

            // 写入目录项
            let de = ext4_dir_en {
                inode: entry.inode.to_le(),
                rec_len: rec_len.to_le(),
                name_len: entry.name_len,
                file_type: entry.file_type,
            };
            write_struct_at(data, offset, &de)?;

            let name_len = entry.name_len as usize;
            data[offset + 8..offset + 8 + name_len].copy_from_slice(&entry.name[..name_len]);

            offset += rec_len as usize;
        }

        // 初始化 tail 和校验和
        if has_csum {
            let tail_offset = block_size - core::mem::size_of::<ext4_dir_entry_tail>();
            let mut tail: ext4_dir_entry_tail = read_struct_at(data, tail_offset)?;
            init_entry_tail(&mut tail);
            write_struct_at(data, tail_offset, &tail)?;

            update_dir_block_checksum(
                has_csum,
                &uuid,
                dir_inode,
                inode_generation,
                data,
                block_size,
            );
        }

        Ok::<(), Error>(())
    })??;

    Ok(())
}

/// Insert an index entry into an index block
///
/// 对应 lwext4 的 `ext4_dir_dx_insert_entry()`
#[allow(dead_code)] // 待 htree 写入功能完成后使用
pub(super) fn insert_index_entry<D: BlockDevice>(
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
        // 确定 entries 起始位置
        let is_root = {
            let fake_entry: ext4_fake_dir_entry = read_struct(data)?;
            // Root block 有 dot entries
            u16::from_le(fake_entry.entry_len) != block_size as u16
        };

        let entries_offset = if is_root {
            root_entries_offset()
        } else {
            non_root_entries_offset()
        };

        // 读取 climit
        let mut climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;
        let count = u16::from_le(climit.count);

        // 计算插入位置
        let entry_size = core::mem::size_of::<ext4_dir_idx_entry>();
        let climit_size = core::mem::size_of::<ext4_dir_idx_climit>();
        let insert_offset = entries_offset + climit_size + entry_size * insert_position;

        // 移动后续条目腾出空间
        let bytes_to_move = entry_size * (count as usize - insert_position);
        if bytes_to_move > 0 {
            data.copy_within(insert_offset..insert_offset + bytes_to_move, insert_offset + entry_size);
        }

        // 写入新条目
        let new_entry = ext4_dir_idx_entry {
            hash: hash.to_le(),
            block: logical_block.to_le(),
        };
        write_struct_at(data, insert_offset, &new_entry)?;

        // 更新 count
        climit.count = (count + 1).to_le();
        write_struct_at(data, entries_offset, &climit)?;

        // 更新校验和（如果需要）
        if has_csum {
            update_index_block_checksum(has_csum, data, block_size);
        }

        Ok::<(), Error>(())
    })??;

    Ok(())
}

/// Update index block checksum
fn update_index_block_checksum(
    _has_csum: bool,
    _data: &mut [u8],
    _block_size: usize,
) {
    // TODO: 实现索引块校验和
    // 类似于 dir block checksum，但使用 ext4_dir_idx_tail
}

/// Split a full HTree index block
///
/// 对应 lwext4 的 `ext4_dir_dx_split_index()`
///
/// **Case A - 非 root 分裂**：分割索引条目，在父级插入新条目
/// **Case B - root 分裂**：所有条目移到新 child 块，root 指向新 child
pub fn split_index_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    index_block_addr: u64,
    is_root: bool,
    position_in_entries: usize,
) -> Result<IndexSplitResult> {
    let block_size = sb.block_size() as usize;
    let has_csum = sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);

    // 1. 读取当前块的 count 和 limit
    let (count, limit) = {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, index_block_addr)?;

        block.with_data(|data| {
            let entries_offset = if is_root {
                root_entries_offset()
            } else {
                non_root_entries_offset()
            };

            let climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;

            Ok::<_, Error>((u16::from_le(climit.count), u16::from_le(climit.limit)))
        })?
    }?;

    // 2. 检查是否需要分裂
    if count < limit {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Index block not full, no split needed"
        ));
    }

    // 3. 分配新索引块
    let mut allocator = BlockAllocator::new();
    let goal = index_block_addr;

    let new_block_addr = {
        let bdev = inode_ref.bdev();
        allocator.alloc_block(bdev, sb, goal)?
    };
    inode_ref.add_blocks(1)?;

    // 计算新块的逻辑块号
    let current_size = inode_ref.size()?;
    let new_logical_block = (current_size / block_size as u64) as u32;

    // 4. 执行分裂
    if !is_root {
        split_non_root_index(
            inode_ref,
            index_block_addr,
            new_block_addr,
            count,
            position_in_entries,
            block_size,
            has_csum
        )?
    } else {
        split_root_index(
            inode_ref,
            index_block_addr,
            new_block_addr,
            new_logical_block,
            count,
            block_size,
            has_csum
        )?
    }

    // 5. 更新 inode size
    let new_size = (new_logical_block as u64 + 1) * block_size as u64;
    inode_ref.set_size(new_size)?;

    // 6. 返回分割信息
    let split_hash = read_first_entry_hash(inode_ref, new_block_addr, false)?;

    Ok(IndexSplitResult {
        new_logical_block,
        split_hash,
        is_root_split: is_root,
    })
}

/// Split a non-root index block
fn split_non_root_index<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    old_block_addr: u64,
    new_block_addr: u64,
    count: u16,
    _position_in_entries: usize,
    block_size: usize,
    has_csum: bool,
) -> Result<()> {
    let count_left = count / 2;
    let count_right = count - count_left;

    let entry_size = core::mem::size_of::<ext4_dir_idx_entry>();
    let entries_offset = non_root_entries_offset();

    // 读取右半部分条目
    let right_entries = {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, old_block_addr)?;

        block.with_data(|data| {
            let start = entries_offset + entry_size * count_left as usize;
            let len = entry_size * count_right as usize;
            let mut entries = Vec::with_capacity(len);
            entries.extend_from_slice(&data[start..start + len]);
            entries
        })?
    };

    // 初始化新块
    {
        let bdev = inode_ref.bdev();
        let mut block = Block::get_noread(bdev, new_block_addr)?;

        block.with_data_mut(|data| {
            data.fill(0);

            // 初始化 fake entry
            let fake = ext4_fake_dir_entry {
                inode: 0,
                entry_len: (block_size as u16).to_le(),
                name_len: 0,
                inode_type: 0,
            };
            write_struct_at(data, 0, &fake)?;

            // 写入 climit
            let tail_size = if has_csum {
                core::mem::size_of::<crate::types::ext4_dir_idx_tail>()
            } else {
                0
            };
            let entry_space = block_size - entries_offset - tail_size;
            let max_entries = (entry_space / entry_size) as u16;

            let climit = ext4_dir_idx_climit {
                limit: max_entries.to_le(),
                count: count_right.to_le(),
            };
            write_struct_at(data, entries_offset, &climit)?;

            // 写入条目
            data[entries_offset + core::mem::size_of::<ext4_dir_idx_climit>()..]
                [..right_entries.len()]
                .copy_from_slice(&right_entries);

            // 更新校验和
            if has_csum {
                update_index_block_checksum(has_csum, data, block_size);
            }

            Ok::<(), Error>(())
        })??;
    }

    // 更新旧块的 count
    {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, old_block_addr)?;

        block.with_data_mut(|data| {
            let mut climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;
            climit.count = count_left.to_le();
            write_struct_at(data, entries_offset, &climit)?;

            if has_csum {
                update_index_block_checksum(has_csum, data, block_size);
            }

            Ok::<(), Error>(())
        })??;
    }

    Ok(())
}

/// Split root index block (grow tree height)
fn split_root_index<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    root_block_addr: u64,
    new_child_addr: u64,
    new_child_logical: u32,
    count: u16,
    block_size: usize,
    has_csum: bool,
) -> Result<()> {
    let entry_size = core::mem::size_of::<ext4_dir_idx_entry>();
    let r_entries_offset = root_entries_offset();
    let child_entries_offset = non_root_entries_offset();

    // 读取所有条目
    let all_entries = {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, root_block_addr)?;

        block.with_data(|data| {
            let start = r_entries_offset + core::mem::size_of::<ext4_dir_idx_climit>();
            let len = entry_size * count as usize;
            let mut entries = Vec::with_capacity(len);
            entries.extend_from_slice(&data[start..start + len]);
            entries
        })?
    };

    // 初始化新 child 块
    {
        let bdev = inode_ref.bdev();
        let mut block = Block::get_noread(bdev, new_child_addr)?;

        block.with_data_mut(|data| {
            data.fill(0);

            // 初始化 fake entry
            let fake = ext4_fake_dir_entry {
                inode: 0,
                entry_len: (block_size as u16).to_le(),
                name_len: 0,
                inode_type: 0,
            };
            write_struct_at(data, 0, &fake)?;

            // 写入 climit
            let tail_size = if has_csum {
                core::mem::size_of::<crate::types::ext4_dir_idx_tail>()
            } else {
                0
            };
            let entry_space = block_size - child_entries_offset - tail_size;
            let max_entries = (entry_space / entry_size) as u16;

            let climit = ext4_dir_idx_climit {
                limit: max_entries.to_le(),
                count: count.to_le(),
            };
            write_struct_at(data, child_entries_offset, &climit)?;

            // 写入所有条目
            data[child_entries_offset + core::mem::size_of::<ext4_dir_idx_climit>()..]
                [..all_entries.len()]
                .copy_from_slice(&all_entries);

            // 更新校验和
            if has_csum {
                update_index_block_checksum(has_csum, data, block_size);
            }

            Ok::<(), Error>(())
        })??;
    }

    // 更新 root 块
    {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, root_block_addr)?;

        block.with_data_mut(|data| {
            // 更新 root info: indirect_levels = 1
            let root_info_offset = 2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>();
            let mut root_info: crate::types::ext4_dir_idx_rinfo = read_struct_at(data, root_info_offset)?;
            root_info.indirect_levels = 1;
            write_struct_at(data, root_info_offset, &root_info)?;

            // 更新 climit: count = 1
            let mut climit: ext4_dir_idx_climit = read_struct_at(data, r_entries_offset)?;
            climit.count = 1_u16.to_le();
            write_struct_at(data, r_entries_offset, &climit)?;

            // 写入唯一的条目，指向新 child
            let entry = ext4_dir_idx_entry {
                hash: 0_u32.to_le(),
                block: new_child_logical.to_le(),
            };
            let entry_offset = r_entries_offset + core::mem::size_of::<ext4_dir_idx_climit>();
            write_struct_at(data, entry_offset, &entry)?;

            // 更新校验和
            if has_csum {
                update_index_block_checksum(has_csum, data, block_size);
            }

            Ok::<(), Error>(())
        })??;
    }

    Ok(())
}

/// Read the hash of the first entry in an index block
fn read_first_entry_hash<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    block_addr: u64,
    is_root: bool,
) -> Result<u32> {
    let bdev = inode_ref.bdev();
    let mut block = Block::get(bdev, block_addr)?;

    block.with_data(|data| {
        let entries_offset = if is_root {
            root_entries_offset()
        } else {
            non_root_entries_offset()
        };

        let first_entry_offset = entries_offset + core::mem::size_of::<ext4_dir_idx_climit>();
        let entry: ext4_dir_idx_entry = read_struct_at(data, first_entry_offset)?;

        Ok(u32::from_le(entry.hash))
    })?
}

/// Handle leaf block split and retry insertion
///
/// Called when the target leaf block is full during htree add_entry
pub(super) fn handle_leaf_split<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    hash_info: &HTreeHashInfo,
    path: &HTreePath,
    old_block_addr: u64,
    name: &str,
    child_inode: u32,
    file_type: u8,
) -> Result<()> {
    use super::write::{update_dir_block_checksum, find_and_insert_entry, calculate_entry_len};

    // Split the leaf block
    let (new_logical_block, split_hash) = split_leaf_block(
        inode_ref,
        sb,
        old_block_addr,
        hash_info,
    )?;

    // Insert index entry pointing to the new block
    if let Some(parent_info) = path.index_blocks.last() {
        // Check if parent has space for the new entry
        if parent_info.entry_count >= parent_info.entry_limit {
            return Err(Error::new(
                ErrorKind::NoSpace,
                "Index block is full, recursive split not yet implemented",
            ));
        }

        // Insert the new index entry at position_idx + 1
        let insert_position = parent_info.position_idx + 1;

        insert_index_entry_at(
            inode_ref,
            parent_info.block_addr,
            insert_position,
            split_hash,
            new_logical_block,
        )?;
    } else {
        return Err(Error::new(
            ErrorKind::NoSpace,
            "Root split not yet implemented in add_entry",
        ));
    }

    // Retry the insertion: decide which block to use based on hash
    let target_block = if hash_info.hash >= split_hash {
        new_logical_block
    } else {
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
/// Similar to `insert_index_entry` but used by handle_leaf_split
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
        // 确定 entries 起始位置
        let is_root = {
            let fake_entry: ext4_fake_dir_entry = read_struct(data)?;
            u16::from_le(fake_entry.entry_len) != block_size as u16
        };

        let entries_offset = if is_root {
            root_entries_offset()
        } else {
            non_root_entries_offset()
        };

        // 读取 climit
        let mut climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;
        let count = u16::from_le(climit.count);

        // 计算插入位置
        let entry_size = core::mem::size_of::<ext4_dir_idx_entry>();
        let climit_size = core::mem::size_of::<ext4_dir_idx_climit>();
        let insert_offset = entries_offset + climit_size + entry_size * insert_position;

        // 移动后续条目腾出空间
        let bytes_to_move = entry_size * (count as usize - insert_position);
        if bytes_to_move > 0 {
            data.copy_within(insert_offset..insert_offset + bytes_to_move, insert_offset + entry_size);
        }

        // 写入新条目
        let new_entry = ext4_dir_idx_entry {
            hash: hash.to_le(),
            block: logical_block.to_le(),
        };
        write_struct_at(data, insert_offset, &new_entry)?;

        // 更新 count
        climit.count = (count + 1).to_le();
        write_struct_at(data, entries_offset, &climit)?;

        // 更新校验和
        if has_csum {
            update_index_block_checksum(has_csum, data, block_size);
        }

        Ok::<(), Error>(())
    })??;

    Ok(())
}
