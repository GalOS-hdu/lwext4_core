//! HTree directory indexing
//!
//! Implements ext4's HTree (hash tree) directory indexing for fast lookups
//! in large directories.
//!
//! # Implementation Status
//!
//! ✅ **Implemented**:
//! - Hash calculation (all versions)
//! - HTree structure parsing
//! - Binary search in index nodes
//! - Leaf node lookup (with and without path tracking)
//! - Leaf block splitting (`split_leaf_block`)
//! - Index block splitting (`split_index_block`)
//!
//! ⚠️ **Partially Implemented**:
//! - Directory entry search (read-only, depends on iterator)
//! - Entry addition (integrated in write module, with splitting support)
//!
//! ❌ **Not Implemented**:
//! - HTree initialization (`dx_init`) - in separate init module
//! - Recursive index splitting (needed for very deep trees)
//! - Parent inode reset
//!
//! # Dependency Status
//!
//! **Missing Dependencies**:
//! - ❌ Transaction support (required for modifications)
//! - ❌ Block allocation for HTree (required for tree expansion)
//! - ❌ Directory entry write operations
//!
//! **Incomplete Dependencies**:
//! - ⚠️ InodeRef write operations (basic support exists, but not fully tested)
//!
//! 对应 lwext4 的 ext4_dir_idx.c

use crate::{
    block::{Block, BlockDevice},
    consts::*,
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_dir_idx_climit, ext4_dir_idx_entry, ext4_dir_idx_root},
};
use alloc::vec::Vec;

use super::hash::{htree_hash, EXT2_HTREE_HALF_MD4, EXT2_HTREE_LEGACY, EXT2_HTREE_TEA};

/// HTree index block structure
///
/// 对应 lwext4 的 `struct ext4_dir_idx_block`
pub struct IndexBlock {
    /// Block address
    pub block_addr: u64,
    /// Current position in entries
    pub position_idx: usize,
    /// Entry count
    pub entry_count: u16,
}

/// Hash information for HTree operations
///
/// 对应 lwext4 的 `struct ext4_hash_info`
#[derive(Debug, Clone)]
pub struct HTreeHashInfo {
    pub hash: u32,
    pub minor_hash: u32,
    pub hash_version: u8,
    pub seed: Option<[u32; 4]>,
}

/// HTree lookup result
pub struct HTreeLookupResult {
    /// Leaf block containing the entry
    pub leaf_block: u32,
    /// Hash value used for lookup
    pub hash: u32,
}

/// HTree path from root to leaf
///
/// 对应 lwext4 的 `struct ext4_dir_idx_block dx_blks[2]`
pub struct HTreePath {
    /// Index blocks in the path (max depth is 2 in ext4)
    pub index_blocks: Vec<IndexBlockInfo>,
    /// Leaf block logical number
    pub leaf_block: u32,
}

/// Information about an index block in the path
#[derive(Clone)]
pub struct IndexBlockInfo {
    /// Logical block number
    pub logical_block: u32,
    /// Physical block address
    pub block_addr: u64,
    /// Position in entries where the search went
    pub position_idx: usize,
    /// Total entry count
    pub entry_count: u16,
    /// Entry limit (capacity)
    pub entry_limit: u16,
}

/// Initialize hash info from root block
///
/// 对应 lwext4 的 `ext4_dir_hinfo_init()`
///
/// # Parameters
///
/// * `inode_ref` - Directory inode reference
/// * `name` - Name to compute hash for
///
/// # Returns
///
/// Hash information structure
pub fn init_hash_info<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
) -> Result<HTreeHashInfo> {
    // Extract data from inode_ref BEFORE getting block
    // (to avoid borrowing conflicts)
    let block_size = inode_ref.sb().block_size();
    let has_unsigned_hash = inode_ref.sb().has_flag(EXT4_SUPERBLOCK_FLAGS_UNSIGNED_HASH);
    let has_metadata_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
    let seed = inode_ref.sb().hash_seed();

    // Calculate entry space (needed for validation)
    let mut entry_space = block_size;
    entry_space -= 2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>() as u32;
    entry_space -= core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>() as u32;
    if has_metadata_csum {
        entry_space -= core::mem::size_of::<crate::types::ext4_dir_idx_tail>() as u32;
    }
    let entry_space = entry_space / core::mem::size_of::<ext4_dir_idx_entry>() as u32;

    // Now read root block (block 0)
    let root_block_addr = inode_ref.get_inode_dblk_idx(0, false)?;
    let bdev = inode_ref.bdev();
    let mut root_block = Block::get(bdev, root_block_addr)?;

    root_block.with_data(|data| {
        // Parse root structure
        let root = unsafe { &*(data.as_ptr() as *const ext4_dir_idx_root) };

        // Validate hash version
        let hash_version = root.info.hash_version();
        if hash_version != EXT2_HTREE_LEGACY
            && hash_version != EXT2_HTREE_HALF_MD4
            && hash_version != EXT2_HTREE_TEA
        {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Invalid HTree hash version",
            ));
        }

        // Check unused flags
        if root.info.unused_flags != 0 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "HTree unused flags must be zero",
            ));
        }

        // Check indirect levels (should be 0 or 1)
        if root.info.indirect_levels() > 1 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "HTree indirect levels > 1 not supported",
            ));
        }

        // Validate count/limit
        let climit = unsafe { &*((&root.en as *const _ as *const u8) as *const ext4_dir_idx_climit) };
        let limit = climit.limit();

        if limit != entry_space as u16 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "HTree root limit mismatch",
            ));
        }

        // Determine hash version (check unsigned flag from superblock)
        let mut hash_version = hash_version;
        if hash_version <= EXT2_HTREE_TEA {
            // Check if superblock requires unsigned hash
            if has_unsigned_hash {
                hash_version += 3; // Convert to unsigned version
            }
        }

        // Compute hash
        let (hash, minor_hash) = htree_hash(name.as_bytes(), Some(&seed), hash_version)?;

        Ok(HTreeHashInfo {
            hash,
            minor_hash,
            hash_version,
            seed: Some(seed),
        })
    })?
}

/// Calculate available entry space in index node
fn calculate_entry_space(block_size: u32, sb: &Superblock) -> u32 {
    let mut entry_space = block_size;
    entry_space -= 2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>() as u32;
    entry_space -= core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>() as u32;

    if sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM) {
        entry_space -= core::mem::size_of::<crate::types::ext4_dir_idx_tail>() as u32;
    }

    entry_space / core::mem::size_of::<ext4_dir_idx_entry>() as u32
}

/// Walk through index tree to find leaf block
///
/// 对应 lwext4 的 `ext4_dir_dx_get_leaf()`
///
/// Uses binary search to navigate the HTree index
///
/// # Parameters
///
/// * `inode_ref` - Directory inode reference
/// * `hash_info` - Hash information for lookup
///
/// # Returns
///
/// Logical block number of the leaf block containing entries with this hash
pub fn get_leaf_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    hash_info: &HTreeHashInfo,
) -> Result<u32> {
    // Start from root block (block 0)
    let mut current_block_idx = 0_u32;
    let _block_size = inode_ref.sb().block_size();

    // Read root to get indirect levels
    let root_block_addr = inode_ref.get_inode_dblk_idx(current_block_idx, false)?;
    let indirect_levels = {
        let bdev = inode_ref.bdev();
        let mut root_block = Block::get(bdev, root_block_addr)?;
        root_block.with_data(|data| {
            let root = unsafe { &*(data.as_ptr() as *const ext4_dir_idx_root) };
            root.info.indirect_levels()
        })?
    }; // root_block is dropped here, releasing the borrow

    let mut current_level = indirect_levels;

    // Walk through the index tree
    loop {
        let physical_block = inode_ref.get_inode_dblk_idx(current_block_idx, false)?;
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, physical_block)?;

        let next_block = block.with_data(|data| -> Result<u32> {
            // Parse entries
            let (entries, count, limit) = if current_block_idx == 0 {
                // Root block
                let root = unsafe { &*(data.as_ptr() as *const ext4_dir_idx_root) };
                let climit = unsafe {
                    &*((&root.en as *const _ as *const u8) as *const ext4_dir_idx_climit)
                };
                let count = climit.count();
                let limit = climit.limit();

                // Entries start after climit
                let entries_ptr = unsafe {
                    ((&root.en as *const _ as *const u8)
                        .add(core::mem::size_of::<ext4_dir_idx_climit>()))
                        as *const ext4_dir_idx_entry
                };
                let entries = unsafe {
                    core::slice::from_raw_parts(entries_ptr, count as usize)
                };

                (entries, count, limit)
            } else {
                // Non-root index node
                let _fake_entry = unsafe { &*(data.as_ptr() as *const crate::types::ext4_fake_dir_entry) };
                let climit = unsafe {
                    &*(data.as_ptr()
                        .add(core::mem::size_of::<crate::types::ext4_fake_dir_entry>())
                        as *const ext4_dir_idx_climit)
                };
                let count = climit.count();
                let limit = climit.limit();

                let entries_ptr = unsafe {
                    data.as_ptr()
                        .add(core::mem::size_of::<crate::types::ext4_fake_dir_entry>())
                        .add(core::mem::size_of::<ext4_dir_idx_climit>())
                        as *const ext4_dir_idx_entry
                };
                let entries = unsafe {
                    core::slice::from_raw_parts(entries_ptr, count as usize)
                };

                (entries, count, limit)
            };

            // Validate count
            if count == 0 || count > limit {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "HTree invalid entry count",
                ));
            }

            // Binary search for the entry
            // Skip first entry (it's the minimum hash, always matches)
            if count == 1 {
                return Ok(entries[0].block());
            }

            let mut left = 1_usize;
            let mut right = (count - 1) as usize;
            let mut result_idx = 0_usize;

            while left <= right {
                let mid = left + (right - left) / 2;
                let mid_hash = entries[mid].hash();

                if mid_hash > hash_info.hash {
                    if mid == 0 {
                        break;
                    }
                    right = mid - 1;
                } else {
                    result_idx = mid;
                    left = mid + 1;
                }
            }

            // Use the entry just before where we would insert
            Ok(entries[result_idx].block())
        })??;

        // Check if we're at a leaf
        if current_level == 0 {
            return Ok(next_block);
        }

        // Move to next level
        current_block_idx = next_block;
        current_level -= 1;
    }
}

/// Get leaf block with full path information
///
/// Similar to `get_leaf_block()` but also returns the path of index blocks
/// traversed to reach the leaf. This is needed for split operations.
///
/// # Parameters
///
/// * `inode_ref` - Directory inode reference
/// * `hash_info` - Hash information
///
/// # Returns
///
/// `HTreePath` containing the path and leaf block
pub fn get_leaf_with_path<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    hash_info: &HTreeHashInfo,
) -> Result<HTreePath> {
    let mut index_blocks = Vec::new();
    let mut current_block_idx = 0_u32;
    let _block_size = inode_ref.sb().block_size();

    // Read root to get indirect levels
    let root_block_addr = inode_ref.get_inode_dblk_idx(current_block_idx, false)?;
    let indirect_levels = {
        let bdev = inode_ref.bdev();
        let mut root_block = Block::get(bdev, root_block_addr)?;
        root_block.with_data(|data| {
            let root = unsafe { &*(data.as_ptr() as *const ext4_dir_idx_root) };
            root.info.indirect_levels()
        })?
    };

    let mut current_level = indirect_levels;

    // Walk through the index tree, recording the path
    loop {
        let physical_block = inode_ref.get_inode_dblk_idx(current_block_idx, false)?;
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, physical_block)?;

        let (next_block, position_idx, count, limit) = block.with_data(|data| -> Result<(u32, usize, u16, u16)> {
            // Parse entries
            let (entries, count, limit) = if current_block_idx == 0 {
                // Root block
                let root = unsafe { &*(data.as_ptr() as *const ext4_dir_idx_root) };
                let climit = unsafe {
                    &*((&root.en as *const _ as *const u8) as *const ext4_dir_idx_climit)
                };
                let count = climit.count();
                let limit = climit.limit();

                let entries_ptr = unsafe {
                    ((&root.en as *const _ as *const u8)
                        .add(core::mem::size_of::<ext4_dir_idx_climit>()))
                        as *const ext4_dir_idx_entry
                };
                let entries = unsafe {
                    core::slice::from_raw_parts(entries_ptr, count as usize)
                };

                (entries, count, limit)
            } else {
                // Non-root index node
                let _fake_entry = unsafe { &*(data.as_ptr() as *const crate::types::ext4_fake_dir_entry) };
                let climit = unsafe {
                    &*(data.as_ptr()
                        .add(core::mem::size_of::<crate::types::ext4_fake_dir_entry>())
                        as *const ext4_dir_idx_climit)
                };
                let count = climit.count();
                let limit = climit.limit();

                let entries_ptr = unsafe {
                    data.as_ptr()
                        .add(core::mem::size_of::<crate::types::ext4_fake_dir_entry>())
                        .add(core::mem::size_of::<ext4_dir_idx_climit>())
                        as *const ext4_dir_idx_entry
                };
                let entries = unsafe {
                    core::slice::from_raw_parts(entries_ptr, count as usize)
                };

                (entries, count, limit)
            };

            // Validate count
            if count == 0 || count > limit {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "HTree invalid entry count",
                ));
            }

            // Binary search for the right entry
            let mut left = 0_usize;
            let mut right = count as usize - 1;
            let mut result_idx = 0_usize;

            while left <= right {
                let mid = left + (right - left) / 2;
                let mid_hash = entries[mid].hash();

                if mid_hash > hash_info.hash {
                    if mid == 0 {
                        break;
                    }
                    right = mid - 1;
                } else {
                    result_idx = mid;
                    left = mid + 1;
                }
            }

            Ok((entries[result_idx].block(), result_idx, count, limit))
        })??;

        // Record this index block in the path (but only if not a leaf)
        if current_level > 0 {
            index_blocks.push(IndexBlockInfo {
                logical_block: current_block_idx,
                block_addr: physical_block,
                position_idx,
                entry_count: count,
                entry_limit: limit,
            });
        }

        drop(block);

        // Check if we're at a leaf
        if current_level == 0 {
            return Ok(HTreePath {
                index_blocks,
                leaf_block: next_block,
            });
        }

        // Move to next level
        current_block_idx = next_block;
        current_level -= 1;
    }
}

/// Find directory entry using HTree index
///
/// 对应 lwext4 的 `ext4_dir_dx_find_entry()`
///
/// # Parameters
///
/// * `inode_ref` - Directory inode reference
/// * `name` - Entry name to find
///
/// # Returns
///
/// `Some(inode_num)` if found, `None` if not found
///
/// # Note
///
/// This function only does HTree lookup to find the candidate leaf block.
/// It then uses linear search within that block to find the actual entry.
/// This is intentionally separated to maintain modularity with the iterator.
pub fn find_entry<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
) -> Result<Option<u32>> {
    // Initialize hash info
    let hash_info = init_hash_info(inode_ref, name)?;

    // Find leaf block
    let _leaf_block = get_leaf_block(inode_ref, &hash_info)?;

    // Linear search in the leaf block
    // TODO: This should use DirIterator but positioned at specific block
    // For now, we return Unsupported as this requires iterator enhancement

    Err(Error::new(
        ErrorKind::Unsupported,
        "HTree find_entry requires positioned iterator (not yet implemented)",
    ))
}

/// Check if directory uses HTree indexing
///
/// # Parameters
///
/// * `inode_ref` - Directory inode reference
///
/// # Returns
///
/// `true` if directory uses HTree indexing
pub fn is_indexed<D: BlockDevice>(inode_ref: &mut InodeRef<D>) -> Result<bool> {
    // Check if inode has INDEX flag
    let has_index_flag = inode_ref.with_inode(|inode| {
        let flags = u32::from_le(inode.flags);
        (flags & EXT4_INODE_FLAG_INDEX) != 0
    })?;

    if !has_index_flag {
        return Ok(false);
    }

    // Check if filesystem supports directory indexing
    let sb_supports = inode_ref.sb().has_compat_feature(EXT4_FEATURE_COMPAT_DIR_INDEX);

    Ok(has_index_flag && sb_supports)
}

// ============================================================================
// NOT IMPLEMENTED: Write Operations
// ============================================================================
//
// The following functions from lwext4 are NOT implemented because they require:
// 1. Transaction support
// 2. Block allocation
// 3. Directory entry modification
//
// =============================================================================
// HTree Splitting Operations
// =============================================================================

use crate::types::{ext4_dir_en, ext4_dir_entry_tail, ext4_fake_dir_entry};
use crate::balloc::BlockAllocator;
use super::checksum::init_entry_tail;

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
///
/// # 参数
///
/// * `inode_ref` - 目录 inode 引用
/// * `sb` - 可变 superblock 引用（用于块分配）
/// * `old_block_addr` - 旧块的物理地址
/// * `hash_info` - 哈希信息
///
/// # 返回
///
/// (new_logical_block, split_hash)
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
    let mut entries = alloc::vec::Vec::new();

    {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, old_block_addr)?;

        block.with_data(|data| {
            let mut offset = 0;

            while offset < block_size {
                if offset + 8 > block_size {
                    break;
                }

                let de = unsafe { &*(data.as_ptr().add(offset) as *const ext4_dir_en) };
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
            let de = unsafe { &mut *(data.as_mut_ptr().add(offset) as *mut ext4_dir_en) };
            de.inode = entry.inode.to_le();
            de.rec_len = rec_len.to_le();
            de.name_len = entry.name_len;
            de.file_type = entry.file_type;

            let name_len = entry.name_len as usize;
            data[offset + 8..offset + 8 + name_len].copy_from_slice(&entry.name[..name_len]);

            offset += rec_len as usize;
        }

        // 初始化 tail 和校验和
        if has_csum {
            let tail_offset = block_size - core::mem::size_of::<ext4_dir_entry_tail>();
            let tail = unsafe {
                &mut *(data[tail_offset..].as_mut_ptr() as *mut ext4_dir_entry_tail)
            };
            init_entry_tail(tail);

            update_dir_block_checksum(
                has_csum,
                &uuid,
                dir_inode,
                inode_generation,
                data,
                block_size,
            );
        }
    })?;

    Ok(())
}

/// Insert an index entry into an index block
///
/// 对应 lwext4 的 `ext4_dir_dx_insert_entry()`
///
/// # 参数
///
/// * `inode_ref` - 目录 inode 引用
/// * `index_block_addr` - 索引块的物理地址
/// * `insert_position` - 插入位置（在 entries 数组中的索引）
/// * `hash` - 哈希值
/// * `logical_block` - 逻辑块号
fn insert_index_entry<D: BlockDevice>(
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
            let fake_entry = unsafe { &*(data.as_ptr() as *const ext4_fake_dir_entry) };
            // Root block 有 dot entries
            u16::from_le(fake_entry.entry_len) != block_size as u16
        };

        let entries_offset = if is_root {
            // Root: skip dot entries (2*12) + root info (8)
            2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>()
                + core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>()
        } else {
            // Non-root: skip fake entry
            core::mem::size_of::<ext4_fake_dir_entry>()
        };

        // 读取 climit
        let climit_ptr = unsafe {
            data.as_mut_ptr().add(entries_offset) as *mut ext4_dir_idx_climit
        };
        let climit = unsafe { &mut *climit_ptr };
        let count = u16::from_le(climit.count);

        // 计算插入位置
        let entry_size = core::mem::size_of::<ext4_dir_idx_entry>();
        let insert_offset = entries_offset + entry_size * insert_position;
        let old_entry_ptr = unsafe { data.as_ptr().add(insert_offset) };
        let new_entry_ptr = unsafe { data.as_mut_ptr().add(insert_offset + entry_size) };

        // 移动后续条目腾出空间
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

        // 写入新条目
        let new_entry = unsafe {
            &mut *(data.as_mut_ptr().add(insert_offset) as *mut ext4_dir_idx_entry)
        };
        new_entry.hash = hash.to_le();
        new_entry.block = logical_block.to_le();

        // 更新 count
        climit.count = (count + 1).to_le();

        // 更新校验和（如果需要）
        if has_csum {
            update_index_block_checksum(has_csum, data, block_size);
        }
    })?;

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
    // 当前占位实现
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

/// Split a full HTree index block
///
/// 对应 lwext4 的 `ext4_dir_dx_split_index()`
///
/// # 算法流程
///
/// **Case A - 非 root 分裂（levels > 0）**：
/// 1. 检查索引块是否已满
/// 2. 分配新索引块
/// 3. 1:1 分割索引条目
/// 4. 在父级插入新条目
///
/// **Case B - root 分裂（levels == 0）**：
/// 1. 所有条目移到新 child 块
/// 2. root 只保留一个条目指向新 child
/// 3. indirect_levels += 1
///
/// # 参数
///
/// * `inode_ref` - 目录 inode 引用
/// * `sb` - 可变 superblock 引用
/// * `index_block_addr` - 索引块的物理地址
/// * `is_root` - 是否是 root 块
/// * `position_in_entries` - 当前插入位置在 entries 中的索引
///
/// # 返回
///
/// IndexSplitResult 包含新块信息和分割哈希值
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
                2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>()
                    + core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>()
            } else {
                core::mem::size_of::<ext4_fake_dir_entry>()
            };

            let climit = unsafe {
                &*(data.as_ptr().add(entries_offset) as *const ext4_dir_idx_climit)
            };

            (u16::from_le(climit.count), u16::from_le(climit.limit))
        })?
    };

    // 2. 检查是否需要分裂
    if count < limit {
        // 还有空间，不需要分裂
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
        // Case A: 非 root 分裂
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
        // Case B: root 分裂
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
    // 分割哈希值需要从新块的第一个条目读取
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
    let entries_offset = core::mem::size_of::<ext4_fake_dir_entry>();

    // 读取右半部分条目
    let right_entries = {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, old_block_addr)?;

        block.with_data(|data| {
            let start = entries_offset + entry_size * count_left as usize;
            let len = entry_size * count_right as usize;
            let mut entries = alloc::vec::Vec::with_capacity(len);
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
            let fake = unsafe { &mut *(data.as_mut_ptr() as *mut ext4_fake_dir_entry) };
            fake.inode = 0;
            fake.entry_len = (block_size as u16).to_le();
            fake.name_len = 0;
            fake.inode_type = 0;

            // 写入 climit
            let climit = unsafe {
                &mut *(data.as_mut_ptr().add(entries_offset) as *mut ext4_dir_idx_climit)
            };

            let tail_size = if has_csum {
                core::mem::size_of::<crate::types::ext4_dir_idx_tail>()
            } else {
                0
            };
            let entry_space = block_size - entries_offset - tail_size;
            let max_entries = (entry_space / entry_size) as u16;

            climit.limit = max_entries.to_le();
            climit.count = count_right.to_le();

            // 写入条目
            data[entries_offset + core::mem::size_of::<ext4_dir_idx_climit>()..]
                [..right_entries.len()]
                .copy_from_slice(&right_entries);

            // 更新校验和
            if has_csum {
                update_index_block_checksum(has_csum, data, block_size);
            }
        })?;
    }

    // 更新旧块的 count
    {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, old_block_addr)?;

        block.with_data_mut(|data| {
            let climit = unsafe {
                &mut *(data.as_mut_ptr().add(entries_offset) as *mut ext4_dir_idx_climit)
            };
            climit.count = count_left.to_le();

            if has_csum {
                update_index_block_checksum(has_csum, data, block_size);
            }
        })?;
    }

    // TODO: 根据 position_in_entries 判断是否需要切换当前块指针
    // 这需要在调用者处理

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
    let root_entries_offset = 2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>()
        + core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>();
    let child_entries_offset = core::mem::size_of::<ext4_fake_dir_entry>();

    // 读取所有条目
    let all_entries = {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, root_block_addr)?;

        block.with_data(|data| {
            let start = root_entries_offset + core::mem::size_of::<ext4_dir_idx_climit>();
            let len = entry_size * count as usize;
            let mut entries = alloc::vec::Vec::with_capacity(len);
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
            let fake = unsafe { &mut *(data.as_mut_ptr() as *mut ext4_fake_dir_entry) };
            fake.inode = 0;
            fake.entry_len = (block_size as u16).to_le();
            fake.name_len = 0;
            fake.inode_type = 0;

            // 写入 climit
            let climit = unsafe {
                &mut *(data.as_mut_ptr().add(child_entries_offset) as *mut ext4_dir_idx_climit)
            };

            let tail_size = if has_csum {
                core::mem::size_of::<crate::types::ext4_dir_idx_tail>()
            } else {
                0
            };
            let entry_space = block_size - child_entries_offset - tail_size;
            let max_entries = (entry_space / entry_size) as u16;

            climit.limit = max_entries.to_le();
            climit.count = count.to_le();

            // 写入所有条目
            data[child_entries_offset + core::mem::size_of::<ext4_dir_idx_climit>()..]
                [..all_entries.len()]
                .copy_from_slice(&all_entries);

            // 更新校验和
            if has_csum {
                update_index_block_checksum(has_csum, data, block_size);
            }
        })?;
    }

    // 更新 root 块
    {
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, root_block_addr)?;

        block.with_data_mut(|data| {
            // 更新 root info: indirect_levels = 1
            let root_info_offset = 2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>();
            let root_info = unsafe {
                &mut *(data.as_mut_ptr().add(root_info_offset) as *mut crate::types::ext4_dir_idx_rinfo)
            };
            root_info.indirect_levels = 1;

            // 更新 climit: count = 1
            let climit = unsafe {
                &mut *(data.as_mut_ptr().add(root_entries_offset) as *mut ext4_dir_idx_climit)
            };
            climit.count = 1_u16.to_le();

            // 写入唯一的条目，指向新 child
            let entry = unsafe {
                &mut *(data.as_mut_ptr().add(
                    root_entries_offset + core::mem::size_of::<ext4_dir_idx_climit>()
                ) as *mut ext4_dir_idx_entry)
            };
            entry.hash = 0_u32.to_le(); // Root entry hash is 0
            entry.block = new_child_logical.to_le();

            // 更新校验和
            if has_csum {
                update_index_block_checksum(has_csum, data, block_size);
            }
        })?;
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
            2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>()
                + core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>()
        } else {
            core::mem::size_of::<ext4_fake_dir_entry>()
        };

        let first_entry_offset = entries_offset + core::mem::size_of::<ext4_dir_idx_climit>();
        let entry = unsafe {
            &*(data.as_ptr().add(first_entry_offset) as *const ext4_dir_idx_entry)
        };

        u32::from_le(entry.hash)
    })
}

// Functions requiring implementation:
//
// ❌ ext4_dir_dx_reset_parent_inode()
//    - Update parent inode reference
//    - Requires: transaction, directory entry modification
//
// ✅ split_leaf_block() - Implemented
// ✅ split_index_block() - Implemented
// ⏳ Integration into add_entry - Next step
//
// These will be integrated into add_entry after testing

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_space_calculation() {
        // Test with 4KB blocks, no metadata_csum
        // Should be (4096 - 2*12 - 8) / 8 = (4096 - 32) / 8 = 508 entries
        let block_size = 4096;
        // Note: This test would need a mock Superblock
        // For now, just check the calculation logic
        let base_space = block_size
            - 2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>() as u32
            - core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>() as u32;
        let entries = base_space / core::mem::size_of::<ext4_dir_idx_entry>() as u32;
        assert_eq!(entries, 508);
    }
}
