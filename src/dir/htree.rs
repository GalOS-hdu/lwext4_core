//! HTree directory indexing — 查找与类型定义
//!
//! Implements ext4's HTree (hash tree) directory indexing for fast lookups
//! in large directories.
//!
//! 分裂与插入操作见 [`super::htree_split`]。
//!
//! 对应 lwext4 的 ext4_dir_idx.c

use crate::{
    block::{Block, BlockDevice},
    bytes::{read_struct, read_struct_at},
    consts::*,
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_dir_idx_climit, ext4_dir_idx_entry, ext4_dir_idx_root, ext4_fake_dir_entry},
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

/// 计算 root 块中 entries/climit 的起始偏移量
pub(super) fn root_entries_offset() -> usize {
    2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>()
        + core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>()
}

/// 计算 non-root 索引块中 entries/climit 的起始偏移量
pub(super) fn non_root_entries_offset() -> usize {
    core::mem::size_of::<ext4_fake_dir_entry>()
}

/// 从 data 中读取 entries 数组（安全版本）
///
/// entries_offset 是 climit 所在的偏移量，entries 紧跟在 climit 之后
fn read_entries_from_data(
    data: &[u8],
    entries_offset: usize,
    count: u16,
) -> Result<Vec<ext4_dir_idx_entry>> {
    let entry_size = core::mem::size_of::<ext4_dir_idx_entry>();
    let climit_size = core::mem::size_of::<ext4_dir_idx_climit>();
    let first_entry_offset = entries_offset + climit_size;
    let mut entries = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let entry: ext4_dir_idx_entry =
            read_struct_at(data, first_entry_offset + i * entry_size)?;
        entries.push(entry);
    }
    Ok(entries)
}

/// Initialize hash info from root block
///
/// 对应 lwext4 的 `ext4_dir_hinfo_init()`
pub fn init_hash_info<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
) -> Result<HTreeHashInfo> {
    // Extract data from inode_ref BEFORE getting block
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
        let root: ext4_dir_idx_root = read_struct(data)?;

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
        let entries_offset = root_entries_offset();
        let climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;
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
#[allow(dead_code)] // 待 htree 写入功能完成后使用
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
pub fn get_leaf_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    hash_info: &HTreeHashInfo,
) -> Result<u32> {
    let mut current_block_idx = 0_u32;
    let _block_size = inode_ref.sb().block_size();

    // Read root to get indirect levels
    let root_block_addr = inode_ref.get_inode_dblk_idx(current_block_idx, false)?;
    let indirect_levels = {
        let bdev = inode_ref.bdev();
        let mut root_block = Block::get(bdev, root_block_addr)?;
        root_block.with_data(|data| {
            let root: ext4_dir_idx_root = read_struct(data)?;
            Ok::<_, Error>(root.info.indirect_levels())
        })?
    }?;

    let mut current_level = indirect_levels;

    // Walk through the index tree
    loop {
        let physical_block = inode_ref.get_inode_dblk_idx(current_block_idx, false)?;
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, physical_block)?;

        let next_block = block.with_data(|data| -> Result<u32> {
            let (entries, count, limit) = if current_block_idx == 0 {
                let entries_offset = root_entries_offset();
                let climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;
                let count = climit.count();
                let limit = climit.limit();
                let entries = read_entries_from_data(data, entries_offset, count)?;
                (entries, count, limit)
            } else {
                let entries_offset = non_root_entries_offset();
                let climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;
                let count = climit.count();
                let limit = climit.limit();
                let entries = read_entries_from_data(data, entries_offset, count)?;
                (entries, count, limit)
            };

            if count == 0 || count > limit {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "HTree invalid entry count",
                ));
            }

            // Binary search
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

            Ok(entries[result_idx].block())
        })??;

        if current_level == 0 {
            return Ok(next_block);
        }

        current_block_idx = next_block;
        current_level -= 1;
    }
}

/// Get leaf block with full path information
///
/// Similar to `get_leaf_block()` but also returns the path of index blocks
/// traversed to reach the leaf. This is needed for split operations.
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
            let root: ext4_dir_idx_root = read_struct(data)?;
            Ok::<_, Error>(root.info.indirect_levels())
        })?
    }?;

    let mut current_level = indirect_levels;

    // Walk through the index tree, recording the path
    loop {
        let physical_block = inode_ref.get_inode_dblk_idx(current_block_idx, false)?;
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, physical_block)?;

        let (next_block, position_idx, count, limit) = block.with_data(|data| -> Result<(u32, usize, u16, u16)> {
            let (entries, count, limit) = if current_block_idx == 0 {
                let entries_offset = root_entries_offset();
                let climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;
                let count = climit.count();
                let limit = climit.limit();
                let entries = read_entries_from_data(data, entries_offset, count)?;
                (entries, count, limit)
            } else {
                let entries_offset = non_root_entries_offset();
                let climit: ext4_dir_idx_climit = read_struct_at(data, entries_offset)?;
                let count = climit.count();
                let limit = climit.limit();
                let entries = read_entries_from_data(data, entries_offset, count)?;
                (entries, count, limit)
            };

            if count == 0 || count > limit {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "HTree invalid entry count",
                ));
            }

            // Binary search
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

        if current_level == 0 {
            return Ok(HTreePath {
                index_blocks,
                leaf_block: next_block,
            });
        }

        current_block_idx = next_block;
        current_level -= 1;
    }
}

/// Find directory entry using HTree index
///
/// 对应 lwext4 的 `ext4_dir_dx_find_entry()`
pub fn find_entry<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
) -> Result<Option<u32>> {
    let hash_info = init_hash_info(inode_ref, name)?;
    let _leaf_block = get_leaf_block(inode_ref, &hash_info)?;

    // TODO: This should use DirIterator but positioned at specific block
    Err(Error::new(
        ErrorKind::Unsupported,
        "HTree find_entry requires positioned iterator (not yet implemented)",
    ))
}

/// Check if directory uses HTree indexing
pub fn is_indexed<D: BlockDevice>(inode_ref: &mut InodeRef<D>) -> Result<bool> {
    let has_index_flag = inode_ref.with_inode(|inode| {
        let flags = u32::from_le(inode.flags);
        (flags & EXT4_INODE_FLAG_INDEX) != 0
    })?;

    if !has_index_flag {
        return Ok(false);
    }

    let sb_supports = inode_ref.sb().has_compat_feature(EXT4_FEATURE_COMPAT_DIR_INDEX);

    Ok(has_index_flag && sb_supports)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_space_calculation() {
        let block_size = 4096;
        let base_space = block_size
            - 2 * core::mem::size_of::<crate::types::ext4_dir_idx_dot_en>() as u32
            - core::mem::size_of::<crate::types::ext4_dir_idx_rinfo>() as u32;
        let entries = base_space / core::mem::size_of::<ext4_dir_idx_entry>() as u32;
        assert_eq!(entries, 508);
    }
}
