//! Extent 树深度增长功能
//!
//! 当根节点需要分裂时，增加树的深度

use crate::{
    balloc::BlockAllocator,
    block::{Block, BlockDevice},
    consts::*,
    error::Result,
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_extent, ext4_extent_header, ext4_extent_idx},
};

use super::helpers::*;
use alloc::vec::Vec;

/// 增加 extent 树的深度
///
/// 对应 lwext4 的 `ext4_ext_grow_indepth()`
///
/// 当根节点（位于 inode 中）需要分裂时，我们不能直接分裂它，
/// 因为根节点必须保持在 inode 中。因此，我们需要：
/// 1. 分配一个新的物理块
/// 2. 将当前根节点的所有内容复制到新块
/// 3. 将 inode 中的根节点转换为索引节点，指向新分配的块
/// 4. 增加树的深度
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `allocator` - 块分配器
///
/// # 返回
///
/// 成功返回新分配的块地址，失败返回错误
///
/// # 实现细节
///
/// 假设原来树深度为 0（根即叶）：
/// ```text
/// Before:
/// Root (in inode, depth=0)
///   [extent1, extent2, extent3, ...]
///
/// After:
/// Root (in inode, depth=1)
///   [index -> new_block]
/// new_block (depth=0)
///   [extent1, extent2, extent3, ...]
/// ```
///
/// # 错误
///
/// - `ErrorKind::NoSpace` - 无法分配新块
/// - `ErrorKind::InvalidInput` - 参数无效
pub fn grow_tree_depth<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
) -> Result<u64> {
    let block_size = sb.block_size();

    // 1. 读取当前根节点信息
    let (old_header, is_leaf) = inode_ref.with_inode(|inode| {
        let header = *inode.extent_header();

        let is_leaf = header.is_leaf();

        (header, is_leaf)
    })?;

    let old_depth = old_header.depth();
    let new_depth = old_depth + 1;

    log::debug!(
        "[GROW_TREE] Starting grow_tree_depth: old_depth={old_depth}, new_depth={new_depth}, is_leaf={is_leaf}"
    );

    // 2. 分配新的物理块
    let new_block = allocator.alloc_block(
        inode_ref.bdev(),
        sb,
        0, // goal = 0 让 balloc 自己选择
    )?;

    log::debug!(
        "[GROW_TREE] Allocated new block: 0x{new_block:x} (decimal: {new_block})"
    );

    // 3. 将当前根节点内容复制到新块
    if is_leaf {
        // 根节点是叶子，复制 extent 数组
        log::debug!("[GROW_TREE] Copying extents to new block 0x{new_block:x}");
        copy_extents_to_new_block(
            inode_ref,
            new_block,
            block_size,
            &old_header,
        )?;
    } else {
        // 根节点是索引节点，复制 index 数组
        log::debug!("[GROW_TREE] Copying indices to new block 0x{new_block:x}");
        copy_indices_to_new_block(
            inode_ref,
            new_block,
            block_size,
            &old_header,
            old_depth,
        )?;
    }

    // 4. 在 inode 中创建新的根节点
    // 新根节点是索引节点，只包含一个 index 指向刚才分配的块
    log::debug!(
        "[GROW_TREE] Creating new root in inode: depth={new_depth}, pointing to block 0x{new_block:x}"
    );
    create_new_root_in_inode(
        inode_ref,
        new_depth,
        new_block,
    )?;

    // 🔧 关键修复：强制写回 inode 到磁盘
    // grow_tree_depth 修改了 inode.blocks（extent 树的根节点），这是文件系统元数据的关键部分
    // 必须确保这个修改被立即持久化，否则后续读取可能读到旧的树结构，导致数据损坏
    log::debug!(
        "[GROW_TREE] Force writeback inode after grow_tree_depth (critical for consistency)"
    );
    inode_ref.force_writeback().map_err(|e| {
        log::error!("[GROW_TREE] Failed to force writeback after grow: {e:?}");
        e
    })?;

    log::debug!("[GROW_TREE] grow_tree_depth completed successfully with forced writeback");

    Ok(new_block)
}

/// 将 inode 中的 extent 数组复制到新分配的块
fn copy_extents_to_new_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    new_block: u64,
    block_size: u32,
    old_header: &ext4_extent_header,
) -> Result<()> {
    // 读取当前根节点的 extent 数组
    let extents: Vec<ext4_extent> = inode_ref.with_inode(|inode| {
        let data = inode.extent_root_data();

        let entries = u16::from_le(old_header.entries);
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let extent_size = core::mem::size_of::<ext4_extent>();

        let mut extents = Vec::new();
        for i in 0..entries as usize {
            let offset = header_size + i * extent_size;
            let extent = unsafe {
                *(data[offset..].as_ptr() as *const ext4_extent)
            };
            extents.push(extent);
        }

        extents
    })?;

    // 创建新块的 header
    let new_header = ext4_extent_header {
        magic: EXT4_EXTENT_MAGIC.to_le(),
        entries: old_header.entries,
        max: ext4_ext_space_block(block_size).to_le(), // 块中可以容纳更多 extent
        depth: 0u16.to_le(), // 叶子节点
        generation: old_header.generation,
    };

    // 写入新块
    {
        let mut block = Block::get(inode_ref.bdev(), new_block)?;

        block.with_data_mut(|data| {
            // 清零整个块
            data.fill(0);

            // 写入 header
            unsafe {
                *(data.as_mut_ptr() as *mut ext4_extent_header) = new_header;
            }

            // 写入 extent 数组
            let header_size = core::mem::size_of::<ext4_extent_header>();
            let extent_size = core::mem::size_of::<ext4_extent>();

            for (i, extent) in extents.iter().enumerate() {
                let offset = header_size + i * extent_size;
                unsafe {
                    *(data[offset..].as_mut_ptr() as *mut ext4_extent) = *extent;
                }
            }
        })?;
    } // block dropped here, marked dirty automatically

    Ok(())
}

/// 将 inode 中的 index 数组复制到新分配的块
fn copy_indices_to_new_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    new_block: u64,
    block_size: u32,
    old_header: &ext4_extent_header,
    old_depth: u16,
) -> Result<()> {
    // 读取当前根节点的 index 数组
    let indices: Vec<ext4_extent_idx> = inode_ref.with_inode(|inode| {
        let data = inode.extent_root_data();

        let entries = u16::from_le(old_header.entries);
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let idx_size = core::mem::size_of::<ext4_extent_idx>();

        let mut indices = Vec::new();
        for i in 0..entries as usize {
            let offset = header_size + i * idx_size;
            let idx = unsafe {
                *(data[offset..].as_ptr() as *const ext4_extent_idx)
            };
            indices.push(idx);
        }

        indices
    })?;

    // 创建新块的 header
    let new_header = ext4_extent_header {
        magic: EXT4_EXTENT_MAGIC.to_le(),
        entries: old_header.entries,
        max: ext4_ext_space_block_idx(block_size).to_le(), // 块中可以容纳更多 index
        depth: old_depth.to_le(), // 保持原深度
        generation: old_header.generation,
    };

    // 写入新块
    {
        let mut block = Block::get(inode_ref.bdev(), new_block)?;

        block.with_data_mut(|data| {
            // 清零整个块
            data.fill(0);

            // 写入 header
            unsafe {
                *(data.as_mut_ptr() as *mut ext4_extent_header) = new_header;
            }

            // 写入 index 数组
            let header_size = core::mem::size_of::<ext4_extent_header>();
            let idx_size = core::mem::size_of::<ext4_extent_idx>();

            for (i, idx) in indices.iter().enumerate() {
                let offset = header_size + i * idx_size;
                unsafe {
                    *(data[offset..].as_mut_ptr() as *mut ext4_extent_idx) = *idx;
                }
            }
        })?;
    } // block dropped here, marked dirty automatically

    Ok(())
}

/// 在 inode 中创建新的根节点
///
/// 新根节点是索引节点，包含一个 index 指向刚才分配的块
fn create_new_root_in_inode<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    new_depth: u16,
    child_block: u64,
) -> Result<()> {
    inode_ref.with_inode_mut(|inode| {
        let data = inode.extent_root_data_mut();

        // 创建新的根节点 header
        let header = unsafe {
            &mut *(data.as_mut_ptr() as *mut ext4_extent_header)
        };

        header.magic = EXT4_EXTENT_MAGIC.to_le();
        header.entries = 1u16.to_le(); // 只有一个 index
        header.max = ext4_ext_space_root_idx().to_le(); // inode 中的最大 index 数
        header.depth = new_depth.to_le(); // 新深度
        header.generation = 0u32.to_le();

        // 创建第一个 index（指向子节点）
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let first_idx = unsafe {
            &mut *(data[header_size..].as_mut_ptr() as *mut ext4_extent_idx)
        };

        first_idx.block = 0u32.to_le(); // 第一个 index 覆盖从逻辑块 0 开始
        ext4_idx_store_pblock(first_idx, child_block);
        first_idx.unused = 0u16.to_le();

        log::debug!(
            "[GROW_TREE] Wrote index to root: block=0, child_block=0x{:x}, leaf_lo=0x{:x}, leaf_hi=0x{:x}",
            child_block, first_idx.leaf_lo, first_idx.leaf_hi
        );

        // 打印整个 inode.blocks 的前 28 字节（header 12 + index 12 + 额外 4）
        log::debug!("[GROW_TREE] inode.blocks[0..28]: {:02x?}", &data[..28]);
    })?;

    inode_ref.mark_dirty()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grow_api() {
        // 需要实际的块设备和 ext4 文件系统进行测试
        // 主要验证 API 编译和基本逻辑
    }
}
