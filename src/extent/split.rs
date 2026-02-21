//! Extent 节点分裂功能
//!
//! 当 extent 节点满时，需要分裂成两个节点

use crate::{
    balloc::BlockAllocator,
    block::{Block, BlockDevice},
    consts::*,
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_extent, ext4_extent_header, ext4_extent_idx},
};

use super::{
    helpers::*,
    write::{ExtentPath, ExtentNodeType},
};

use alloc::vec::Vec;

/// 分裂 extent 节点
///
/// 对应 lwext4 的 `ext4_ext_split()`
///
/// 当一个 extent 节点满时，将其分裂成两个节点：
/// 1. 分配新的物理块
/// 2. 将当前节点的一半条目移动到新节点
/// 3. 在父节点中插入新的索引
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `allocator` - 块分配器
/// * `path` - Extent 路径（包含需要分裂的节点）
/// * `at` - 需要分裂的节点在路径中的索引（0 = root）
/// * `new_extent` - 触发分裂的新 extent（用于确定插入位置）
///
/// # 返回
///
/// 成功返回 ()，失败返回错误
///
/// # 实现细节
///
/// 1. 分配新的extent块作为分裂后的右节点
/// 2. 将当前节点后半部分的条目移动到新节点
/// 3. 更新两个节点的 header
/// 4. 在父节点中插入指向新节点的索引
/// 5. 如果父节点也满了，递归分裂父节点
///
/// # 错误
///
/// - `ErrorKind::NoSpace` - 无法分配新块
/// - `ErrorKind::InvalidInput` - 参数无效
pub fn split_extent_node<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    path: &mut ExtentPath,
    at: usize,
    new_extent_logical_block: u32,
) -> Result<()> {
    // 检查路径有效性
    if at >= path.nodes.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "split: at out of bounds",
        ));
    }

    // 获取当前节点信息
    let node = &path.nodes[at];
    let is_leaf = node.node_type == ExtentNodeType::Leaf
                  || node.node_type == ExtentNodeType::Root && node.header.is_leaf();
    let _depth = node.depth;
    let entries = node.header.entries_count();

    if entries < 2 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "split: node has too few entries",
        ));
    }

    // 计算分裂点（将节点从中间分成两半）
    let split_at = entries / 2;

    // 分配新的物理块用于右节点
    let new_block = allocator.alloc_block(
        inode_ref.bdev(),
        sb,
        0, // goal = 0 让 balloc 自己选择
    )?;

    // 根据节点类型执行不同的分裂逻辑
    if is_leaf {
        split_leaf_node(
            inode_ref,
            sb,
            allocator,
            path,
            at,
            new_block,
            split_at,
            new_extent_logical_block,
        )?;
    } else {
        split_index_node(
            inode_ref,
            sb,
            allocator,
            path,
            at,
            new_block,
            split_at,
            new_extent_logical_block,
        )?;
    }

    Ok(())
}

/// 分裂叶子节点
///
/// 将叶子节点的 extent 条目分裂到两个节点
fn split_leaf_node<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    path: &mut ExtentPath,
    at: usize,
    new_block: u64,
    split_at: u16,
    _new_extent_logical_block: u32,
) -> Result<()> {
    let block_size = sb.block_size();
    let node = &path.nodes[at];
    let _depth = node.depth;

    // 读取当前节点数据
    let (old_extents, old_header) = if node.node_type == ExtentNodeType::Root {
        // 根节点在 inode 中
        read_extents_from_inode(inode_ref)?
    } else {
        // 从独立块读取
        read_extents_from_block(inode_ref.bdev(), node.block_addr, block_size)?
    };

    let entries = old_header.entries_count();

    // 计算移动到新节点的条目数
    let move_count = entries - split_at;

    // 创建新节点（右节点）
    let new_header = ext4_extent_header {
        magic: EXT4_EXTENT_MAGIC.to_le(),
        entries: move_count.to_le(),
        max: ext4_ext_space_block(block_size).to_le(),
        depth: 0u16.to_le(), // 叶子节点
        generation: old_header.generation,
    };

    // 准备新节点的 extent 数组（后半部分）
    let new_extents = old_extents[split_at as usize..entries as usize].to_vec();

    // 写入新节点到新分配的块
    write_extents_to_block(
        inode_ref.bdev(),
        new_block,
        block_size,
        &new_header,
        &new_extents,
    )?;

    // 更新旧节点（保留前半部分）
    let updated_header = ext4_extent_header {
        magic: old_header.magic,
        entries: split_at.to_le(),
        max: old_header.max,
        depth: old_header.depth,
        generation: old_header.generation,
    };

    let kept_extents = old_extents[0..split_at as usize].to_vec();

    if node.node_type == ExtentNodeType::Root {
        write_extents_to_inode(inode_ref, &updated_header, &kept_extents)?;
    } else {
        write_extents_to_block(
            inode_ref.bdev(),
            node.block_addr,
            block_size,
            &updated_header,
            &kept_extents,
        )?;
    }

    // 获取新节点的第一个逻辑块号（用于父索引）
    let new_node_first_block = if !new_extents.is_empty() {
        new_extents[0].logical_block()
    } else {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "split: new node has no extents",
        ));
    };

    // 在父节点中插入新索引
    insert_parent_index(
        inode_ref,
        sb,
        allocator,
        path,
        at,
        new_node_first_block,
        new_block,
    )?;

    Ok(())
}

/// 分裂索引节点
///
/// 将索引节点的 index 条目分裂到两个节点
fn split_index_node<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    path: &mut ExtentPath,
    at: usize,
    new_block: u64,
    split_at: u16,
    _new_extent_logical_block: u32,
) -> Result<()> {
    let block_size = sb.block_size();
    let node = &path.nodes[at];
    let depth = node.depth;

    // 读取当前节点数据
    let (old_indices, old_header) = if node.node_type == ExtentNodeType::Root {
        read_indices_from_inode(inode_ref)?
    } else {
        read_indices_from_block(inode_ref.bdev(), node.block_addr, block_size)?
    };

    let entries = old_header.entries_count();
    let move_count = entries - split_at;

    // 创建新节点（右节点）
    let new_header = ext4_extent_header {
        magic: EXT4_EXTENT_MAGIC.to_le(),
        entries: move_count.to_le(),
        max: ext4_ext_space_block_idx(block_size).to_le(),
        depth: depth.to_le(),
        generation: old_header.generation,
    };

    // 准备新节点的 index 数组（后半部分）
    let new_indices = old_indices[split_at as usize..entries as usize].to_vec();

    // 写入新节点
    write_indices_to_block(
        inode_ref.bdev(),
        new_block,
        block_size,
        &new_header,
        &new_indices,
    )?;

    // 更新旧节点（保留前半部分）
    let updated_header = ext4_extent_header {
        magic: old_header.magic,
        entries: split_at.to_le(),
        max: old_header.max,
        depth: old_header.depth,
        generation: old_header.generation,
    };

    let kept_indices = old_indices[0..split_at as usize].to_vec();

    if node.node_type == ExtentNodeType::Root {
        write_indices_to_inode(inode_ref, &updated_header, &kept_indices)?;
    } else {
        write_indices_to_block(
            inode_ref.bdev(),
            node.block_addr,
            block_size,
            &updated_header,
            &kept_indices,
        )?;
    }

    // 获取新节点的第一个逻辑块号
    let new_node_first_block = if !new_indices.is_empty() {
        new_indices[0].logical_block()
    } else {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "split: new index node has no entries",
        ));
    };

    // 在父节点中插入新索引
    insert_parent_index(
        inode_ref,
        sb,
        allocator,
        path,
        at,
        new_node_first_block,
        new_block,
    )?;

    Ok(())
}

/// 在父节点中插入指向新节点的索引
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `allocator` - 块分配器
/// * `path` - Extent 路径
/// * `child_at` - 子节点在路径中的位置
/// * `first_block` - 新索引的逻辑块号
/// * `physical_block` - 新索引指向的物理块号
fn insert_parent_index<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    path: &mut ExtentPath,
    child_at: usize,
    first_block: u32,
    physical_block: u64,
) -> Result<()> {
    // 如果child是根节点，需要增加树深度
    let parent_at = if child_at == 0 {
        // 调用 grow_tree_depth 增加树的深度
        // grow_tree_depth 会将当前根节点移到新块，并创建新的根索引节点
        // 新根节点包含一个指向旧根内容的索引（逻辑块0）
        crate::extent::grow_tree_depth(inode_ref, sb, allocator)?;

        // 🔧 BUG FIX: 不要直接返回！
        // grow_tree_depth 只插入了指向原root内容的第一个索引
        // 我们还需要在新root中插入第二个索引，指向分裂出的右半部分（physical_block）
        // 新root就是parent_at=0
        log::debug!(
            "[insert_parent_index] After grow_tree_depth, inserting second index: first_block={first_block}, physical_block={physical_block:#x}"
        );
        0
    } else {
        child_at - 1
    };

    // 检查父节点是否有空间
    // 注意：如果刚执行了grow_tree_depth，需要重新读取root header
    let (parent_entries, parent_max_entries) = if child_at == 0 {
        // grow_tree_depth之后，root已经是新的索引节点了
        // 需要从inode重新读取header
        inode_ref.with_inode(|inode| {
            let header = inode.extent_header();
            (header.entries_count(), header.max_entries())
        })?
    } else {
        let parent_node = &path.nodes[parent_at];
        (parent_node.header.entries_count(), parent_node.header.max_entries())
    };

    if parent_entries >= parent_max_entries {
        // 父节点也满了，需要先递归分裂父节点
        // 这里我们使用 first_block 作为分裂点的提示
        split_extent_node(
            inode_ref,
            sb,
            allocator,
            path,
            parent_at,
            first_block,
        )?;

        // 分裂后，路径可能已经改变，需要重新查找正确的父节点
        // 但是由于我们只是要插入索引，可以继续使用当前的路径
        // （分裂会确保有足够的空间）
    }

    // 在父节点中插入新索引
    insert_index_to_node(
        inode_ref,
        sb,
        path,
        parent_at,
        first_block,
        physical_block,
    )?;

    Ok(())
}

/// 在指定节点中插入索引
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `path` - Extent 路径
/// * `at` - 要插入索引的节点位置
/// * `first_block` - 新索引的逻辑块号
/// * `physical_block` - 新索引指向的物理块号
fn insert_index_to_node<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    path: &mut ExtentPath,
    at: usize,
    first_block: u32,
    physical_block: u64,
) -> Result<()> {
    let block_size = sb.block_size();
    let node = &path.nodes[at];

    // 读取当前节点的 index 数组
    let (mut indices, mut header) = if node.node_type == ExtentNodeType::Root {
        read_indices_from_inode(inode_ref)?
    } else {
        read_indices_from_block(inode_ref.bdev(), node.block_addr, block_size)?
    };

    let entries = header.entries_count();
    let max_entries = header.max_entries();

    // 确保有空间
    if entries >= max_entries {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Node is full, cannot insert index",
        ));
    }

    // 创建新索引
    let mut new_idx = ext4_extent_idx {
        block: first_block.to_le(),
        leaf_lo: 0u32.to_le(),
        leaf_hi: 0u16.to_le(),
        unused: 0u16.to_le(),
    };
    ext4_idx_store_pblock(&mut new_idx, physical_block);

    // 找到插入位置（保持索引按 first_block 排序）
    let insert_pos = indices
        .iter()
        .position(|idx| idx.logical_block() > first_block)
        .unwrap_or(indices.len());

    // 插入新索引
    indices.insert(insert_pos, new_idx);

    // 更新 header
    header.entries = (entries + 1).to_le();

    // 写回节点
    if node.node_type == ExtentNodeType::Root {
        write_indices_to_inode(inode_ref, &header, &indices)?;
    } else {
        write_indices_to_block(
            inode_ref.bdev(),
            node.block_addr,
            block_size,
            &header,
            &indices,
        )?;
    }

    Ok(())
}

//=============================================================================
// 辅助函数：读取/写入 extent 和 index
//=============================================================================

/// 从 inode 读取 extent 数组
pub(super) fn read_extents_from_inode<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
) -> Result<(Vec<ext4_extent>, ext4_extent_header)> {
    inode_ref.with_inode(|inode| {
        let data = inode.extent_root_data();

        let header = unsafe {
            *(data.as_ptr() as *const ext4_extent_header)
        };

        let entries = u16::from_le(header.entries);
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

        Ok((extents, header))
    })?
}

/// 从块读取 extent 数组
pub(super) fn read_extents_from_block<D: BlockDevice>(
    bdev: &mut crate::block::BlockDev<D>,
    block_addr: u64,
    _block_size: u32,
) -> Result<(Vec<ext4_extent>, ext4_extent_header)> {
    let mut block = Block::get(bdev, block_addr)?;

    block.with_data(|data| -> Result<(Vec<ext4_extent>, ext4_extent_header)> {
        let header = unsafe {
            *(data.as_ptr() as *const ext4_extent_header)
        };

        let entries = u16::from_le(header.entries);
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let extent_size = core::mem::size_of::<ext4_extent>();

        let mut extents = Vec::new();
        for i in 0..entries as usize {
            let offset = header_size + i * extent_size;
            if offset + extent_size > data.len() {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Extent block data too short",
                ));
            }
            let extent = unsafe {
                *(data[offset..].as_ptr() as *const ext4_extent)
            };
            extents.push(extent);
        }

        Ok((extents, header))
    })?
}

/// 从 inode 读取 index 数组
pub(super) fn read_indices_from_inode<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
) -> Result<(Vec<ext4_extent_idx>, ext4_extent_header)> {
    inode_ref.with_inode(|inode| {
        let data = inode.extent_root_data();

        let header = unsafe {
            *(data.as_ptr() as *const ext4_extent_header)
        };

        let entries = u16::from_le(header.entries);
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

        Ok((indices, header))
    })?
}

/// 从块读取 index 数组
pub(super) fn read_indices_from_block<D: BlockDevice>(
    bdev: &mut crate::block::BlockDev<D>,
    block_addr: u64,
    _block_size: u32,
) -> Result<(Vec<ext4_extent_idx>, ext4_extent_header)> {
    let mut block = Block::get(bdev, block_addr)?;

    block.with_data(|data| -> Result<(Vec<ext4_extent_idx>, ext4_extent_header)> {
        let header = unsafe {
            *(data.as_ptr() as *const ext4_extent_header)
        };

        let entries = u16::from_le(header.entries);
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let idx_size = core::mem::size_of::<ext4_extent_idx>();

        let mut indices = Vec::new();
        for i in 0..entries as usize {
            let offset = header_size + i * idx_size;
            if offset + idx_size > data.len() {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Extent index block data too short",
                ));
            }
            let idx = unsafe {
                *(data[offset..].as_ptr() as *const ext4_extent_idx)
            };
            indices.push(idx);
        }

        Ok((indices, header))
    })?
}

/// 写入 extent 数组到 inode
pub(super) fn write_extents_to_inode<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    header: &ext4_extent_header,
    extents: &[ext4_extent],
) -> Result<()> {
    inode_ref.with_inode_mut(|inode| -> Result<()> {
        let data = inode.extent_root_data_mut();

        // 写入 header
        unsafe {
            *(data.as_mut_ptr() as *mut ext4_extent_header) = *header;
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

        Ok(())
    })??;

    inode_ref.mark_dirty()?;
    Ok(())
}

/// 写入 extent 数组到块
pub(super) fn write_extents_to_block<D: BlockDevice>(
    bdev: &mut crate::block::BlockDev<D>,
    block_addr: u64,
    _block_size: u32,
    header: &ext4_extent_header,
    extents: &[ext4_extent],
) -> Result<()> {
    {
        let mut block = Block::get(bdev, block_addr)?;

        block.with_data_mut(|data| {
            // 清零整个块
            data.fill(0);

            // 写入 header
            unsafe {
                *(data.as_mut_ptr() as *mut ext4_extent_header) = *header;
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

/// 写入 index 数组到 inode
fn write_indices_to_inode<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    header: &ext4_extent_header,
    indices: &[ext4_extent_idx],
) -> Result<()> {
    inode_ref.with_inode_mut(|inode| -> Result<()> {
        let data = inode.extent_root_data_mut();

        // 写入 header
        unsafe {
            *(data.as_mut_ptr() as *mut ext4_extent_header) = *header;
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

        Ok(())
    })??;

    inode_ref.mark_dirty()?;
    Ok(())
}

/// 写入 index 数组到块
fn write_indices_to_block<D: BlockDevice>(
    bdev: &mut crate::block::BlockDev<D>,
    block_addr: u64,
    _block_size: u32,
    header: &ext4_extent_header,
    indices: &[ext4_extent_idx],
) -> Result<()> {
    {
        let mut block = Block::get(bdev, block_addr)?;

        block.with_data_mut(|data| {
            // 清零整个块
            data.fill(0);

            // 写入 header
            unsafe {
                *(data.as_mut_ptr() as *mut ext4_extent_header) = *header;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_api() {
        // 需要实际的块设备和 ext4 文件系统进行测试
        // 主要验证 API 编译和基本逻辑
    }
}
