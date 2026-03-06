//! Unwritten Extent 多层树支持
//!
//! 扩展 unwritten extent 功能以支持任意深度的 extent 树
//!
//! ## 主要功能
//!
//! - 在多层树中分裂 unwritten extent
//! - 在多层树中转换 unwritten/initialized 状态
//! - 支持大文件的预分配和状态转换
//!
//! ## 对应 lwext4
//!
//! - `ext4_ext_split_extent()` - extent 分裂

use crate::{
    balloc::BlockAllocator,
    block::{Block, BlockDevice},
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_extent, ext4_extent_header},
};

use super::{
    split::{read_extents_from_block, read_extents_from_inode,
            write_extents_to_block, write_extents_to_inode},
    unwritten::{get_actual_len, get_pblock, is_unwritten,
                mark_initialized, mark_unwritten, store_pblock,
                EXT4_EXT_MARK_UNWRIT1, EXT4_EXT_MARK_UNWRIT2},
    path::ExtentNodeType,
    write::ExtentWriter,
};


/// 在多层树中分裂 extent
///
/// 对应 lwext4 的 `ext4_ext_split_extent()`
///
/// # 参数
///
/// * `writer` - ExtentWriter（包含 transaction）
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `allocator` - 块分配器
/// * `logical_block` - 要分裂的逻辑块号
/// * `split_flag` - 分裂标志（UNWRIT1/UNWRIT2）
///
/// # 分裂标志
///
/// - `EXT4_EXT_MARK_UNWRIT1`: 标记第一部分为 unwritten
/// - `EXT4_EXT_MARK_UNWRIT2`: 标记第二部分为 unwritten
///
/// # 返回
///
/// 成功返回 Ok(())
///
/// # 工作流程
///
/// 1. 查找包含 logical_block 的 extent 所在的叶子节点
/// 2. 在该叶子节点中找到对应的 extent
/// 3. 如果节点空间不足，先分裂节点
/// 4. 分裂 extent 成两部分
/// 5. 根据 split_flag 设置状态
pub fn split_extent_at_multilevel<D: BlockDevice>(
    writer: &mut ExtentWriter<D>,
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    logical_block: u32,
    split_flag: u32,
) -> Result<()> {
    // 1. 查找 extent 路径
    let mut path = writer.find_extent_path(inode_ref, logical_block)?;

    // 2. 获取叶子节点
    let leaf = path.leaf().ok_or_else(|| {
        Error::new(ErrorKind::Corrupted, "Extent path has no leaf node")
    })?;

    let leaf_block_addr = leaf.block_addr;
    let leaf_node_type = leaf.node_type;
    let _leaf_depth = leaf.depth;

    // 3. 在叶子节点中查找 extent
    let (extent_idx, extent_info) = find_extent_in_leaf(
        inode_ref,
        leaf_block_addr,
        leaf_node_type,
        sb.block_size(),
        logical_block,
    )?;

    let (ee_block, ee_len, _ee_start, _was_unwritten) = extent_info;

    // 验证分裂点在 extent 范围内
    if logical_block < ee_block || logical_block >= ee_block + ee_len as u32 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Split point not in extent range",
        ));
    }

    // 4. 如果分裂点在起始位置，只需改变状态
    if logical_block == ee_block {
        return change_extent_status(
            inode_ref,
            leaf_block_addr,
            leaf_node_type,
            sb.block_size(),
            extent_idx,
            split_flag,
        );
    }

    // 5. 检查叶子节点是否有空间插入新 extent
    let entries = leaf.header.entries_count();
    let max_entries = leaf.header.max_entries();

    if entries >= max_entries {
        // 节点满了，需要先分裂节点
        let leaf_at = path.nodes.len() - 1;
        writer.split_extent_node(
            inode_ref,
            sb,
            allocator,
            &mut path,
            leaf_at,
            logical_block,
        )?;

        // 重新查找路径（树结构已改变）
        path = writer.find_extent_path(inode_ref, logical_block)?;

        let new_leaf = path.leaf().ok_or_else(|| {
            Error::new(ErrorKind::Corrupted, "Extent path has no leaf after split")
        })?;

        // 在新的叶子节点中执行分裂
        return split_extent_in_leaf(
            inode_ref,
            new_leaf.block_addr,
            new_leaf.node_type,
            sb.block_size(),
            logical_block,
            split_flag,
        );
    }

    // 6. 节点有空间，直接执行分裂
    split_extent_in_leaf(
        inode_ref,
        leaf_block_addr,
        leaf_node_type,
        sb.block_size(),
        logical_block,
        split_flag,
    )
}

/// 在叶子节点中查找包含指定逻辑块的 extent
///
/// # 返回
///
/// (extent_index, (ee_block, ee_len, ee_start, was_unwritten))
fn find_extent_in_leaf<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    block_addr: u64,
    node_type: ExtentNodeType,
    block_size: u32,
    logical_block: u32,
) -> Result<(usize, (u32, u16, u64, bool))> {
    let extents = if node_type == ExtentNodeType::Root {
        let (extents, _) = read_extents_from_inode(inode_ref)?;
        extents
    } else {
        let (extents, _) = read_extents_from_block(
            inode_ref.bdev(),
            block_addr,
            block_size,
        )?;
        extents
    };

    // 查找包含 logical_block 的 extent
    for (i, extent) in extents.iter().enumerate() {
        let ee_block = extent.logical_block();
        let ee_len = get_actual_len(extent);
        let ee_end = ee_block + ee_len as u32 - 1;

        if logical_block >= ee_block && logical_block <= ee_end {
            let ee_start = get_pblock(extent);
            let was_unwritten = is_unwritten(extent);
            return Ok((i, (ee_block, ee_len, ee_start, was_unwritten)));
        }
    }

    Err(Error::new(
        ErrorKind::NotFound,
        "Extent not found in leaf node",
    ))
}

/// 改变 extent 的状态（不分裂）
fn change_extent_status<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    block_addr: u64,
    node_type: ExtentNodeType,
    _block_size: u32,
    extent_idx: usize,
    split_flag: u32,
) -> Result<()> {
    if node_type == ExtentNodeType::Root {
        inode_ref.with_inode_mut(|inode| -> Result<()> {
            let data = inode.extent_root_data_mut();

            let header_size = core::mem::size_of::<ext4_extent_header>();
            let extent_size = core::mem::size_of::<ext4_extent>();
            let offset = header_size + extent_idx * extent_size;

            let extent = unsafe {
                &mut *(data.as_mut_ptr().add(offset) as *mut ext4_extent)
            };

            if split_flag & EXT4_EXT_MARK_UNWRIT2 != 0 {
                mark_unwritten(extent);
            } else {
                mark_initialized(extent);
            }

            Ok(())
        })??;

        inode_ref.mark_dirty()?;
    } else {
        let mut block = Block::get(inode_ref.bdev(), block_addr)?;

        block.with_data_mut(|data| -> Result<()> {
            let header_size = core::mem::size_of::<ext4_extent_header>();
            let extent_size = core::mem::size_of::<ext4_extent>();
            let offset = header_size + extent_idx * extent_size;

            let extent = unsafe {
                &mut *(data.as_mut_ptr().add(offset) as *mut ext4_extent)
            };

            if split_flag & EXT4_EXT_MARK_UNWRIT2 != 0 {
                mark_unwritten(extent);
            } else {
                mark_initialized(extent);
            }

            Ok(())
        })??;
    }

    Ok(())
}

/// 在叶子节点中分裂 extent
fn split_extent_in_leaf<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    block_addr: u64,
    node_type: ExtentNodeType,
    block_size: u32,
    split_at: u32,
    split_flag: u32,
) -> Result<()> {
    // 读取 extent 数组
    let (mut extents, header) = if node_type == ExtentNodeType::Root {
        read_extents_from_inode(inode_ref)?
    } else {
        read_extents_from_block(inode_ref.bdev(), block_addr, block_size)?
    };

    // 找到要分裂的 extent
    let extent_idx = extents
        .iter()
        .position(|e| {
            let ee_block = e.logical_block();
            let ee_len = get_actual_len(e);
            split_at >= ee_block && split_at < ee_block + ee_len as u32
        })
        .ok_or_else(|| {
            Error::new(ErrorKind::NotFound, "Extent to split not found")
        })?;

    let extent = extents[extent_idx];
    let ee_block = extent.logical_block();
    let ee_len = get_actual_len(&extent);
    let ee_start = get_pblock(&extent);

    // 分裂 extent
    let first_len = split_at - ee_block;
    let second_len = ee_len as u32 - first_len;

    // 修改原 extent（第一部分）
    let mut first_extent = extent;
    first_extent.len = (first_len as u16).to_le();

    if split_flag & EXT4_EXT_MARK_UNWRIT1 != 0 {
        mark_unwritten(&mut first_extent);
    } else {
        mark_initialized(&mut first_extent);
    }

    extents[extent_idx] = first_extent;

    // 创建新 extent（第二部分）
    let mut second_extent = ext4_extent {
        block: split_at.to_le(),
        len: (second_len as u16).to_le(),
        ..ext4_extent::default()
    };
    store_pblock(&mut second_extent, ee_start + first_len as u64);

    if split_flag & EXT4_EXT_MARK_UNWRIT2 != 0 {
        mark_unwritten(&mut second_extent);
    } else {
        mark_initialized(&mut second_extent);
    }

    // 插入新 extent（在原 extent 之后）
    extents.insert(extent_idx + 1, second_extent);

    // 更新 header
    let mut new_header = header;
    new_header.entries = (extents.len() as u16).to_le();

    // 写回
    if node_type == ExtentNodeType::Root {
        write_extents_to_inode(inode_ref, &new_header, &extents)?;
    } else {
        write_extents_to_block(
            inode_ref.bdev(),
            block_addr,
            block_size,
            &new_header,
            &extents,
        )?;
    }

    Ok(())
}

/// 转换 unwritten extent 为 initialized（多层树版本）
///
/// # 参数
///
/// * `writer` - ExtentWriter
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `allocator` - 块分配器
/// * `logical_block` - 逻辑块号
/// * `count` - 要转换的块数量
///
/// # 返回
///
/// 成功返回实际转换的块数
pub fn convert_to_initialized_multilevel<D: BlockDevice>(
    writer: &mut ExtentWriter<D>,
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    logical_block: u32,
    count: u32,
) -> Result<u32> {
    let mut converted = 0u32;
    let mut current_block = logical_block;
    let end_block = logical_block + count;

    while current_block < end_block {
        // 查找当前块所在的 extent
        let path = writer.find_extent_path(inode_ref, current_block)?;

        let leaf = path.leaf().ok_or_else(|| {
            Error::new(ErrorKind::Corrupted, "No leaf node found")
        })?;

        // 查找 extent
        let (extent_idx, (ee_block, ee_len, _ee_start, was_unwritten)) =
            find_extent_in_leaf(
                inode_ref,
                leaf.block_addr,
                leaf.node_type,
                sb.block_size(),
                current_block,
            )?;

        if !was_unwritten {
            // 已经是 initialized，跳过
            current_block = ee_block + ee_len as u32;
            continue;
        }

        let ee_end = ee_block + ee_len as u32 - 1;
        let convert_start = current_block.max(ee_block);
        let convert_end = end_block.min(ee_end + 1);
        let convert_count = convert_end - convert_start;

        // 处理三种情况：
        // 1. 转换整个 extent
        // 2. 转换开头部分（需要分裂）
        // 3. 转换中间部分（需要两次分裂）
        // 4. 转换结尾部分（需要分裂）

        if convert_start == ee_block && convert_end == ee_end + 1 {
            // 情况 1: 转换整个 extent
            change_extent_status(
                inode_ref,
                leaf.block_addr,
                leaf.node_type,
                sb.block_size(),
                extent_idx,
                0, // mark as initialized
            )?;

            converted += ee_len as u32;
            current_block = ee_end + 1;
        } else if convert_start == ee_block {
            // 情况 2: 转换开头部分
            split_extent_at_multilevel(
                writer,
                inode_ref,
                sb,
                allocator,
                convert_end,
                EXT4_EXT_MARK_UNWRIT2, // 第二部分保持 unwritten
            )?;

            // 然后标记第一部分为 initialized
            let new_path = writer.find_extent_path(inode_ref, convert_start)?;
            let new_leaf = new_path.leaf().unwrap();
            let (new_idx, _) = find_extent_in_leaf(
                inode_ref,
                new_leaf.block_addr,
                new_leaf.node_type,
                sb.block_size(),
                convert_start,
            )?;

            change_extent_status(
                inode_ref,
                new_leaf.block_addr,
                new_leaf.node_type,
                sb.block_size(),
                new_idx,
                0, // initialized
            )?;

            converted += convert_count;
            current_block = convert_end;
        } else if convert_end == ee_end + 1 {
            // 情况 4: 转换结尾部分
            split_extent_at_multilevel(
                writer,
                inode_ref,
                sb,
                allocator,
                convert_start,
                EXT4_EXT_MARK_UNWRIT1, // 第一部分保持 unwritten
            )?;

            // 然后标记第二部分为 initialized
            let new_path = writer.find_extent_path(inode_ref, convert_start)?;
            let new_leaf = new_path.leaf().unwrap();
            let (new_idx, _) = find_extent_in_leaf(
                inode_ref,
                new_leaf.block_addr,
                new_leaf.node_type,
                sb.block_size(),
                convert_start,
            )?;

            change_extent_status(
                inode_ref,
                new_leaf.block_addr,
                new_leaf.node_type,
                sb.block_size(),
                new_idx,
                0, // initialized
            )?;

            converted += convert_count;
            current_block = convert_end;
        } else {
            // 情况 3: 转换中间部分（需要两次分裂）
            // 第一次分裂：在 convert_start 处
            split_extent_at_multilevel(
                writer,
                inode_ref,
                sb,
                allocator,
                convert_start,
                EXT4_EXT_MARK_UNWRIT1 | EXT4_EXT_MARK_UNWRIT2,
            )?;

            // 第二次分裂：在 convert_end 处
            split_extent_at_multilevel(
                writer,
                inode_ref,
                sb,
                allocator,
                convert_end,
                EXT4_EXT_MARK_UNWRIT2,
            )?;

            // 标记中间部分为 initialized
            let new_path = writer.find_extent_path(inode_ref, convert_start)?;
            let new_leaf = new_path.leaf().unwrap();
            let (new_idx, _) = find_extent_in_leaf(
                inode_ref,
                new_leaf.block_addr,
                new_leaf.node_type,
                sb.block_size(),
                convert_start,
            )?;

            change_extent_status(
                inode_ref,
                new_leaf.block_addr,
                new_leaf.node_type,
                sb.block_size(),
                new_idx,
                0, // initialized
            )?;

            converted += convert_count;
            current_block = convert_end;
        }
    }

    Ok(converted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multilevel_split_api() {
        // 需要完整的文件系统环境进行测试
        // 主要验证 API 编译通过
    }
}
