//! Extent 查找操作

use crate::{
    block::{Block, BlockDevice},
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    types::{ext4_extent, ext4_extent_header, ext4_extent_idx},
};
use alloc::vec::Vec;

/// 查找包含指定逻辑块的 extent
///
/// 内部辅助函数，用于在 extent 树中查找包含指定逻辑块的 extent
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `logical_block` - 要查找的逻辑块号
///
/// # 返回
///
/// * `Some(extent)` - 找到包含此逻辑块的 extent
/// * `None` - 未找到
pub(crate) fn find_extent_for_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    logical_block: u32,
) -> Result<Option<ext4_extent>> {
    // 读取 inode 中的 extent 树根节点
    let root_data = inode_ref.with_inode(|inode| {
        let root_data = inode.extent_root_data().to_vec();
        root_data
    })?;

    // 解析根节点 header
    let header = unsafe {
        *(root_data.as_ptr() as *const ext4_extent_header)
    };

    let depth = u16::from_le(header.depth);
    let entries = u16::from_le(header.entries);
    let max = u16::from_le(header.max);

    log::debug!(
        "[FIND_EXTENT] Searching for logical={}, root: depth={}, entries={}/{}, inode.blocks[0..28]={:02x?}",
        logical_block, depth, entries, max, &root_data[..28]
    );

    // 根据深度选择查找方式
    if depth == 0 {
        // 叶子节点：直接在根节点中查找
        let result = find_extent_in_leaf(&root_data, logical_block)?;
        log::debug!(
            "[FIND_EXTENT] depth=0, result={:?}",
            result.as_ref().map(|e| (u32::from_le(e.block), u16::from_le(e.len)))
        );
        return Ok(result);
    }

    // 多层树：需要遍历索引节点
    let result = find_extent_in_multilevel_tree(inode_ref, &root_data, &header, logical_block)?;
    log::debug!(
        "[FIND_EXTENT] depth={}, result={:?}",
        depth,
        result.as_ref().map(|e| (u32::from_le(e.block), u16::from_le(e.len)))
    );
    Ok(result)
}

/// 在多层 extent 树中查找 extent
///
/// 递归遍历索引节点，直到找到包含目标逻辑块的叶子节点
fn find_extent_in_multilevel_tree<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    node_data: &[u8],
    header: &ext4_extent_header,
    logical_block: u32,
) -> Result<Option<ext4_extent>> {
    let depth = u16::from_le(header.depth);
    let entries = u16::from_le(header.entries);

    log::debug!(
        "[FIND_EXTENT_MULTI] depth={depth}, entries={entries}, searching for logical={logical_block}"
    );

    // 如果已经是叶子节点，直接查找
    if header.is_leaf() {
        log::debug!("[FIND_EXTENT_MULTI] Node is leaf, searching in leaf");
        return find_extent_in_leaf(node_data, logical_block);
    }

    // 索引节点：查找指向目标块的索引
    let header_size = core::mem::size_of::<ext4_extent_header>();
    let idx_size = core::mem::size_of::<ext4_extent_idx>();

    let mut target_idx: Option<(ext4_extent_idx, usize)> = None;

    for i in 0..entries as usize {
        let offset = header_size + i * idx_size;
        if offset + idx_size > node_data.len() {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Extent index node data too short",
            ));
        }

        let idx: ext4_extent_idx = crate::bytes::read_struct(&node_data[offset..])?;

        let idx_block = u32::from_le(idx.block);
        let leaf_lo = u32::from_le(idx.leaf_lo);
        let leaf_hi = u16::from_le(idx.leaf_hi);
        let child_block = (leaf_hi as u64) << 32 | (leaf_lo as u64);

        log::debug!(
            "[FIND_EXTENT_MULTI] Index[{i}]: idx_block={idx_block}, child_block=0x{child_block:x}"
        );

        // 找到最后一个 logical_block >= idx.block 的索引
        if logical_block >= idx_block {
            target_idx = Some((idx, i));
        } else {
            break;
        }
    }

    if let Some((idx, idx_num)) = target_idx {
        // 读取子节点
        let child_block = {
            let leaf_lo = u32::from_le(idx.leaf_lo);
            let leaf_hi = u16::from_le(idx.leaf_hi);
            (leaf_hi as u64) << 32 | (leaf_lo as u64)
        };

        log::debug!(
            "[FIND_EXTENT_MULTI] Selected index[{idx_num}], reading child block 0x{child_block:x}"
        );

        let mut block = Block::get(inode_ref.bdev(), child_block)?;

        // 复制子节点数据
        let child_data = block.with_data(|data| {
            let mut buf = Vec::with_capacity(data.len());
            buf.extend_from_slice(data);
            buf
        })?;

        drop(block);

        // 解析子节点 header
        let child_header = unsafe {
            *(child_data.as_ptr() as *const ext4_extent_header)
        };

        if !child_header.is_valid() {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Invalid extent header in child node",
            ));
        }

        let child_depth = u16::from_le(child_header.depth);
        let child_entries = u16::from_le(child_header.entries);
        log::debug!(
            "[FIND_EXTENT_MULTI] Child node: depth={child_depth}, entries={child_entries}"
        );

        // 递归查找
        find_extent_in_multilevel_tree(inode_ref, &child_data, &child_header, logical_block)
    } else {
        log::debug!("[FIND_EXTENT_MULTI] No suitable index found, returning None");
        Ok(None)
    }
}

/// 在叶子节点中查找 extent
fn find_extent_in_leaf(node_data: &[u8], logical_block: u32) -> Result<Option<ext4_extent>> {
    let header = unsafe { *(node_data.as_ptr() as *const ext4_extent_header) };
    let entries = u16::from_le(header.entries);

    log::debug!(
        "[FIND_EXTENT_LEAF] Searching in leaf: entries={entries}, logical={logical_block}"
    );

    let header_size = core::mem::size_of::<ext4_extent_header>();
    let extent_size = core::mem::size_of::<ext4_extent>();

    for i in 0..entries as usize {
        let offset = header_size + i * extent_size;
        if offset + extent_size > node_data.len() {
            break;
        }

        let extent = unsafe {
            *(node_data.as_ptr().add(offset) as *const ext4_extent)
        };

        let ee_block = u32::from_le(extent.block);
        let ee_len = u16::from_le(extent.len);

        // 检查逻辑块是否在这个 extent 范围内
        if logical_block >= ee_block && logical_block < ee_block + ee_len as u32 {
            log::debug!(
                "[FIND_EXTENT_LEAF] Found at entry[{}]: range=[{}-{}], physical=0x{:x}",
                i, ee_block, ee_block + ee_len as u32 - 1,
                crate::extent::helpers::ext4_ext_pblock(&extent)
            );
            return Ok(Some(extent));
        }
    }

    log::debug!("[FIND_EXTENT_LEAF] Not found in leaf");
    Ok(None)
}
