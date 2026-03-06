//! Extent 节点操作原语

use crate::{
    block::{Block, BlockDevice},
    error::{Error, ErrorKind, Result},
    types::{ext4_extent, ext4_extent_header},
};

/// 向 extent 节点数据（header + sorted extent array）中有序插入一个 extent
///
/// 这是所有 extent 插入操作的统一原语。操作 `&mut [u8]` 格式的节点数据，
/// 包含一个 ext4_extent_header 后跟有序的 ext4_extent 数组。
///
/// # 参数
///
/// * `data` - 节点数据（header + extent array）
/// * `logical_block` - 新 extent 的逻辑块号
/// * `physical_block` - 新 extent 的物理块号
/// * `length` - 新 extent 的长度（块数）
/// * `try_merge` - 是否尝试与相邻 extent 合并（前后合并、桥接合并）
///
/// # 返回
///
/// 成功返回 `Ok(())`，节点满时返回 `NoSpace`，重复插入返回 `InvalidInput`
pub(crate) fn insert_extent_into_node(
    data: &mut [u8],
    logical_block: u32,
    physical_block: u64,
    length: u32,
    try_merge: bool,
) -> Result<()> {
    let header_size = core::mem::size_of::<ext4_extent_header>();
    let extent_size = core::mem::size_of::<ext4_extent>();

    // 解析 header
    let header: ext4_extent_header = crate::bytes::read_struct(data)?;

    if !header.is_valid() {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "Invalid extent header in node",
        ));
    }

    let entries_count = header.entries_count();
    let max_entries = header.max_entries();

    // 找到插入位置（保持排序），并收集合并信息
    let mut insert_pos = entries_count as usize;
    let mut merge_prev: Option<(usize, u16)> = None; // (pos, prev_len)
    let mut merge_next: Option<(usize, u16)> = None; // (pos, next_len)

    for i in 0..entries_count as usize {
        let offset = header_size + i * extent_size;
        let existing: ext4_extent = crate::bytes::read_struct(&data[offset..])?;

        let existing_block = existing.logical_block();
        let existing_len = existing.len();
        let existing_physical = existing.physical_block();

        // 检查重复
        if existing_block == logical_block {
            log::error!(
                "[EXTENT_INSERT] DUPLICATE DETECTED: logical_block={logical_block} already exists at pos {i}, \
                 existing_physical=0x{existing_physical:x}, new_physical=0x{physical_block:x}"
            );
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Extent for this logical block already exists (duplicate insert prevented)",
            ));
        }

        // 检查是否可以与前一个 extent 合并
        if try_merge
            && existing_block + existing_len as u32 == logical_block
            && existing_physical + existing_len as u64 == physical_block
        {
            merge_prev = Some((i, existing_len));
        }

        if existing_block > logical_block {
            insert_pos = i;

            // 检查是否可以与后一个 extent 合并
            if try_merge
                && logical_block + length == existing_block
                && physical_block + length as u64 == existing_physical
            {
                merge_next = Some((i, existing_len));
            }
            break;
        }
    }

    // 执行合并操作
    if try_merge {
        match (merge_prev, merge_next) {
            (Some((prev_idx, prev_len)), Some((next_idx, next_len))) => {
                // 桥接合并：prev + new + next → prev
                let new_total_len = (prev_len as u32 + length + next_len as u32) as u16;
                let prev_offset = header_size + prev_idx * extent_size;

                // 扩展 prev extent
                let mut prev_ext: ext4_extent = crate::bytes::read_struct(&data[prev_offset..])?;
                prev_ext.len = new_total_len.to_le();
                let prev_bytes = crate::bytes::as_bytes(&prev_ext);
                data[prev_offset..prev_offset + extent_size].copy_from_slice(prev_bytes);

                // 删除 next extent（向前移动后续 extents）
                if next_idx + 1 < entries_count as usize {
                    let src_offset = header_size + (next_idx + 1) * extent_size;
                    let dst_offset = header_size + next_idx * extent_size;
                    let move_count = (entries_count as usize - next_idx - 1) * extent_size;
                    data.copy_within(src_offset..src_offset + move_count, dst_offset);
                }

                // 更新 header（entries - 1）
                let h = unsafe { &mut *(data.as_mut_ptr() as *mut ext4_extent_header) };
                h.entries = (entries_count - 1).to_le();

                log::info!(
                    "[EXTENT_MERGE] BRIDGE MERGE: prev_pos={prev_idx}, next_pos={next_idx}, total_len={new_total_len}"
                );
                return Ok(());
            }
            (Some((prev_idx, prev_len)), None) => {
                // 与前一个 extent 合并
                let new_len = (prev_len as u32 + length) as u16;
                let prev_offset = header_size + prev_idx * extent_size;

                let mut prev_ext: ext4_extent = crate::bytes::read_struct(&data[prev_offset..])?;
                prev_ext.len = new_len.to_le();
                let prev_bytes = crate::bytes::as_bytes(&prev_ext);
                data[prev_offset..prev_offset + extent_size].copy_from_slice(prev_bytes);

                log::info!(
                    "[EXTENT_MERGE] PREV MERGE: pos={prev_idx}, extended_len={prev_len} -> {new_len}"
                );
                return Ok(());
            }
            (None, Some((next_idx, next_len))) => {
                // 与后一个 extent 合并
                let new_len = (length + next_len as u32) as u16;
                let next_offset = header_size + next_idx * extent_size;

                let mut next_ext: ext4_extent = crate::bytes::read_struct(&data[next_offset..])?;
                next_ext.block = logical_block.to_le();
                next_ext.start_lo = (physical_block as u32).to_le();
                next_ext.start_hi = ((physical_block >> 32) as u16).to_le();
                next_ext.len = new_len.to_le();
                let next_bytes = crate::bytes::as_bytes(&next_ext);
                data[next_offset..next_offset + extent_size].copy_from_slice(next_bytes);

                log::info!(
                    "[EXTENT_MERGE] NEXT MERGE: pos={next_idx}, extended_len={next_len} -> {new_len}"
                );
                return Ok(());
            }
            (None, None) => {
                // 无法合并，继续插入
            }
        }
    }

    // 检查空间（合并路径不需要新空间，仅纯插入需要）
    if entries_count >= max_entries {
        return Err(Error::new(
            ErrorKind::NoSpace,
            "Extent node is full",
        ));
    }

    // 移动后面的 extent 为新 extent 腾出空间
    if insert_pos < entries_count as usize {
        let src_offset = header_size + insert_pos * extent_size;
        let dst_offset = header_size + (insert_pos + 1) * extent_size;
        let move_count = (entries_count as usize - insert_pos) * extent_size;
        data.copy_within(src_offset..src_offset + move_count, dst_offset);
    }

    // 写入新 extent
    let new_extent = ext4_extent {
        block: logical_block.to_le(),
        len: (length as u16).to_le(),
        start_lo: (physical_block as u32).to_le(),
        start_hi: ((physical_block >> 32) as u16).to_le(),
    };
    let new_extent_offset = header_size + insert_pos * extent_size;
    let ext_bytes = crate::bytes::as_bytes(&new_extent);
    data[new_extent_offset..new_extent_offset + extent_size].copy_from_slice(ext_bytes);

    // 更新 header
    let h = unsafe { &mut *(data.as_mut_ptr() as *mut ext4_extent_header) };
    h.entries = (entries_count + 1).to_le();

    log::debug!(
        "[EXTENT_INSERT] Inserted at pos {insert_pos}: logical={logical_block}, physical=0x{physical_block:x}, len={length}, entries {} -> {}",
        entries_count, entries_count + 1
    );

    Ok(())
}

/// 尝试插入 extent 到叶子块（不处理分裂）
///
/// 这是一个辅助函数，仅执行插入操作。如果块满，返回 NoSpace 错误。
pub(crate) fn try_insert_to_leaf_block<D: BlockDevice>(
    bdev: &mut crate::block::BlockDev<D>,
    leaf_block: u64,
    logical_block: u32,
    physical_block: u64,
    length: u32,
) -> Result<()> {
    let mut block = Block::get(bdev, leaf_block)?;
    block.with_data_mut(|data| {
        insert_extent_into_node(data, logical_block, physical_block, length, true)
    })??;

    Ok(())
}
