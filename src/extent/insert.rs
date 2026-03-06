//! Extent 插入操作

use crate::{
    balloc::BlockAllocator,
    block::{Block, BlockDevice},
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_extent, ext4_extent_header, ext4_extent_idx},
};

use super::node_ops::{insert_extent_into_node, try_insert_to_leaf_block};
use super::path::{ExtentPath, ExtentPathNode, ExtentNodeType};

pub(crate) fn insert_extent_with_auto_split<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    logical_block: u32,
    physical_block: u64,
    length: u32,
) -> Result<()> {
    // 1. 检查根节点是否满
    let (is_full, depth, entries, max) = inode_ref.with_inode(|inode| -> (bool, u16, u16, u16) {
        let header = inode.extent_header();

        let entries = u16::from_le(header.entries);
        let max = u16::from_le(header.max);
        let depth = u16::from_le(header.depth);

        (entries >= max, depth, entries, max)
    })?;

    log::debug!(
        "[EXTENT_INSERT] logical={logical_block}, physical=0x{physical_block:x}, len={length}, is_full={is_full}, depth={depth}, entries={entries}/{max}"
    );

    // 2. 根据当前状态决定插入策略
    if is_full {
        // 根节点满了，需要增加树深度
        log::debug!("[EXTENT_INSERT] Root is FULL, calling grow_tree_depth (depth {} -> {})", depth, depth + 1);
        let new_block = super::grow_tree_depth(inode_ref, sb, allocator)?;

        // 关键修复：grow 后需要根据新深度确定如何插入
        // - 如果原 depth = 0，grow 后 depth = 1，new_block 是叶子节点（depth=0）
        // - 如果原 depth >= 1，grow 后 depth >= 2，new_block 是索引节点，需要继续遍历
        let new_depth = depth + 1;

        // 根据新深度确定目标叶子块
        let leaf_block: u64 = (match new_depth {
            1 => {
                // depth 0->1: new_block 就是叶子节点
                log::debug!("[EXTENT_INSERT] After grow (0->1), new_block 0x{new_block:x} is leaf");
                Ok(new_block)
            }
            2 => {
                // depth 1->2: new_block 是索引节点，读取其第一个 index 指向的叶子块
                log::debug!("[EXTENT_INSERT] After grow (1->2), new_block 0x{new_block:x} is index node");

                Block::get(inode_ref.bdev(), new_block)
                    .and_then(|mut idx_block| {
                        // with_data 返回 Result<Result<u64>>, 需要展开外层并返回内层
                        match idx_block.with_data(|data| -> Result<u64> {
                            let header = unsafe {
                                *(data.as_ptr() as *const ext4_extent_header)
                            };

                            let depth_check = u16::from_le(header.depth);
                            if depth_check != 1 {
                                log::error!("[EXTENT_INSERT] Expected depth=1 in new index block, got {depth_check}");
                                return Err(Error::new(
                                    ErrorKind::Corrupted,
                                    "Expected depth=1 in new index block after grow",
                                ));
                            }

                            // 读取第一个 index
                            let header_size = core::mem::size_of::<ext4_extent_header>();
                            let idx_ptr = unsafe {
                                data.as_ptr().add(header_size) as *const ext4_extent_idx
                            };
                            let idx = unsafe { &*idx_ptr };

                            let leaf = super::helpers::ext4_idx_pblock(idx);
                            log::debug!("[EXTENT_INSERT] Read first index from 0x{new_block:x}: leaf=0x{leaf:x}");
                            Ok(leaf)
                        }) {
                            Ok(inner_result) => inner_result,  // 返回内层 Result<u64>
                            Err(e) => Err(e),
                        }
                    })
            }
            _ => {
                // TODO: depth > 2 需要递归遍历索引树找到叶子节点
                // 当前实现仅支持 depth <= 2，这在绝大多数情况下足够
                // (depth=2 可支持约 340*340*32K = 3.7TB 文件)
                log::error!("[EXTENT_INSERT] Tree depth {new_depth} not supported after grow");
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "Tree depth > 2 not supported after grow",
                ))
            }
        })?;

        log::debug!("[EXTENT_INSERT] After grow, inserting to leaf block 0x{leaf_block:x}");
        insert_extent_to_leaf_direct(inode_ref, sb, allocator, leaf_block, logical_block, physical_block, length)?;
    } else if depth == 0 {
        // 深度为 0 且未满，直接插入到根节点（inode.blocks）
        log::debug!("[EXTENT_INSERT] Depth=0 and not full, using insert_extent_simple");
        let extent = ext4_extent {
            block: logical_block.to_le(),
            len: (length as u16).to_le(),
            start_hi: ((physical_block >> 32) as u16).to_le(),
            start_lo: (physical_block as u32).to_le(),
        };

        insert_extent_simple(inode_ref, &extent)?;
    } else {
        // 深度 > 0 且未满，需要插入到叶子节点
        log::debug!("[EXTENT_INSERT] Depth={depth} and not full, inserting to leaf");

        // 🔧 关键修复：根据 logical_block 查找正确的目标叶子块
        // 不能使用 read_first_leaf_block，因为它总是返回第一个索引
        // 必须遍历索引树找到包含 logical_block 的正确叶子
        let leaf_block = find_target_leaf_block(inode_ref, logical_block)?;
        log::debug!("[EXTENT_INSERT] Found target leaf block for logical={logical_block}: 0x{leaf_block:x}");

        insert_extent_to_leaf_direct(inode_ref, sb, allocator, leaf_block, logical_block, physical_block, length)?;
    }

    Ok(())
}

/// 根据 logical_block 查找目标叶子块
///
/// 🔧 关键修复：遍历索引树，找到应该包含 logical_block 的叶子块
/// 不同于 read_first_leaf_block（总是返回第一个索引），这个函数会：
/// 1. 读取根节点的所有索引
/// 2. 选择最后一个 logical_block >= idx.block 的索引
/// 3. 递归遍历直到找到叶子节点
fn find_target_leaf_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    logical_block: u32,
) -> Result<u64> {
    // 读取根节点
    let (current_block, root_depth) = inode_ref.with_inode_mut(|inode| -> Result<(u64, u16)> {
        let header = inode.extent_header();

        let depth = u16::from_le(header.depth);
        if depth == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "find_target_leaf_block called on depth-0 tree",
            ));
        }

        let entries = u16::from_le(header.entries);
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let idx_size = core::mem::size_of::<ext4_extent_idx>();

        // 遍历索引，找到最后一个 logical_block >= idx.block 的
        let mut target_idx: Option<ext4_extent_idx> = None;

        for i in 0..entries as usize {
            let offset = header_size + i * idx_size;
            let idx = unsafe {
                *(inode.blocks.as_ptr().add(offset / 4) as *const ext4_extent_idx)
            };

            let idx_block = u32::from_le(idx.block);

            log::debug!(
                "[FIND_TARGET_LEAF] Index[{i}]: idx_block={idx_block}, comparing with logical={logical_block}"
            );

            if logical_block >= idx_block {
                target_idx = Some(idx);
            } else {
                break;
            }
        }

        let idx = target_idx.ok_or_else(|| {
            Error::new(ErrorKind::NotFound, "No suitable index found for logical block")
        })?;

        let child_block = super::helpers::ext4_idx_pblock(&idx);

        log::debug!(
            "[FIND_TARGET_LEAF] Selected child_block=0x{child_block:x} for logical={logical_block}"
        );

        Ok((child_block, depth))
    })??;

    // 递归遍历直到找到叶子节点（depth=0）
    let leaf_block = traverse_to_leaf(inode_ref, current_block, root_depth - 1, logical_block)?;

    Ok(leaf_block)
}

/// 递归遍历索引树直到找到叶子节点
fn traverse_to_leaf<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    mut current_block: u64,
    mut current_depth: u16,
    logical_block: u32,
) -> Result<u64> {
    while current_depth > 0 {
        let mut block = crate::block::Block::get(inode_ref.bdev(), current_block)?;

        let child_block = block.with_data(|data| {
            let header = unsafe {
                &*(data.as_ptr() as *const crate::types::ext4_extent_header)
            };

            if !header.is_valid() {
                return Err(crate::error::Error::new(
                    ErrorKind::Corrupted,
                    "Invalid extent header in index node",
                ));
            }

            let node_depth = u16::from_le(header.depth);
            let entries = u16::from_le(header.entries);

            log::debug!(
                "[TRAVERSE_LEAF] At block=0x{current_block:x}, depth={node_depth}, entries={entries}, searching for logical={logical_block}"
            );

            // 遍历索引，找到最后一个 logical_block >= idx.block 的
            let header_size = core::mem::size_of::<crate::types::ext4_extent_header>();
            let idx_size = core::mem::size_of::<crate::types::ext4_extent_idx>();

            let mut target_idx: Option<crate::types::ext4_extent_idx> = None;

            for i in 0..entries as usize {
                let offset = header_size + i * idx_size;
                let idx = unsafe {
                    *(data[offset..].as_ptr() as *const crate::types::ext4_extent_idx)
                };

                let idx_block = u32::from_le(idx.block);

                if logical_block >= idx_block {
                    target_idx = Some(idx);
                } else {
                    break;
                }
            }

            let idx = target_idx.ok_or_else(|| {
                Error::new(ErrorKind::NotFound, "No suitable index in intermediate node")
            })?;

            let child = super::helpers::ext4_idx_pblock(&idx);

            log::debug!("[TRAVERSE_LEAF] Selected child=0x{child:x}");

            Ok(child)
        })??;

        current_block = child_block;
        current_depth -= 1;
    }

    log::debug!("[TRAVERSE_LEAF] Found leaf block: 0x{current_block:x}");
    Ok(current_block)
}

/// 读取 inode 中第一个索引的 leaf_block
///
/// ⚠️ 注意：这个函数总是返回第一个索引，不考虑 logical_block！
/// 对于需要根据 logical_block 选择叶子的情况，应该使用 find_target_leaf_block
///
/// 注意：使用 with_inode_mut 而非 with_inode 来读取，确保能看到最新的修改
/// 即使我们不修改 inode，使用 mut 访问也能保证读到最新的 Block 缓存数据
fn read_first_leaf_block<D: BlockDevice>(inode_ref: &mut InodeRef<D>) -> Result<u64> {
    // 使用 with_inode_mut 而不是 with_inode 来读取
    // 这确保了我们能读到 grow_tree_depth 中 with_inode_mut 的最新修改
    let (mut current_block, root_depth) = inode_ref.with_inode_mut(|inode| -> Result<(u64, u16)> {
        // 读取 extent header
        let header = inode.extent_header();

        let depth = u16::from_le(header.depth);
        if depth == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "read_first_leaf_block called on depth-0 tree",
            ));
        }

        // 读取第一个索引
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let idx_ptr = unsafe {
            // 关键修复：inode.blocks 是 [u32; 15]，需要先转为 *const u8 再按字节偏移
            (inode.blocks.as_ptr() as *const u8).add(header_size) as *const ext4_extent_idx
        };
        let idx = unsafe { &*idx_ptr };

        // 使用辅助函数而不是手动组合
        let child_block = super::helpers::ext4_idx_pblock(idx);

        log::debug!(
            "[READ_LEAF_BLOCK] root_depth={depth}, first_child=0x{child_block:x}"
        );

        Ok((child_block, depth))
    })??;

    // 🔧 BUG FIX: 递归遍历extent树直到找到真正的leaf节点（depth=0）
    // 对于depth >= 2的树，root的第一个索引指向的是另一个index节点，不是leaf节点
    let mut current_depth = root_depth - 1; // child节点的深度

    while current_depth > 0 {
        // 读取当前index节点的第一个索引
        let _block_size = inode_ref.bdev().block_size();
        let mut block = crate::block::Block::get(inode_ref.bdev(), current_block)?;

        let child_block = block.with_data(|data| {
            let header = unsafe {
                &*(data.as_ptr() as *const crate::types::ext4_extent_header)
            };

            if !header.is_valid() {
                return Err(crate::error::Error::new(
                    ErrorKind::Corrupted,
                    "Invalid extent header in index node",
                ));
            }

            let node_depth = u16::from_le(header.depth);
            if node_depth != current_depth {
                log::warn!(
                    "[READ_LEAF_BLOCK] Depth mismatch: expected={current_depth}, actual={node_depth}"
                );
            }

            // 读取第一个索引
            let header_size = core::mem::size_of::<crate::types::ext4_extent_header>();
            let idx = unsafe {
                &*(data.as_ptr().add(header_size) as *const crate::types::ext4_extent_idx)
            };

            let child = super::helpers::ext4_idx_pblock(idx);

            log::debug!(
                "[READ_LEAF_BLOCK] Traversing: block=0x{current_block:x}, depth={current_depth} -> child=0x{child:x}"
            );

            Ok(child)
        })??;

        current_block = child_block;
        current_depth -= 1;
    }

    log::debug!("[READ_LEAF_BLOCK] Found leaf block: 0x{current_block:x}");
    Ok(current_block)
}

/// 直接插入 extent 到指定的叶子块（支持分裂）
///
/// 这个函数直接使用给定的 leaf_block，而不是从 inode 读取索引。
/// 当叶子满时，会自动构建路径并执行分裂操作。
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `allocator` - 块分配器（用于分裂时分配新块）
/// * `leaf_block` - 叶子块地址
/// * `logical_block` - 要插入的逻辑块号
/// * `physical_block` - 要插入的物理块号
/// * `length` - extent 长度
fn insert_extent_to_leaf_direct<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    leaf_block: u64,
    logical_block: u32,
    physical_block: u64,
    length: u32,
) -> Result<()> {
    log::debug!(
        "[EXTENT_LEAF_DIRECT] Inserting to leaf block 0x{leaf_block:x}: logical={logical_block}, physical=0x{physical_block:x}, len={length}"
    );

    // 首先尝试直接插入
    let insert_result = try_insert_to_leaf_block(
        inode_ref.bdev(),
        leaf_block,
        logical_block,
        physical_block,
        length,
    );

    match insert_result {
        Ok(()) => {
            log::debug!("[EXTENT_LEAF_DIRECT] Insert succeeded without split");
            Ok(())
        }
        Err(e) if e.kind() == ErrorKind::NoSpace => {
            log::debug!("[EXTENT_LEAF_DIRECT] Leaf is full, need to split");

            // 构建 ExtentPath 用于分裂
            let mut path = build_extent_path_for_leaf(inode_ref, leaf_block)?;

            // 执行分裂（在 path 的最后一个节点，即叶子节点）
            let leaf_at = path.nodes.len() - 1;
            log::debug!(
                "[EXTENT_LEAF_DIRECT] Calling split_extent_node at depth={}, leaf_at={}",
                path.nodes[leaf_at].depth, leaf_at
            );

            super::split_extent_node(
                inode_ref,
                sb,
                allocator,
                &mut path,
                leaf_at,
                logical_block,
            )?;

            log::debug!("[EXTENT_LEAF_DIRECT] Split succeeded, retrying insert");

            // 分裂后，需要重新确定应该插入到哪个叶子节点
            // 可能是原来的 leaf_block，也可能是新分裂出来的块
            let new_leaf_block = determine_target_leaf_after_split(
                inode_ref,
                &path,
                logical_block,
            )?;

            log::debug!(
                "[EXTENT_LEAF_DIRECT] Target leaf after split: 0x{new_leaf_block:x}"
            );

            // 重试插入（分裂后必定有空间）
            try_insert_to_leaf_block(
                inode_ref.bdev(),
                new_leaf_block,
                logical_block,
                physical_block,
                length,
            )?;

            log::debug!("[EXTENT_LEAF_DIRECT] Retry insert succeeded");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// 构建从根到指定叶子块的 ExtentPath
///
/// 用于分裂操作前构建路径信息
fn build_extent_path_for_leaf<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    leaf_block: u64,
) -> Result<ExtentPath> {
    // 读取根节点信息
    let (root_header, max_depth) = inode_ref.with_inode(|inode| {
        let header = inode.extent_header();
        let depth = u16::from_le(header.depth);
        (*header, depth)
    })?;

    let mut path = ExtentPath::new(max_depth);

    // 添加根节点
    path.push(ExtentPathNode {
        block_addr: 0, // 根节点在 inode 中
        depth: max_depth,
        header: root_header,
        index_pos: 0,
        node_type: ExtentNodeType::Root,
    });

    // 如果深度为 0，根节点就是叶子节点
    if max_depth == 0 {
        return Ok(path);
    }

    // 对于深度 > 0，需要从根节点遍历到目标叶子节点
    // 支持任意深度的索引树
    if max_depth > 0 {
        let mut current_block = leaf_block;
        let mut current_depth = max_depth;

        // 从根节点开始，逐层向下查找，直到叶子节点
        // 注意：我们已知目标叶子块地址，需要构建到达它的路径
        // 这里采用简化策略：直接读取每一层的节点
        while current_depth > 0 {
            // 读取当前层的节点 header
            let mut block = Block::get(inode_ref.bdev(), current_block)?;
            let node_header = block.with_data(|data| {

                unsafe {
                    *(data.as_ptr() as *const ext4_extent_header)
                }
            })?;

            // 验证深度一致性
            let node_depth = u16::from_le(node_header.depth);
            if node_depth != current_depth - 1 {
                log::warn!(
                    "[BUILD_PATH] Depth mismatch: expected {}, got {} at block 0x{:x}",
                    current_depth - 1, node_depth, current_block
                );
            }

            // 添加节点到路径
            let node_type = if current_depth == 1 {
                ExtentNodeType::Leaf
            } else {
                ExtentNodeType::Index
            };

            path.push(ExtentPathNode {
                block_addr: current_block,
                depth: node_depth,
                header: node_header,
                index_pos: 0,
                node_type,
            });

            log::debug!(
                "[BUILD_PATH] Added node: depth={node_depth}, block=0x{current_block:x}, type={node_type:?}"
            );

            // 如果是叶子节点，完成路径构建
            if current_depth == 1 {
                break;
            }

            // 否则，读取第一个索引项，继续向下遍历
            // 注意：这里的假设是我们要找的叶子块在某个索引项下
            // 实际上，由于我们已知 leaf_block，应该在索引中查找它
            // 但为了简化，我们暂时读取第一个索引
            // TODO: 改进为在索引中查找匹配的 leaf_block
            let first_idx = block.with_data(|data| {
                let header_size = core::mem::size_of::<ext4_extent_header>();
                unsafe {
                    *(data[header_size..].as_ptr() as *const ext4_extent_idx)
                }
            })?;

            current_block = super::helpers::ext4_idx_pblock(&first_idx);
            current_depth -= 1;

            log::debug!(
                "[BUILD_PATH] Moving to next level: block=0x{current_block:x}, depth={current_depth}"
            );
        }

        return Ok(path);
    }

    // 深度为 0 时，根节点就是叶子，已在前面处理
    Ok(path)
}

/// 分裂后确定目标叶子块
///
/// 根据 logical_block，决定应该插入到原叶子还是新分裂的叶子
fn determine_target_leaf_after_split<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    path: &ExtentPath,
    logical_block: u32,
) -> Result<u64> {
    // 对于深度 1 的简单情况，从根节点的索引中找到目标叶子
    let depth = path.nodes[0].header.depth();

    log::debug!(
        "[DETERMINE_TARGET] Starting: depth={depth}, logical_block={logical_block}"
    );

    // 支持任意深度的树
    // 从根节点开始，逐层查找覆盖 logical_block 的索引项
    let mut current_depth = depth;
    let mut current_block: Option<u64> = None;

    // 第一层：从根节点（inode）读取索引
    if current_depth > 0 {
        let (indices, _) = super::split::read_indices_from_inode(inode_ref)?;

        log::debug!(
            "[DETERMINE_TARGET] Level {}: Read {} indices from inode",
            current_depth, indices.len()
        );

        // 找到最后一个 first_block <= logical_block 的索引
        let mut target_idx: Option<&ext4_extent_idx> = None;
        for (i, idx) in indices.iter().enumerate() {
            let idx_block = u32::from_le(idx.block);
            let next_block = super::helpers::ext4_idx_pblock(idx);

            log::debug!(
                "[DETERMINE_TARGET] Index {i}: idx_block={idx_block}, next_block=0x{next_block:x}"
            );

            if logical_block >= idx_block {
                target_idx = Some(idx);
            } else {
                break;
            }
        }

        if let Some(idx) = target_idx {
            current_block = Some(super::helpers::ext4_idx_pblock(idx));
            current_depth -= 1;
        } else {
            log::error!("[DETERMINE_TARGET] No matching index found in root!");
            return Err(Error::new(
                ErrorKind::Corrupted,
                "No matching index found in root after split",
            ));
        }
    }

    // 后续层：从索引块读取索引，直到达到叶子层
    while current_depth > 0 {
        if let Some(block_addr) = current_block {
            // 读取索引块
            let block_size = inode_ref.superblock().block_size();
            let (indices, _) = super::split::read_indices_from_block(
                inode_ref.bdev(),
                block_addr,
                block_size,
            )?;

            log::debug!(
                "[DETERMINE_TARGET] Level {}: Read {} indices from block 0x{:x}",
                current_depth, indices.len(), block_addr
            );

            // 找到覆盖 logical_block 的索引
            let mut target_idx: Option<&ext4_extent_idx> = None;
            for (i, idx) in indices.iter().enumerate() {
                let idx_block = u32::from_le(idx.block);
                let next_block = super::helpers::ext4_idx_pblock(idx);

                log::debug!(
                    "[DETERMINE_TARGET] Index {i}: idx_block={idx_block}, next_block=0x{next_block:x}"
                );

                if logical_block >= idx_block {
                    target_idx = Some(idx);
                } else {
                    break;
                }
            }

            if let Some(idx) = target_idx {
                current_block = Some(super::helpers::ext4_idx_pblock(idx));
                current_depth -= 1;
            } else {
                log::error!("[DETERMINE_TARGET] No matching index found at depth {current_depth}!");
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "No matching index found in index block after split",
                ));
            }
        } else {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Invalid block address in determine_target_leaf_after_split",
            ));
        }
    }

    // 此时 current_block 应该指向目标叶子块
    if let Some(leaf_block) = current_block {
        log::debug!(
            "[DETERMINE_TARGET] Final target: leaf_block=0x{leaf_block:x}"
        );
        Ok(leaf_block)
    } else {
        Err(Error::new(
            ErrorKind::Corrupted,
            "No leaf block found after traversing index tree",
        ))
    }
}

/// 简单插入 extent（仅支持深度 0 的树）
///
/// 这是一个简化的 extent 插入实现，仅支持在 inode 的根节点（深度=0）中插入 extent。
/// 不支持节点分裂和 extent 合并。
pub(crate) fn insert_extent_simple<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    extent: &ext4_extent,
) -> Result<()> {
    inode_ref.with_inode_mut(|inode| {
        let header = *inode.extent_header();
        if header.depth() != 0 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "insert_extent_simple only supports depth=0 trees",
            ));
        }

        let data = inode.extent_root_data_mut();
        insert_extent_into_node(
            data,
            extent.logical_block(),
            extent.physical_block(),
            extent.len() as u32,
            false, // 不合并
        )
    })??;

    inode_ref.mark_dirty()?;
    Ok(())
}
