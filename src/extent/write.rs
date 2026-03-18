//! Extent 树写操作
//!
//! 对应 lwext4 的 ext4_extent.c 中的写操作部分
//!
//! ## 功能
//!
//! - ✅ Extent 树初始化 (`tree_init`)
//! - ✅ Extent 块获取/分配 (`get_blocks`)
//! - ✅ Extent 移除 (`remove_space`)
//! - ✅ `ExtentWriter` 高级写入器（支持事务）
//!
//! ## 子模块
//!
//! - `insert` — 插入操作（自动分裂/增长）
//! - `lookup` — extent 查找
//! - `node_ops` — 节点操作原语
//! - `path` — 路径类型定义

use crate::{
    balloc::{self, BlockAllocator},
    block::BlockDevice,
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    transaction::SimpleTransaction,
    types::{ext4_extent, ext4_extent_header, ext4_extent_idx},
};
use log::*;
use alloc::vec::Vec;

use super::insert::{insert_extent_with_auto_split, insert_extent_simple};
use super::lookup::find_extent_for_block;
use super::node_ops::insert_extent_into_node;
use super::path::{ExtentPath, ExtentPathNode, ExtentNodeType};

//=============================================================================
// Extent 树初始化
//=============================================================================

/// 初始化 extent 树
///
/// 对应 lwext4 的 `ext4_extent_tree_init()`
///
/// 在 inode 中初始化一个空的 extent 树，用于新创建的文件。
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
///
/// # 实现细节
///
/// 1. 获取 inode 中的 extent header（在 inode.blocks 数组中）
/// 2. 设置 header 的各个字段：
///    - depth = 0（根节点即叶子）
///    - entries_count = 0（空树）
///    - generation = 0
///    - magic = 0xF30A
/// 3. 计算 max_entries（基于 inode.blocks 的大小）
/// 4. 标记 inode 为 dirty
///
/// # 示例
///
/// ```rust,ignore
/// use lwext4_core::extent::tree_init;
///
/// // 为新创建的 inode 初始化 extent 树
/// tree_init(&mut inode_ref)?;
/// ```
pub fn tree_init<D: BlockDevice>(inode_ref: &mut InodeRef<D>) -> Result<()> {
    // Extent 树魔数
    const EXT4_EXTENT_MAGIC: u16 = 0xF30A;

    // 在 inode 中直接修改 extent header
    inode_ref.with_inode_mut(|inode| {
        // inode.blocks 是 15 个 u32，总共 60 字节
        // 前面是 ext4_extent_header，后面是 extent 或 extent_idx 数组
        let header = inode.extent_header_mut();

        // 设置 header 字段
        header.depth = 0u16.to_le();       // 根节点即叶子
        header.entries = 0u16.to_le();     // 空树
        header.max = 0u16.to_le();         // 稍后计算
        header.magic = EXT4_EXTENT_MAGIC.to_le(); // 0xF30A
        header.generation = 0u32.to_le();

        // 计算 max_entries
        // inode.blocks 是 60 字节，减去 header (12 字节)，剩下可以存放 extent
        // 每个 ext4_extent 是 12 字节
        const INODE_BLOCKS_SIZE: usize = 60; // 15 * 4
        const HEADER_SIZE: usize = core::mem::size_of::<ext4_extent_header>();
        const EXTENT_SIZE: usize = core::mem::size_of::<ext4_extent>();

        let max_entries = (INODE_BLOCKS_SIZE - HEADER_SIZE) / EXTENT_SIZE;
        header.max = (max_entries as u16).to_le();
    })?;

    // 标记 inode 为 dirty
    inode_ref.mark_dirty()?;

    Ok(())
}

//=============================================================================
// Extent 块获取和分配
//=============================================================================

/// 查找下一个已分配的逻辑块
///
/// 对应 lwext4 的 `ext4_ext_next_allocated_block()`
///
/// 用于确定可以分配多少块而不会覆盖已有的 extent。
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `logical_block` - 当前逻辑块号
///
/// # 返回
///
/// 下一个已分配的逻辑块号，如果没有则返回 u32::MAX
/// TODO: 这个函数只处理了深度为0的情况，对于多层树总是返回 u32::MAX
/// 虽然不会导致功能错误，但会影响连续分配的优化效果
fn find_next_allocated_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    logical_block: u32,
) -> Result<u32> {
    // 读取 extent 树根节点
    let (root_data, depth) = inode_ref.with_inode(|inode| {
        let root_data = inode.extent_root_data().to_vec();

        let header = unsafe {
            *(root_data.as_ptr() as *const ext4_extent_header)
        };

        (root_data, u16::from_le(header.depth))
    })?;

    // 如果深度为 0，直接在根节点查找
    if depth == 0 {
        let header = unsafe { *(root_data.as_ptr() as *const ext4_extent_header) };
        let entries = u16::from_le(header.entries);
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let extent_size = core::mem::size_of::<ext4_extent>();

        let mut next_block = u32::MAX;

        for i in 0..entries as usize {
            let offset = header_size + i * extent_size;
            if offset + extent_size > root_data.len() {
                break;
            }

            let extent = unsafe {
                *(root_data.as_ptr().add(offset) as *const ext4_extent)
            };

            let ee_block = u32::from_le(extent.block);

            // 找到第一个大于 logical_block 的 extent
            if ee_block > logical_block && ee_block < next_block {
                next_block = ee_block;
            }
        }

        return Ok(next_block);
    }

    // TODO: 支持多层树
    Ok(u32::MAX)
}

/// 计算块分配目标
///
/// 对应 lwext4 的 `ext4_ext_find_goal()`
///
/// 根据当前文件的 extent 分布，智能选择一个物理块作为分配目标，作为该inode该逻辑块对应的物理块
/// 这有助于减少文件碎片化。
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `logical_block` - 要分配的逻辑块号
/// * `cached_extent_opt` - 可选的已查找的extent结果（性能优化）
///
/// # 返回
///
/// 建议的物理块起始地址（goal）
///
/// # 策略
///
/// 1. 如果存在相邻的 extent，尝试在其后继续分配
/// 2. 否则，使用 inode 所在块组的默认位置
///
/// # 性能优化
///
/// 如果调用者已经调用过 `find_extent_for_block`，可以通过 `cached_extent_opt` 传入结果，
/// 避免重复查找，提升性能。
fn find_goal<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    logical_block: u32,
    cached_extent_opt: Option<Option<ext4_extent>>,
) -> Result<u64> {
    //  性能优化：使用缓存的查找结果，或者执行新查找
    let extent_opt = if let Some(cached) = cached_extent_opt {
        cached
    } else {
        find_extent_for_block(inode_ref, logical_block)?
    };

    if let Some(extent) = extent_opt {
        let ee_block = u32::from_le(extent.block);
        let ee_start_lo = u32::from_le(extent.start_lo);
        let ee_start_hi = u16::from_le(extent.start_hi);
        let ee_start = (ee_start_hi as u64) << 32 | (ee_start_lo as u64);

        // 如果逻辑块在当前 extent 之后，预测物理块也应该在其后
        if logical_block > ee_block {
            return Ok(ee_start + (logical_block - ee_block) as u64);
        } else {
            // 如果在之前，尝试在其前面分配（反向写）
            return Ok(ee_start.saturating_sub((ee_block - logical_block) as u64));
        }
    }

    // 如果没有找到相邻 extent，使用 inode 所在块组的默认位置
    // 这是最保守的 fallback 策略
    Ok(0) // 0 表示让 balloc 自己选择
}

/// 获取或分配物理块
///
/// 对应 lwext4 的 `ext4_extent_get_blocks()`
///
/// 给定逻辑块号，返回对应的物理块号。如果逻辑块尚未映射，
/// 根据 `create` 参数决定是否分配新的物理块。
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `logical_block` - 逻辑块号
/// * `max_blocks` - 最多获取/分配的块数
/// * `create` - 如果为 true，在块不存在时分配新块
///
/// # 返回
///
/// * `Ok((physical_block, allocated_count))` - 物理块号和实际分配的块数
///   - 如果 `physical_block` 为 0，表示块不存在且未创建
/// * `Err(_)` - 发生错误
///
/// # 示例
///
/// ```rust,ignore
/// // 查找逻辑块 100 对应的物理块
/// let (phys_block, count) = get_blocks(&mut inode_ref, 100, 1, false)?;
/// if phys_block == 0 {
///     println!("Block not allocated");
/// }
///
/// // 分配新块
/// let (phys_block, count) = get_blocks(&mut inode_ref, 100, 10, true)?;
/// ```
pub fn get_blocks<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    allocator: &mut BlockAllocator,
    logical_block: u32,
    max_blocks: u32,
    create: bool,
) -> Result<(u64, u32)> {
    // 1. 查找包含此逻辑块的 extent
    let extent_opt = find_extent_for_block(inode_ref, logical_block)?;

    if let Some(extent) = extent_opt {
        // 提取 extent 信息
        let ee_block = u32::from_le(extent.block);
        let ee_len = u16::from_le(extent.len);
        let ee_start_lo = u32::from_le(extent.start_lo);
        let ee_start_hi = u16::from_le(extent.start_hi);

        // 计算物理块起始地址
        let ee_start = (ee_start_hi as u64) << 32 | (ee_start_lo as u64);

        // 检查逻辑块是否在这个 extent 范围内
        if logical_block >= ee_block && logical_block < ee_block + ee_len as u32 {
            // 计算物理块号
            let offset = logical_block - ee_block;
            let physical_block = ee_start + offset as u64;

            // 计算剩余块数
            let remaining = ee_len as u32 - offset;
            let allocated = remaining.min(max_blocks);

            return Ok((physical_block, allocated));
        }
    }

    // 2. 没有找到包含此逻辑块的 extent
    if !create {
        // 不创建，返回 0
        return Ok((0, 0));
    }

    // 3. 分配新块并使用 ExtentWriter 插入
    //
    // 与 lwext4 一致：使用完整的 extent 插入逻辑（支持自动 split/grow/merge）
    // 而不是简化版的 insert_extent_simple

    // 3.1 计算可以分配多少块（不能超过下一个已分配的 extent）
    // TODO: find_next_allocated_block 对多层树返回 u32::MAX，不影响正确性但影响连续分配优化
    let next_allocated = find_next_allocated_block(inode_ref, logical_block)?;
    let mut allocated_count = if next_allocated > logical_block {
        (next_allocated - logical_block).min(max_blocks)
    } else {
        max_blocks
    };

    // 3.2 计算分配目标（goal）
    // 🚀 性能优化：传入已经查找到的 extent_opt，避免在 find_goal 中重复查找
    let goal = find_goal(inode_ref, logical_block, Some(extent_opt))?;

    // 3.3 分配物理块（支持批量分配）
    let (physical_block, actual_allocated) = balloc::alloc_blocks(
        inode_ref.bdev(),
        sb,
        goal,
        allocated_count,
    )?;
    allocated_count = actual_allocated;

    // 🚀 性能优化：降低日志级别
    debug!(
        "[EXTENT WRITE] Allocated blocks: logical={logical_block}, physical={physical_block:#x}, count={actual_allocated}, goal={goal:#x}"
    );

    // 3.4 插入新 extent（支持自动 split/grow）
    // 与 lwext4 的 ext4_ext_insert_extent 行为一致
    //
    // 逻辑：
    // 1. 检查根节点是否满
    // 2. 如果满了，先 grow_tree_depth 增加树深度
    // 3. 然后插入extent (使用通用的 insert_extent_any_depth)

    let insert_result = insert_extent_with_auto_split(
        inode_ref,
        sb,
        allocator,
        logical_block,
        physical_block,
        allocated_count,
    );

    match insert_result {
        Ok(_) => {
            // 成功插入，更新 inode 的 blocks_count
            // 注意：blocks_count 以 512 字节扇区为单位
            inode_ref.add_blocks(allocated_count)?;

            // 🚀 性能优化：降低日志级别
            debug!(
                "[EXTENT WRITE] Successfully inserted extent: logical={}, physical={:#x} (hi={:#x}, lo={:#x}), count={}",
                logical_block, physical_block,
                (physical_block >> 32) as u16, physical_block as u32,
                allocated_count
            );
            Ok((physical_block, allocated_count))
        }
        Err(e) => {
            // 插入失败，释放已分配的块
            error!(
                "[EXTENT WRITE] Failed to insert extent: logical={logical_block}, physical={physical_block:#x}, error={e:?}"
            );
            let _ = balloc::free_blocks(
                inode_ref.bdev(),
                sb,
                physical_block,
                allocated_count,
            );
            Err(e)
        }
    }
}

//=============================================================================
// ExtentWriter（事务性写操作器）
//=============================================================================

/// Extent 写操作器
///
/// 提供 extent 树的修改操作
pub struct ExtentWriter<'a, D: BlockDevice> {
    trans: &'a mut SimpleTransaction<'a, D>,
    #[allow(dead_code)] // 供 ExtentWriter 扩展方法使用（如 split_node 等）
    block_size: u32,
}

impl<'a, D: BlockDevice> ExtentWriter<'a, D> {
    /// 创建新的 extent 写操作器
    pub fn new(trans: &'a mut SimpleTransaction<'a, D>, block_size: u32) -> Self {
        Self { trans, block_size }
    }

    /// 查找 extent 路径
    ///
    /// 从 inode 根节点开始，查找到包含指定逻辑块的叶子节点的路径
    ///
    /// 对应 lwext4 的 `ext4_find_extent`
    ///
    /// # 参数
    ///
    /// * `inode_ref` - Inode 引用
    /// * `logical_block` - 目标逻辑块号
    ///
    /// # 返回
    ///
    /// Extent 路径
    pub fn find_extent_path(
        &mut self,
        inode_ref: &mut InodeRef<D>,
        logical_block: u32,
    ) -> Result<ExtentPath> {
        // 读取 inode 中的 extent 根节点
        let root_data = inode_ref.with_inode(|inode| {
            let mut buf = alloc::vec![0u8; 60];
            buf.copy_from_slice(inode.extent_root_data());
            buf
        })?;

        // 解析根节点 header
        let root_header: ext4_extent_header = crate::bytes::read_struct(&root_data)?;

        if !root_header.is_valid() {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Invalid extent header in inode",
            ));
        }

        let max_depth = root_header.depth();
        let mut path = ExtentPath::new(max_depth);

        // 添加根节点到路径
        path.push(ExtentPathNode {
            block_addr: 0, // 根节点在 inode 中，没有独立块地址
            depth: max_depth,
            header: root_header,
            index_pos: 0,
            node_type: ExtentNodeType::Root,
        });

        // 如果根节点就是叶子，直接返回
        if root_header.is_leaf() {
            return Ok(path);
        }

        // 递归查找路径
        let mut current_data = root_data;
        let mut current_depth = max_depth;

        while current_depth > 0 {
            // 在当前索引节点中查找
            let next_block = self.find_index_in_node(&current_data, logical_block)?;

            // 读取子节点
            let mut child_block = self.trans.get_block(next_block)?;
            current_data = child_block.with_data(|data| {
                let mut buf = alloc::vec![0u8; data.len()];
                buf.copy_from_slice(data);
                buf
            })?;

            drop(child_block);

            // 解析子节点 header
            let child_header: ext4_extent_header = crate::bytes::read_struct(&current_data)?;

            if !child_header.is_valid() {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Invalid extent header in child node",
                ));
            }

            current_depth -= 1;

            let node_type = if child_header.is_leaf() {
                ExtentNodeType::Leaf
            } else {
                ExtentNodeType::Index
            };

            // 添加到路径
            path.push(ExtentPathNode {
                block_addr: next_block,
                depth: current_depth,
                header: child_header,
                index_pos: 0,
                node_type,
            });

            if child_header.is_leaf() {
                break;
            }
        }

        Ok(path)
    }

    /// 在索引节点中查找目标块
    fn find_index_in_node(&self, node_data: &[u8], logical_block: u32) -> Result<u64> {
        let header: ext4_extent_header = crate::bytes::read_struct(node_data)?;

        let entries = header.entries_count() as usize;
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let idx_size = core::mem::size_of::<ext4_extent_idx>();

        // 找到最后一个 logical_block >= idx.first_block 的索引
        let mut target_idx: Option<ext4_extent_idx> = None;

        for i in 0..entries {
            let offset = header_size + i * idx_size;
            if offset + idx_size > node_data.len() {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Extent index node data too short",
                ));
            }

            let idx: ext4_extent_idx = crate::bytes::read_struct(&node_data[offset..])?;

            let idx_block = idx.logical_block();

            if logical_block >= idx_block {
                target_idx = Some(idx);
            } else {
                break;
            }
        }

        if let Some(idx) = target_idx {
            Ok(idx.leaf_block())
        } else {
            Err(Error::new(
                ErrorKind::NotFound,
                "No matching index found",
            ))
        }
    }

    /// 插入新的 extent（支持自动分裂）
    ///
    /// 对应 lwext4 的 `ext4_ext_insert_extent`
    pub fn insert_extent(
        &mut self,
        inode_ref: &mut InodeRef<D>,
        sb: &mut crate::superblock::Superblock,
        allocator: &mut crate::balloc::BlockAllocator,
        logical_block: u32,
        physical_block: u64,
        length: u32,
    ) -> Result<()> {
        // 1. 查找路径到应该包含此 extent 的叶子节点
        let mut path = self.find_extent_path(inode_ref, logical_block)?;

        // 2. 获取叶子节点
        let leaf = path.leaf().ok_or_else(|| {
            Error::new(ErrorKind::Corrupted, "Extent path has no leaf node")
        })?;

        // 检查节点是否有空间
        let entries_count = leaf.header.entries_count();
        let max_entries = leaf.header.max_entries();

        if entries_count >= max_entries {
            // 节点满了，需要分裂
            if leaf.node_type == ExtentNodeType::Root {
                // 根节点满了，需要增加树深度
                self.grow_tree_depth(inode_ref, sb, allocator)?;

                // 重新查找路径（树结构已改变）
                path = self.find_extent_path(inode_ref, logical_block)?;
            } else {
                // 叶子节点满了，分裂它
                let leaf_at = path.nodes.len() - 1;
                self.split_extent_node(
                    inode_ref,
                    sb,
                    allocator,
                    &mut path,
                    leaf_at,
                    logical_block,
                )?;

                // 重新查找路径（树结构已改变）
                path = self.find_extent_path(inode_ref, logical_block)?;
            }
        }

        // 3. 尝试与现有 extent 合并（简化版本）
        // TODO: 实现完整的合并逻辑

        // 4. 重新获取叶子节点（可能已改变）
        let leaf = path.leaf().ok_or_else(|| {
            Error::new(ErrorKind::Corrupted, "Extent path has no leaf node after split")
        })?;

        // 5. 在 inode 或块中插入新 extent
        if leaf.node_type == ExtentNodeType::Root {
            // 插入到 inode 的 extent 根节点
            self.insert_extent_to_inode(inode_ref, logical_block, physical_block, length)?;
        } else {
            // 插入到独立的 extent 块
            self.insert_extent_to_block(
                leaf.block_addr,
                logical_block,
                physical_block,
                length,
            )?;
        }

        Ok(())
    }

    /// 插入 extent 到 inode 中的根节点
    fn insert_extent_to_inode(
        &mut self,
        inode_ref: &mut InodeRef<D>,
        logical_block: u32,
        physical_block: u64,
        length: u32,
    ) -> Result<()> {
        inode_ref.with_inode_mut(|inode| {
            let data = inode.extent_root_data_mut();
            insert_extent_into_node(data, logical_block, physical_block, length, false)
        })?
    }

    /// 插入 extent 到独立的 extent 块
    fn insert_extent_to_block(
        &mut self,
        block_addr: u64,
        logical_block: u32,
        physical_block: u64,
        length: u32,
    ) -> Result<()> {
        {
            let mut block = self.trans.get_block(block_addr)?;
            block.with_data_mut(|data| {
                insert_extent_into_node(data, logical_block, physical_block, length, false)
            })??;
        }

        self.trans.mark_dirty(block_addr)?;
        Ok(())
    }

    /// 分裂 extent 节点
    ///
    /// 对应 lwext4 的 `ext4_ext_split()`
    pub fn split_extent_node(
        &mut self,
        inode_ref: &mut InodeRef<D>,
        sb: &mut crate::superblock::Superblock,
        allocator: &mut crate::balloc::BlockAllocator,
        path: &mut ExtentPath,
        at: usize,
        logical_block: u32,
    ) -> Result<()> {
        crate::extent::split_extent_node(
            inode_ref,
            sb,
            allocator,
            path,
            at,
            logical_block,
        )
    }

    /// 增加 extent 树的深度
    ///
    /// 对应 lwext4 的 `ext4_ext_grow_indepth()`
    pub fn grow_tree_depth(
        &mut self,
        inode_ref: &mut InodeRef<D>,
        sb: &mut crate::superblock::Superblock,
        allocator: &mut crate::balloc::BlockAllocator,
    ) -> Result<u64> {
        crate::extent::grow_tree_depth(inode_ref, sb, allocator)
    }
}

//=============================================================================
// Extent 空间移除（删除/截断）
//=============================================================================

/// 移除 extent 空间（删除/截断文件）
///
/// 对应 lwext4 的 `ext4_extent_remove_space()`
///
/// 删除指定范围内的所有 extent，释放对应的物理块。
///
/// # 参数
///
/// * `inode_ref` - Inode 引用
/// * `sb` - Superblock 引用
/// * `from` - 起始逻辑块号
/// * `to` - 结束逻辑块号（包含）
///
/// # 示例
///
/// ```rust,ignore
/// // 删除逻辑块 10-19（共 10 个块）
/// remove_space(&mut inode_ref, &mut sb, 10, 19)?;
///
/// // 截断文件到 100 个块
/// remove_space(&mut inode_ref, &mut sb, 100, u32::MAX)?;
/// ```
pub fn remove_space<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    from: u32,
    to: u32,
) -> Result<()> {
    // 读取 extent 树深度
    let depth = inode_ref.with_inode(|inode| {
        let header = inode.extent_header();
        u16::from_le(header.depth)
    })?;

    let mut allocator = balloc::BlockAllocator::new();

    if depth == 0 {
        // 深度 0 使用优化的简化版本
        remove_space_simple(inode_ref, sb, from, to)?;
    } else {
        // 多层树使用完整实现
        crate::extent::remove_space_multilevel(
            inode_ref,
            sb,
            &mut allocator,
            from,
            to,
        )?;
    }

    Ok(())
}

/// 简单的空间移除（仅支持深度 0）
fn remove_space_simple<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    from: u32,
    to: u32,
) -> Result<()> {
    // 收集需要删除/修改的 extent 信息
    let modifications = inode_ref.with_inode(|inode| {
        let mut mods = Vec::new();
        let header = inode.extent_header();
        let entries = u16::from_le(header.entries);

        let header_size = core::mem::size_of::<ext4_extent_header>();
        let extent_size = core::mem::size_of::<ext4_extent>();

        // 遍历所有 extent，找出需要修改的
        for i in 0..entries as usize {
            let offset = header_size + i * extent_size;
            let extent = unsafe {
                *(inode.blocks.as_ptr().add(offset / 4) as *const ext4_extent)
            };

            let ee_block = u32::from_le(extent.block);
            let ee_len = u16::from_le(extent.len);
            let ee_end = ee_block + ee_len as u32 - 1;

            // 检查是否与删除范围重叠
            if ee_end < from || ee_block > to {
                continue;
            }

            let ee_start_lo = u32::from_le(extent.start_lo);
            let ee_start_hi = u16::from_le(extent.start_hi);
            let ee_start = (ee_start_hi as u64) << 32 | (ee_start_lo as u64);

            mods.push(ExtentModification {
                index: i,
                ee_block,
                ee_len: ee_len as u32,
                ee_start,
            });
        }

        mods
    })?;

    // 应用修改（从后往前，避免索引问题）
    for modification in modifications.iter().rev() {
        apply_extent_removal(
            inode_ref,
            sb,
            modification.index,
            modification.ee_block,
            modification.ee_len,
            modification.ee_start,
            from,
            to,
        )?;
    }

    Ok(())
}

/// Extent 修改信息
struct ExtentModification {
    index: usize,
    ee_block: u32,
    ee_len: u32,
    ee_start: u64,
}

/// 应用 extent 移除
fn apply_extent_removal<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    extent_idx: usize,
    ee_block: u32,
    ee_len: u32,
    ee_start: u64,
    from: u32,
    to: u32,
) -> Result<()> {
    let ee_end = ee_block + ee_len - 1;

    // 情况 1: 删除范围完全包含 extent
    if from <= ee_block && to >= ee_end {
        balloc::free_blocks(inode_ref.bdev(), sb, ee_start, ee_len)?;
        remove_extent_at_index(inode_ref, extent_idx)?;
    }
    // 情况 2: 删除范围在 extent 开头
    else if from <= ee_block && to < ee_end && to >= ee_block {
        let removed_len = to - ee_block + 1;
        let new_len = ee_len - removed_len;
        let new_block = to + 1;
        let new_start = ee_start + removed_len as u64;

        balloc::free_blocks(inode_ref.bdev(), sb, ee_start, removed_len)?;
        update_extent_at_index(inode_ref, extent_idx, new_block, new_len, new_start)?;
    }
    // 情况 3: 删除范围在 extent 结尾
    else if from > ee_block && to >= ee_end && from <= ee_end {
        let removed_len = ee_end - from + 1;
        let new_len = ee_len - removed_len;
        let removed_start = ee_start + (from - ee_block) as u64;

        balloc::free_blocks(inode_ref.bdev(), sb, removed_start, removed_len)?;
        update_extent_at_index(inode_ref, extent_idx, ee_block, new_len, ee_start)?;
    }
    // 情况 4: 删除范围在 extent 中间（需要分裂）
    else if from > ee_block && to < ee_end {
        let left_len = from - ee_block;
        let middle_len = to - from + 1;
        let right_len = ee_end - to;

        let middle_start = ee_start + left_len as u64;
        let right_block = to + 1;
        let right_start = ee_start + (left_len + middle_len) as u64;

        balloc::free_blocks(inode_ref.bdev(), sb, middle_start, middle_len)?;
        update_extent_at_index(inode_ref, extent_idx, ee_block, left_len, ee_start)?;

        let right_extent = ext4_extent {
            block: right_block.to_le(),
            len: (right_len as u16).to_le(),
            start_hi: ((right_start >> 32) as u16).to_le(),
            start_lo: (right_start as u32).to_le(),
        };

        insert_extent_simple(inode_ref, &right_extent)?;
    }

    Ok(())
}

/// 移除指定索引处的 extent
fn remove_extent_at_index<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    index: usize,
) -> Result<()> {
    inode_ref.with_inode_mut(|inode| {
        let entries = inode.extent_header().entries_count();
        if index >= entries as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid extent index in remove",
            ));
        }

        let header_size = core::mem::size_of::<ext4_extent_header>();
        let extent_size = core::mem::size_of::<ext4_extent>();

        // 移动后续 extent
        if index < entries as usize - 1 {
            let src_offset = header_size + (index + 1) * extent_size;
            let dst_offset = header_size + index * extent_size;
            let move_count = (entries as usize - index - 1) * extent_size;

            unsafe {
                let src = inode.blocks.as_ptr().add(src_offset / 4) as *const u8;
                let dst = inode.blocks.as_mut_ptr().add(dst_offset / 4) as *mut u8;
                core::ptr::copy(src, dst, move_count);
            }
        }

        // 更新 entries 计数
        inode.extent_header_mut().entries = (entries - 1).to_le();

        Ok(())
    })??;

    inode_ref.mark_dirty()?;
    Ok(())
}

/// 更新指定索引处的 extent
fn update_extent_at_index<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    index: usize,
    new_block: u32,
    new_len: u32,
    new_start: u64,
) -> Result<()> {
    inode_ref.with_inode_mut(|inode| {
        let header = inode.extent_header();

        let entries = u16::from_le(header.entries);
        if index >= entries as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid extent index in update",
            ));
        }

        let header_size = core::mem::size_of::<ext4_extent_header>();
        let extent_size = core::mem::size_of::<ext4_extent>();
        let offset = header_size + index * extent_size;

        let new_extent = ext4_extent {
            block: new_block.to_le(),
            len: (new_len as u16).to_le(),
            start_hi: ((new_start >> 32) as u16).to_le(),
            start_lo: (new_start as u32).to_le(),
        };

        unsafe {
            let dst = inode.blocks.as_mut_ptr().add(offset / 4) as *mut ext4_extent;
            core::ptr::write(dst, new_extent);
        }

        Ok(())
    })??;

    inode_ref.mark_dirty()?;
    Ok(())
}
