//! Extent 树解析和块映射

use crate::{
    block::{Block, BlockDev, BlockDevice},
    error::{Error, ErrorKind, Result},
    inode::Inode,
    types::{ext4_extent, ext4_extent_header, ext4_extent_idx, ext4_inode},
};
use log::*;

/// Extent 树遍历器
///
/// 用于解析 inode 中的 extent 树并将逻辑块号映射到物理块号
pub struct ExtentTree<'a, D: BlockDevice> {
    bdev: &'a mut BlockDev<D>,
    block_size: u32,
    device_total_blocks: u64,
}

impl<'a, D: BlockDevice> ExtentTree<'a, D> {
    /// 创建新的 extent 树遍历器
    pub fn new(bdev: &'a mut BlockDev<D>, block_size: u32) -> Self {
        let device_total_blocks = bdev.total_blocks();
        Self {
            bdev,
            block_size,
            device_total_blocks,
        }
    }


    /// 将逻辑块号映射到物理块号（内部实现，在 with_inode 闭包内使用）
    ///
    /// # 参数
    ///
    /// * `inode` - ext4_inode 引用（通常从 InodeRef::with_inode 闭包获得）
    /// * `logical_block` - 逻辑块号
    ///
    /// # 返回
    ///
    /// 成功返回物理块号，如果找不到对应的 extent 返回 None
    ///
    /// # 使用场景
    ///
    /// 此方法设计为在 `InodeRef::with_inode` 闭包内使用，保证数据一致性：
    /// ```rust,ignore
    /// inode_ref.with_inode(|inode| {
    ///     extent_tree.map_block_internal(inode, logical_block)
    /// })?
    /// ```
    pub(crate) fn map_block_internal(&mut self, inode: &ext4_inode, logical_block: u32) -> Result<Option<u64>> {
        // 检查 inode 是否使用 extent（检查 flags）
        let flags = u32::from_le(inode.flags);
        if flags & 0x80000 == 0 {  // EXT4_EXTENTS_FL
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Inode does not use extents",
            ));
        }

        // extent 树根节点位于 inode 的 blocks 数组中
        let root_data = inode.extent_root_data();

        // 解析根节点的 extent header
        let header: ext4_extent_header = crate::bytes::read_struct(root_data)?;

        if !header.is_valid() {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Invalid extent header magic",
            ));
        }

        // 从根节点开始查找
        self.find_extent_in_node(root_data, &header, logical_block)
    }

    /// 在给定的节点中查找 extent
    fn find_extent_in_node(
        &mut self,
        node_data: &[u8],
        header: &ext4_extent_header,
        logical_block: u32,
    ) -> Result<Option<u64>> {
        if header.is_leaf() {
            // 叶子节点：包含实际的 extent
            self.search_leaf_node(node_data, header, logical_block)
        } else {
            // 索引节点：包含指向下层节点的索引
            self.search_index_node(node_data, header, logical_block)
        }
    }

    /// 在叶子节点中搜索 extent
    fn search_leaf_node(
        &mut self,
        node_data: &[u8],
        header: &ext4_extent_header,
        logical_block: u32,
    ) -> Result<Option<u64>> {
        let entries = header.entries_count() as usize;
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let extent_size = core::mem::size_of::<ext4_extent>();

        for i in 0..entries {
            let offset = header_size + i * extent_size;
            if offset + extent_size > node_data.len() {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Extent node data too short",
                ));
            }

            let extent: ext4_extent = crate::bytes::read_struct(&node_data[offset..])?;

            let extent_start = extent.logical_block();
            let extent_len = extent.actual_len() as u32;
            let extent_end = extent_start + extent_len;

            // 检查逻辑块是否在这个 extent 范围内
            if logical_block >= extent_start && logical_block < extent_end {
                let offset_in_extent = logical_block - extent_start;
                let extent_physical_base = extent.physical_block();
                let physical_block = extent_physical_base + offset_in_extent as u64;

                // 读取原始字段值用于日志
                let start_lo = u32::from_le(extent.start_lo);
                let start_hi = u16::from_le(extent.start_hi);

                // 记录详细日志
                info!(
                    "[EXTENT READ] logical={}, found in extent[{}]: range=[{}-{}], \
                     physical_base={:#x}, physical_result={:#x}, start_hi={:#x}, start_lo={:#x}",
                    logical_block, i, extent_start, extent_end - 1,
                    extent_physical_base, physical_block, start_hi, start_lo
                );

                // 🔧 边界检查：验证物理块号是否在设备范围内
                if physical_block >= self.device_total_blocks {
                    error!(
                        "[EXTENT READ] Physical block OUT OF BOUNDS! \
                         physical={:#x}, device_total={}, extent_base={:#x}, \
                         start_hi={:#x}, start_lo={:#x}, offset_in_extent={}",
                        physical_block, self.device_total_blocks,
                        extent_physical_base, start_hi, start_lo, offset_in_extent
                    );
                    return Err(Error::new(
                        ErrorKind::Corrupted,
                        "Physical block address exceeds device size",
                    ));
                }

                return Ok(Some(physical_block));
            }
        }

        Ok(None)
    }

    /// 在索引节点中搜索
    fn search_index_node(
        &mut self,
        node_data: &[u8],
        header: &ext4_extent_header,
        logical_block: u32,
    ) -> Result<Option<u64>> {
        let entries = header.entries_count() as usize;
        let header_size = core::mem::size_of::<ext4_extent_header>();
        let idx_size = core::mem::size_of::<ext4_extent_idx>();

        // 找到应该包含目标逻辑块的索引
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

            // 索引按逻辑块号排序
            // 找到第一个 logical_block >= idx_block 的索引
            if logical_block >= idx_block {
                target_idx = Some(idx);
            } else {
                break;
            }
        }

        if let Some(idx) = target_idx {
            // 读取子节点
            let child_block = idx.leaf_block();
            let mut block = Block::get(self.bdev, child_block)?;

            // 复制子节点数据到独立的缓冲区
            let child_data = block.with_data(|data| {
                let mut buf = alloc::vec![0u8; data.len()];
                buf.copy_from_slice(data);
                buf
            })?;

            // 释放 block，这样我们就可以递归调用了
            drop(block);

            // 解析子节点的头部
            let child_header: ext4_extent_header = crate::bytes::read_struct(&child_data)?;

            if !child_header.is_valid() {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Invalid extent header in child node",
                ));
            }

            // 递归查找
            self.find_extent_in_node(&child_data, &child_header, logical_block)
        } else {
            Ok(None)
        }
    }

    /// 将逻辑块号映射到物理块号
    ///
    /// # 参数
    ///
    /// * `inode` - inode 引用
    /// * `logical_block` - 逻辑块号
    ///
    /// # 返回
    ///
    /// 成功返回物理块号，如果找不到对应的 extent 返回 None
    ///
    /// # 数据一致性说明
    ///
    /// 此方法接受 `Inode` 包装类型，内部会访问其 `ext4_inode` 数据。
    /// 在单线程场景下安全使用。在需要保证数据一致性的场景，
    /// 应在 `InodeRef::with_inode` 闭包内使用 `map_block_internal`。
    pub fn map_block(&mut self, inode: &Inode, logical_block: u32) -> Result<Option<u64>> {
        self.map_block_internal(inode.inner(), logical_block)
    }

    /// 读取文件的某个逻辑块
    ///
    /// # 参数
    ///
    /// * `inode` - ext4_inode 引用
    /// * `logical_block` - 逻辑块号
    /// * `buf` - 输出缓冲区（大小应该等于块大小）
    pub(crate) fn read_block(
        &mut self,
        inode: &ext4_inode,
        logical_block: u32,
        buf: &mut [u8],
    ) -> Result<()> {
        if buf.len() < self.block_size as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Buffer too small for block",
            ));
        }

        match self.map_block_internal(inode, logical_block)? {
            Some(physical_block) => {
                let mut block = Block::get(self.bdev, physical_block)?;
                block.with_data(|data| {
                    buf[..self.block_size as usize].copy_from_slice(data);
                    Ok(())
                })?
            }
            None => Err(Error::new(
                ErrorKind::NotFound,
                "Logical block not found in extent tree",
            )),
        }
    }


    /// 读取文件内容（内部实现，在 with_inode 闭包内使用）
    ///
    /// # 参数
    ///
    /// * `inode` - ext4_inode 引用（通常从 InodeRef::with_inode 闭包获得）
    /// * `offset` - 文件内偏移（字节）
    /// * `buf` - 输出缓冲区
    ///
    /// # 返回
    ///
    /// 实际读取的字节数
    ///
    /// # 使用场景
    ///
    /// 此方法设计为在 `InodeRef::with_inode` 闭包内使用，保证数据一致性。
    pub(crate) fn read_file_internal(
        &mut self,
        inode: &ext4_inode,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize> {
        // 计算文件大小
        let file_size = {
            let size_lo = u32::from_le(inode.size_lo) as u64;
            let size_hi = u32::from_le(inode.size_hi) as u64;
            size_lo | (size_hi << 32)
        };

        // 检查偏移是否超出文件大小
        if offset >= file_size {
            return Ok(0);
        }

        // 计算实际可以读取的字节数
        let remaining = file_size - offset;
        let to_read = core::cmp::min(buf.len() as u64, remaining) as usize;

        let block_size = self.block_size as u64;
        let mut bytes_read = 0;

        while bytes_read < to_read {
            let current_offset = offset + bytes_read as u64;
            let block_num = (current_offset / block_size) as u32;
            let block_offset = (current_offset % block_size) as usize;

            // 计算本次读取的字节数
            let bytes_in_block = core::cmp::min(
                block_size as usize - block_offset,
                to_read - bytes_read,
            );

            // 读取块
            let mut block_buf = alloc::vec![0u8; block_size as usize];
            self.read_block(inode, block_num, &mut block_buf)?;

            // 复制数据到输出缓冲区
            buf[bytes_read..bytes_read + bytes_in_block]
                .copy_from_slice(&block_buf[block_offset..block_offset + bytes_in_block]);

            bytes_read += bytes_in_block;
        }

        Ok(bytes_read)
    }

    /// 读取文件内容
    ///
    /// # 参数
    ///
    /// * `inode` - inode 引用
    /// * `offset` - 文件内偏移（字节）
    /// * `buf` - 输出缓冲区
    ///
    /// # 返回
    ///
    /// 实际读取的字节数
    ///
    /// # 数据一致性说明
    ///
    /// 此方法接受 `Inode` 包装类型。在需要保证数据一致性的场景，
    /// 应在 `InodeRef::with_inode` 闭包内使用 `read_file_internal`。
    pub fn read_file(
        &mut self,
        inode: &Inode,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize> {
        self.read_file_internal(inode.inner(), offset, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ext4_extent_header;

    #[test]
    fn test_extent_header_validation() {
        let mut header = ext4_extent_header::default();
        assert!(!header.is_valid());

        header.magic = 0xF30Au16.to_le();
        assert!(header.is_valid());
    }

    #[test]
    fn test_extent_header_depth() {
        let mut header = ext4_extent_header::default();
        header.magic = 0xF30Au16.to_le();
        header.depth = 0u16.to_le();
        assert!(header.is_leaf());

        header.depth = 1u16.to_le();
        assert!(!header.is_leaf());
    }

    #[test]
    fn test_extent_physical_block() {
        let mut extent = ext4_extent::default();
        extent.start_lo = 0x12345678u32.to_le();
        extent.start_hi = 0xABCDu16.to_le();

        let physical = extent.physical_block();
        assert_eq!(physical, 0x0000ABCD12345678u64);
    }
}
