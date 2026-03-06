//! 间接块映射器实现
//!
//! 将文件的逻辑块号映射到物理块号，支持直接块和多级间接块。

use crate::block::BlockDev;
use crate::consts::EXT4_INODE_DIRECT_BLOCKS;
use crate::error::{Error, ErrorKind, Result};
use crate::inode::Inode;
use crate::BlockDevice;

/// 间接块映射器
///
/// 用于计算文件系统中的间接块限制和执行块映射。
pub struct IndirectBlockMapper {
    /// 每个间接块可以容纳的指针数量 (block_size / 4)
    blocks_per_indirect: u32,

    /// 每个间接层级的块数限制
    ///
    /// - limits[0] = 12 (直接块)
    /// - limits[1] = 12 + blocks_per_indirect (一级间接)
    /// - limits[2] = limits[1] + blocks_per_indirect^2 (二级间接)
    /// - limits[3] = limits[2] + blocks_per_indirect^3 (三级间接)
    block_limits: [u64; 4],

    /// 每个层级可以寻址的块数
    ///
    /// - blocks_per_level[0] = 1
    /// - blocks_per_level[1] = blocks_per_indirect
    /// - blocks_per_level[2] = blocks_per_indirect^2
    /// - blocks_per_level[3] = blocks_per_indirect^3
    blocks_per_level: [u64; 4],
}

impl IndirectBlockMapper {
    /// 创建新的间接块映射器
    ///
    /// # 参数
    ///
    /// - `block_size`: 文件系统块大小（字节）
    pub fn new(block_size: u32) -> Self {
        // 每个间接块能存储的指针数 = 块大小 / sizeof(u32)
        let blocks_per_indirect = block_size / 4;

        let mut block_limits = [0u64; 4];
        let mut blocks_per_level = [0u64; 4];

        // 初始化层级 0（直接块）
        block_limits[0] = EXT4_INODE_DIRECT_BLOCKS as u64;
        blocks_per_level[0] = 1;

        // 计算每个间接层级的限制
        for i in 1..4 {
            blocks_per_level[i] = blocks_per_level[i - 1] * blocks_per_indirect as u64;
            block_limits[i] = block_limits[i - 1] + blocks_per_level[i];
        }

        Self {
            blocks_per_indirect,
            block_limits,
            blocks_per_level,
        }
    }

    /// 将逻辑块号映射到物理块号
    ///
    /// # 参数
    ///
    /// - `blockdev`: 块设备引用
    /// - `inode`: inode 包装器
    /// - `logical_block`: 文件内的逻辑块号
    ///
    /// # 返回
    ///
    /// - `Ok(Some(physical_block))`: 找到了对应的物理块
    /// - `Ok(None)`: 逻辑块号对应的是文件空洞（sparse file）
    /// - `Err(...)`: 发生错误
    pub fn map_block<D: BlockDevice>(
        &self,
        blockdev: &mut BlockDev<D>,
        inode: &Inode,
        logical_block: u64,
    ) -> Result<Option<u64>> {
        #[cfg(feature = "std")]
        eprintln!("[indirect] Mapping logical block {}", logical_block);

        // 1. 检查是否是直接块
        if logical_block < EXT4_INODE_DIRECT_BLOCKS as u64 {
            let result = self.map_direct_block(inode, logical_block as u32);
            #[cfg(feature = "std")]
            eprintln!("[indirect] Direct block {} -> {:?}", logical_block, result);
            return result;
        }

        // 2. 确定间接层级
        let level = self.determine_indirect_level(logical_block)?;
        #[cfg(feature = "std")]
        eprintln!("[indirect] Block {} is at indirect level {}", logical_block, level);

        // 3. 根据层级进行映射
        match level {
            1 => self.map_single_indirect(blockdev, inode, logical_block),
            2 => self.map_double_indirect(blockdev, inode, logical_block),
            3 => self.map_triple_indirect(blockdev, inode, logical_block),
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid indirect level",
            )),
        }
    }

    /// 映射直接块（前 12 个块）
    fn map_direct_block(
        &self,
        inode: &Inode,
        logical_block: u32,
    ) -> Result<Option<u64>> {
        match inode.get_direct_block(logical_block as usize) {
            Some(physical_block) if physical_block != 0 => Ok(Some(physical_block as u64)),
            _ => Ok(None),
        }
    }

    /// 确定逻辑块号对应的间接层级
    fn determine_indirect_level(&self, logical_block: u64) -> Result<u32> {
        for level in 1..4 {
            if logical_block < self.block_limits[level] {
                return Ok(level as u32);
            }
        }

        Err(Error::new(
            ErrorKind::InvalidInput,
            "Logical block number exceeds maximum file size",
        ))
    }

    /// 映射一级间接块
    fn map_single_indirect<D: BlockDevice>(
        &self,
        blockdev: &mut BlockDev<D>,
        inode: &Inode,
        logical_block: u64,
    ) -> Result<Option<u64>> {
        // 获取一级间接块的物理地址
        let indirect_block = inode.get_indirect_block();

        if indirect_block == 0 {
            return Ok(None); // 空洞
        }

        // 计算在间接块内的偏移
        let offset_in_indirect = (logical_block - self.block_limits[0]) as u32;

        // 读取间接块并获取目标物理块号
        self.read_block_pointer(blockdev, indirect_block as u64, offset_in_indirect)
    }

    /// 映射二级间接块
    fn map_double_indirect<D: BlockDevice>(
        &self,
        blockdev: &mut BlockDev<D>,
        inode: &Inode,
        logical_block: u64,
    ) -> Result<Option<u64>> {
        // 获取二级间接块的物理地址
        let double_indirect_block = inode.get_double_indirect_block();
        if double_indirect_block == 0 {
            return Ok(None); // 空洞
        }

        // 计算在二级间接块范围内的相对偏移
        let offset_in_level = logical_block - self.block_limits[1];

        // 计算第一级索引（在二级间接块中的位置）
        let first_level_index = (offset_in_level / self.blocks_per_level[1]) as u32;

        // 读取第一级间接块地址
        let first_indirect_block = self.read_block_pointer(
            blockdev,
            double_indirect_block as u64,
            first_level_index,
        )?;

        if first_indirect_block.is_none() {
            return Ok(None); // 空洞
        }
        let first_indirect_block = first_indirect_block.unwrap();

        // 计算第二级索引（在一级间接块中的位置）
        let second_level_index = (offset_in_level % self.blocks_per_level[1]) as u32;

        // 读取最终的数据块地址
        self.read_block_pointer(blockdev, first_indirect_block, second_level_index)
    }

    /// 映射三级间接块
    fn map_triple_indirect<D: BlockDevice>(
        &self,
        blockdev: &mut BlockDev<D>,
        inode: &Inode,
        logical_block: u64,
    ) -> Result<Option<u64>> {
        // 获取三级间接块的物理地址
        let triple_indirect_block = inode.get_triple_indirect_block();
        if triple_indirect_block == 0 {
            return Ok(None); // 空洞
        }

        // 计算在三级间接块范围内的相对偏移
        let offset_in_level = logical_block - self.block_limits[2];

        // 计算第一级索引（在三级间接块中的位置）
        let first_level_index = (offset_in_level / self.blocks_per_level[2]) as u32;

        // 读取第一级间接块地址
        let first_indirect_block = self.read_block_pointer(
            blockdev,
            triple_indirect_block as u64,
            first_level_index,
        )?;

        if first_indirect_block.is_none() {
            return Ok(None); // 空洞
        }
        let first_indirect_block = first_indirect_block.unwrap();

        // 计算第二级索引
        let remaining_offset = offset_in_level % self.blocks_per_level[2];
        let second_level_index = (remaining_offset / self.blocks_per_level[1]) as u32;

        // 读取第二级间接块地址
        let second_indirect_block = self.read_block_pointer(
            blockdev,
            first_indirect_block,
            second_level_index,
        )?;

        if second_indirect_block.is_none() {
            return Ok(None); // 空洞
        }
        let second_indirect_block = second_indirect_block.unwrap();

        // 计算第三级索引
        let third_level_index = (remaining_offset % self.blocks_per_level[1]) as u32;

        // 读取最终的数据块地址
        self.read_block_pointer(blockdev, second_indirect_block, third_level_index)
    }

    /// 从间接块中读取指定位置的块指针
    ///
    /// # 参数
    ///
    /// - `blockdev`: 块设备
    /// - `indirect_block`: 间接块的物理块号
    /// - `index`: 在间接块内的索引位置
    ///
    /// # 返回
    ///
    /// - `Ok(Some(block_num))`: 读取到的块号
    /// - `Ok(None)`: 块号为 0（空洞）
    /// - `Err(...)`: 读取错误
    fn read_block_pointer<D: BlockDevice>(
        &self,
        blockdev: &mut BlockDev<D>,
        indirect_block: u64,
        index: u32,
    ) -> Result<Option<u64>> {
        use alloc::vec;

        // 读取间接块数据
        let block_size = blockdev.block_size() as usize;
        let mut buf = vec![0u8; block_size];
        blockdev.read_block(indirect_block, &mut buf)?;

        // 计算指针在块内的字节偏移
        let offset = (index as usize) * 4;

        // 检查边界
        if offset + 4 > buf.len() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Index out of bounds in indirect block",
            ));
        }

        // 读取 4 字节的块号（小端序）
        let block_num = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);

        Ok(if block_num == 0 {
            None
        } else {
            Some(block_num as u64)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapper_initialization() {
        let mapper = IndirectBlockMapper::new(4096);

        // 验证每个间接块可以存储 1024 个指针
        assert_eq!(mapper.blocks_per_indirect, 1024);

        // 验证层级限制
        assert_eq!(mapper.block_limits[0], 12); // 直接块
        assert_eq!(mapper.block_limits[1], 12 + 1024); // 一级间接
        assert_eq!(mapper.block_limits[2], 1036 + 1024 * 1024); // 二级间接

        // 验证每层可寻址的块数
        assert_eq!(mapper.blocks_per_level[0], 1);
        assert_eq!(mapper.blocks_per_level[1], 1024);
        assert_eq!(mapper.blocks_per_level[2], 1024 * 1024);
    }

    #[test]
    fn test_determine_indirect_level() {
        let mapper = IndirectBlockMapper::new(4096);

        // 直接块不应该调用这个函数，但测试边界
        assert_eq!(mapper.determine_indirect_level(12).unwrap(), 1);
        assert_eq!(mapper.determine_indirect_level(1035).unwrap(), 1);
        assert_eq!(mapper.determine_indirect_level(1036).unwrap(), 2);
        assert_eq!(mapper.determine_indirect_level(1049611).unwrap(), 2);
        assert_eq!(mapper.determine_indirect_level(1049612).unwrap(), 3);
    }
}
