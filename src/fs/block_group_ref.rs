//! 块组引用结构
//!
//! 对应 lwext4 的 `ext4_block_group_ref`，提供 RAII 风格的块组操作

use crate::{
    block::{Block, BlockDev, BlockDevice},
    block_group::get_block_group_desc_location,
    consts::*,
    error::Result,
    superblock::Superblock,
    types::ext4_group_desc,
};

/// 块组引用
///
/// 类似 lwext4 的 `ext4_block_group_ref`，自动管理块组描述符的加载和写回
///
/// # 设计说明
///
/// 与 lwext4 C 版本一致，BlockGroupRef 持有一个 Block 句柄，
/// 直接操作 cache 中的块组描述符数据，而不是持有数据副本。
/// 这保证了：
/// 1. **一致性**: 所有对同一块组的访问都操作同一份 cache 数据
/// 2. **性能**: 避免不必要的数据复制
/// 3. **正确语义**: 修改直接作用于 cache，自动标记为脏
///
/// # 生命周期
///
/// - 创建时获取包含块组描述符的 block 句柄
/// - 通过 block 句柄访问和修改块组描述符数据
/// - Drop 时自动释放 block 句柄
///
/// # 示例
///
/// ```rust,ignore
/// let mut bg_ref = BlockGroupRef::get(&mut bdev, &sb, bgid)?;
/// bg_ref.set_free_blocks_count(100)?;
/// bg_ref.mark_dirty()?;
/// // Drop 时自动释放 block 句柄
/// ```
pub struct BlockGroupRef<'a, D: BlockDevice> {
    /// Block 句柄，持有包含块组描述符的块
    block: Block<'a, D>,
    /// Superblock 引用
    sb: &'a Superblock,
    /// 块组 ID
    bgid: u32,
    /// 块组描述符在块内的偏移（字节）
    offset_in_block: usize,
    /// 是否已标记为脏
    dirty: bool,
}

impl<'a, D: BlockDevice> BlockGroupRef<'a, D> {
    /// 获取块组引用（自动加载）
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `sb` - superblock 引用
    /// * `bgid` - 块组 ID
    ///
    /// # 返回
    ///
    /// 成功返回 BlockGroupRef
    ///
    /// # 实现说明
    ///
    /// 对应 lwext4 的 `ext4_fs_get_block_group_ref()`
    pub fn get(
        bdev: &'a mut BlockDev<D>,
        sb: &'a Superblock,
        bgid: u32,
    ) -> Result<Self> {
        // 使用统一的 GDT 定位函数，支持 META_BG
        let (desc_block_addr, offset_in_block_u64) = get_block_group_desc_location(sb, bgid);
        let offset_in_block = offset_in_block_u64 as usize;

        // 获取包含块组描述符的 block 句柄
        let block = Block::get(bdev, desc_block_addr)?;

        Ok(Self {
            block,
            sb,
            bgid,
            offset_in_block,
            dirty: false,
        })
    }

    /// 获取块组 ID
    pub fn bgid(&self) -> u32 {
        self.bgid
    }

    /// 访问块组描述符数据（只读）
    ///
    /// 通过闭包访问块组描述符数据，避免生命周期问题
    pub fn with_block_group<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&ext4_group_desc) -> R,
    {
        self.block.with_data(|data| {
            let desc = unsafe {
                &*(data.as_ptr().add(self.offset_in_block) as *const ext4_group_desc)
            };
            f(desc)
        })
    }

    /// 访问块组描述符数据（可写）
    ///
    /// 通过闭包修改块组描述符数据，自动标记 block 为脏
    pub fn with_block_group_mut<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut ext4_group_desc) -> R,
    {
        let result = self.block.with_data_mut(|data| {
            let desc = unsafe {
                &mut *(data.as_mut_ptr().add(self.offset_in_block) as *mut ext4_group_desc)
            };
            f(desc)
        })?;
        self.dirty = true;
        Ok(result)
    }

    /// 标记为脏（需要写回）
    ///
    /// 注意：修改块组描述符时会自动标记为脏，通常不需要手动调用
    pub fn mark_dirty(&mut self) -> Result<()> {
        if !self.dirty {
            // 标记 block 为脏
            self.block.with_data_mut(|_| {})?;
            self.dirty = true;
        }
        Ok(())
    }

    /// 检查是否为脏
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// 手动写回
    ///
    /// 通常不需要手动调用，Drop 时 Block 会自动写回脏数据
    pub fn flush(&mut self) -> Result<()> {
        // Block 的 Drop 会自动处理写回
        // 这里只需要清除 dirty 标志
        if self.dirty {
            self.dirty = false;
        }
        Ok(())
    }

    // ===== 便捷方法 =====

    /// 获取块位图地址
    pub fn block_bitmap(&mut self) -> Result<u64> {
        let sb = self.sb;
        self.with_block_group(|desc| {
            let mut v = u32::from_le(desc.block_bitmap_lo) as u64;

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                v |= (u32::from_le(desc.block_bitmap_hi) as u64) << 32;
            }

            v
        })
    }

    /// 获取 inode 位图地址
    pub fn inode_bitmap(&mut self) -> Result<u64> {
        let sb = self.sb;
        self.with_block_group(|desc| {
            let mut v = u32::from_le(desc.inode_bitmap_lo) as u64;

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                v |= (u32::from_le(desc.inode_bitmap_hi) as u64) << 32;
            }

            v
        })
    }

    /// 获取 inode 表地址
    pub fn inode_table(&mut self) -> Result<u64> {
        let sb = self.sb;
        self.with_block_group(|desc| {
            let mut v = u32::from_le(desc.inode_table_lo) as u64;

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                v |= (u32::from_le(desc.inode_table_hi) as u64) << 32;
            }

            v
        })
    }

    /// 获取空闲块数
    pub fn free_blocks_count(&mut self) -> Result<u32> {
        let sb = self.sb;
        self.with_block_group(|desc| {
            let mut v = u16::from_le(desc.free_blocks_count_lo) as u32;

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                v |= (u16::from_le(desc.free_blocks_count_hi) as u32) << 16;
            }

            v
        })
    }

    /// 设置空闲块数
    pub fn set_free_blocks_count(&mut self, count: u32) -> Result<()> {
        let sb = self.sb;
        self.with_block_group_mut(|desc| {
            desc.free_blocks_count_lo = (count as u16).to_le();

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                desc.free_blocks_count_hi = ((count >> 16) as u16).to_le();
            }
        })
    }

    /// 增加空闲块数
    pub fn inc_free_blocks(&mut self, delta: u32) -> Result<()> {
        let current = self.free_blocks_count()?;
        self.set_free_blocks_count(current + delta)
    }

    /// 减少空闲块数
    pub fn dec_free_blocks(&mut self, delta: u32) -> Result<()> {
        let current = self.free_blocks_count()?;
        if current >= delta {
            self.set_free_blocks_count(current - delta)
        } else {
            self.set_free_blocks_count(0)
        }
    }

    /// 获取空闲 inode 数
    pub fn free_inodes_count(&mut self) -> Result<u32> {
        let sb = self.sb;
        self.with_block_group(|desc| {
            let mut v = u16::from_le(desc.free_inodes_count_lo) as u32;

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                v |= (u16::from_le(desc.free_inodes_count_hi) as u32) << 16;
            }

            v
        })
    }

    /// 设置空闲 inode 数
    pub fn set_free_inodes_count(&mut self, count: u32) -> Result<()> {
        let sb = self.sb;
        self.with_block_group_mut(|desc| {
            desc.free_inodes_count_lo = (count as u16).to_le();

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                desc.free_inodes_count_hi = ((count >> 16) as u16).to_le();
            }
        })
    }

    /// 增加空闲 inode 数
    pub fn inc_free_inodes(&mut self, delta: u32) -> Result<()> {
        let current = self.free_inodes_count()?;
        self.set_free_inodes_count(current + delta)
    }

    /// 减少空闲 inode 数
    pub fn dec_free_inodes(&mut self, delta: u32) -> Result<()> {
        let current = self.free_inodes_count()?;
        if current >= delta {
            self.set_free_inodes_count(current - delta)
        } else {
            self.set_free_inodes_count(0)
        }
    }

    /// 获取已使用目录数
    pub fn used_dirs_count(&mut self) -> Result<u32> {
        let sb = self.sb;
        self.with_block_group(|desc| {
            let mut v = u16::from_le(desc.used_dirs_count_lo) as u32;

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                v |= (u16::from_le(desc.used_dirs_count_hi) as u32) << 16;
            }

            v
        })
    }

    /// 设置已使用目录数
    pub fn set_used_dirs_count(&mut self, count: u32) -> Result<()> {
        let sb = self.sb;
        self.with_block_group_mut(|desc| {
            desc.used_dirs_count_lo = (count as u16).to_le();

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                desc.used_dirs_count_hi = ((count >> 16) as u16).to_le();
            }
        })
    }

    /// 增加已使用目录数
    pub fn inc_used_dirs(&mut self) -> Result<()> {
        let current = self.used_dirs_count()?;
        self.set_used_dirs_count(current + 1)
    }

    /// 减少已使用目录数
    pub fn dec_used_dirs(&mut self) -> Result<()> {
        let current = self.used_dirs_count()?;
        if current > 0 {
            self.set_used_dirs_count(current - 1)
        } else {
            Ok(())
        }
    }

    /// 获取未使用 inode 表数
    pub fn itable_unused(&mut self) -> Result<u32> {
        let sb = self.sb;
        self.with_block_group(|desc| {
            let mut v = u16::from_le(desc.itable_unused_lo) as u32;

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                v |= (u16::from_le(desc.itable_unused_hi) as u32) << 16;
            }

            v
        })
    }

    /// 设置未使用 inode 表数
    pub fn set_itable_unused(&mut self, count: u32) -> Result<()> {
        let sb = self.sb;
        self.with_block_group_mut(|desc| {
            desc.itable_unused_lo = (count as u16).to_le();

            if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                desc.itable_unused_hi = ((count >> 16) as u16).to_le();
            }
        })
    }

    /// 获取块组描述符的拷贝（用于需要长期持有的场景）
    ///
    /// 注意：返回的是数据副本，修改不会反映到磁盘
    pub fn get_block_group_copy(&mut self) -> Result<ext4_group_desc> {
        self.with_block_group(|desc| *desc)
    }
}

impl<'a, D: BlockDevice> Drop for BlockGroupRef<'a, D> {
    fn drop(&mut self) {
        // Block 的 Drop 会自动处理写回
        // 这里不需要额外操作
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_group_ref_api() {
        // 这些测试需要实际的块设备和 ext4 文件系统
        // 主要是验证 API 的设计和编译
    }
}
