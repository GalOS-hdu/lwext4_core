//! Ext4 文件系统核心结构

use crate::{
    block::{BlockDev, BlockDevice},
    error::{Error, ErrorKind, Result},
    superblock::Superblock,
};

use super::{inode_ref::InodeRef, block_group_ref::BlockGroupRef};

/// 文件系统统计信息
#[derive(Debug, Clone)]
pub struct FileSystemStats {
    /// 块大小（字节）
    pub block_size: u32,
    /// 总块数
    pub blocks_total: u64,
    /// 空闲块数
    pub blocks_free: u64,
    /// 可用块数（考虑保留块）
    pub blocks_available: u64,
    /// 总 inode 数
    pub inodes_total: u32,
    /// 空闲 inode 数
    pub inodes_free: u32,
    /// 文件系统 ID
    pub filesystem_id: u64,
    /// 最大文件名长度
    pub max_filename_len: u32,
}

/// Ext4 文件系统
///
/// 提供完整的文件系统操作接口
///
/// # 示例
///
/// ```rust,ignore
/// use lwext4_core::{Ext4FileSystem, BlockDev};
///
/// let device = MyBlockDevice::new();
/// let mut bdev = BlockDev::new(device);
/// let mut fs = Ext4FileSystem::mount(&mut bdev)?;
///
/// // 打开文件
/// let mut file = fs.open("/etc/passwd")?;
/// let mut buf = vec![0u8; 1024];
/// let n = file.read(&mut buf)?;
///
/// // 读取目录
/// let entries = fs.read_dir("/bin")?;
/// for entry in entries {
///     println!("{}", entry.name);
/// }
///
/// // 获取文件元数据
/// let metadata = fs.metadata("/etc/passwd")?;
/// println!("File size: {} bytes", metadata.size);
/// ```
pub struct Ext4FileSystem<D: BlockDevice> {
    pub(crate) bdev: BlockDev<D>,
    pub(crate) sb: Superblock,
}

impl<D: BlockDevice> Ext4FileSystem<D> {
    /// 挂载文件系统
    pub fn mount(mut bdev: BlockDev<D>) -> Result<Self> {
        let sb = Superblock::load(&mut bdev)?;

        Ok(Self { bdev, sb })
    }

    /// 卸载文件系统
    ///
    /// 此方法会消费 `self`，写回 superblock 并返回底层的块设备。
    pub fn unmount(mut self) -> Result<BlockDev<D>> {
        self.sb.write(&mut self.bdev)?;
        Ok(self.bdev)
    }

    /// 获取 superblock 引用
    pub fn superblock(&self) -> &Superblock {
        &self.sb
    }

    /// 获取块设备引用
    pub fn block_device(&self) -> &BlockDev<D> {
        &self.bdev
    }

    /// 获取可变块设备引用
    pub fn block_device_mut(&mut self) -> &mut BlockDev<D> {
        &mut self.bdev
    }

    /// 获取可变 superblock 引用
    pub fn superblock_mut(&mut self) -> &mut Superblock {
        &mut self.sb
    }

    /// 获取文件系统统计信息
    pub fn stats(&self) -> Result<FileSystemStats> {
        let sb_inner = self.sb.inner();

        Ok(FileSystemStats {
            block_size: self.sb.block_size(),
            blocks_total: u32::from_le(sb_inner.blocks_count_lo) as u64
                | ((u32::from_le(sb_inner.blocks_count_hi) as u64) << 32),
            blocks_free: u32::from_le(sb_inner.free_blocks_count_lo) as u64
                | ((u32::from_le(sb_inner.free_blocks_count_hi) as u64) << 32),
            blocks_available: {
                let free = u32::from_le(sb_inner.free_blocks_count_lo) as u64
                    | ((u32::from_le(sb_inner.free_blocks_count_hi) as u64) << 32);
                let reserved = u32::from_le(sb_inner.r_blocks_count_lo) as u64
                    | ((u32::from_le(sb_inner.r_blocks_count_hi) as u64) << 32);
                free.saturating_sub(reserved)
            },
            inodes_total: u32::from_le(sb_inner.inodes_count),
            inodes_free: u32::from_le(sb_inner.free_inodes_count),
            filesystem_id: {
                let uuid = &sb_inner.uuid;
                u64::from_le_bytes([
                    uuid[0], uuid[1], uuid[2], uuid[3],
                    uuid[4], uuid[5], uuid[6], uuid[7],
                ])
            },
            max_filename_len: 255,
        })
    }

    /// 刷新所有缓存的脏数据到磁盘
    pub fn flush(&mut self) -> Result<()> {
        self.bdev.flush()
    }

    /// 获取 inode 引用
    pub fn get_inode_ref(&mut self, inode_num: u32) -> Result<InodeRef<D>> {
        InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)
    }

    /// 获取块组引用
    pub fn get_block_group_ref(&mut self, bgid: u32) -> Result<BlockGroupRef<D>> {
        BlockGroupRef::get(&mut self.bdev, &self.sb, bgid)
    }

    // ========== 内部辅助方法 ==========

    /// 获取或分配文件块（供 File::write 使用）
    #[allow(dead_code)] // 待文件随机写入 API 重设计后使用
    pub(crate) fn get_file_block(&mut self, inode_num: u32, logical_block: u32) -> Result<u64> {
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        let physical_block = inode_ref.map_extent_block(logical_block)?
            .ok_or_else(|| Error::new(ErrorKind::Unsupported, "Block not allocated - automatic allocation requires API redesign"))?;

        Ok(physical_block)
    }

    /// 添加目录项（内部辅助方法）
    pub(crate) fn add_dir_entry(&mut self, dir_inode: u32, name: &str, child_inode: u32, file_type: u8) -> Result<()> {
        use crate::dir::write;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dir_inode)?;

        let sb_ptr = inode_ref.superblock_mut() as *mut Superblock;
        let sb_ref = unsafe { &mut *sb_ptr };

        write::add_entry(&mut inode_ref, sb_ref, name, child_inode, file_type)?;

        Ok(())
    }

    /// 删除目录项（内部辅助方法）
    pub(crate) fn remove_dir_entry(&mut self, dir_inode: u32, name: &str) -> Result<()> {
        use crate::dir::write;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dir_inode)?;

        write::remove_entry(&mut inode_ref, name)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filesystem_api() {
        // 这些测试需要实际的块设备和 ext4 文件系统
        // 主要是验证 API 的设计和编译
    }
}
