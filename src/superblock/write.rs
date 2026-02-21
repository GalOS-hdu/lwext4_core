//! Superblock 写入和更新

use crate::{
    block::{BlockDev, BlockDevice},
    consts::*,
    error::Result,
    types::ext4_sblock,
};

/// 将 superblock 写回块设备（仅主 superblock）
///
/// 对应 lwext4 的 `ext4_sb_write()`
///
/// 在写入前会自动更新校验和（如果启用）
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 结构
///
/// # 返回
///
/// 成功返回 ()
///
/// # 注意
///
/// 这个函数只写主 superblock，不写备份。
/// 如果需要写入备份 superblock，请使用 `write_superblock_with_backups()`
pub fn write_superblock<D: BlockDevice>(bdev: &mut BlockDev<D>, sb: &mut ext4_sblock) -> Result<()> {
    // 在写入前设置校验和
    super::checksum::set_checksum(sb);

    // 序列化 superblock 到字节数组
    let sb_bytes = crate::bytes::as_bytes(sb);

    // 写入到设备（偏移 1024 字节）
    bdev.write_bytes(EXT4_SUPERBLOCK_OFFSET, sb_bytes)?;

    Ok(())
}

/// 将 superblock 写回块设备（包括所有备份）
///
/// 根据 SPARSE_SUPER 特性写入备份 superblock 到正确的块组
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 结构
///
/// # 返回
///
/// 成功返回 ()
///
/// # 实现说明
///
/// 根据 ext4 规范：
/// - 主 superblock 总是在偏移 1024 字节处
/// - 备份 superblock 在每个包含超级块的块组的起始位置
/// - 块组是否包含超级块由 SPARSE_SUPER 特性决定
///   - 未启用 SPARSE_SUPER：每个块组都有备份
///   - 启用 SPARSE_SUPER：仅块组 0, 1, 以及 3/5/7 的幂次
///
/// 这确保了文件系统的鲁棒性，即使主 superblock 损坏也能恢复
pub fn write_superblock_with_backups<D: BlockDevice>(bdev: &mut BlockDev<D>, sb: &mut ext4_sblock) -> Result<()> {
    // 在写入前设置校验和
    super::checksum::set_checksum(sb);

    // 序列化 superblock 到字节数组
    let sb_bytes = crate::bytes::as_bytes(sb);

    // 1. 写入主 superblock（偏移 1024 字节）
    bdev.write_bytes(EXT4_SUPERBLOCK_OFFSET, sb_bytes)?;

    // 2. 写入备份 superblock
    // 创建临时 Superblock 包装器以使用 has_super_in_bg() 方法
    let sb_wrapper = super::Superblock::new(*sb);
    let block_size = sb_wrapper.block_size() as u64;
    let block_group_count = sb_wrapper.block_group_count();

    // 遍历所有块组，写入包含 superblock 的块组
    for bgid in 0..block_group_count {
        // 块组 0 已经通过主 superblock 写入，跳过
        if bgid == 0 {
            continue;
        }

        // 检查此块组是否应该包含 superblock 备份
        if sb_wrapper.has_super_in_bg(bgid) {
            // 计算此块组的起始块号
            let bg_start_block = sb_wrapper.first_data_block() as u64
                + (bgid as u64) * sb_wrapper.blocks_per_group() as u64;

            // superblock 在块组起始位置
            let sb_offset = bg_start_block * block_size;

            // 写入备份 superblock
            bdev.write_bytes(sb_offset, sb_bytes)?;
        }
    }

    Ok(())
}

/// Superblock 更新操作
impl super::Superblock {
    /// 获取可变的内部 superblock 结构
    ///
    /// 允许修改 superblock 字段
    pub fn inner_mut(&mut self) -> &mut ext4_sblock {
        &mut self.inner
    }

    /// 将 superblock 写回块设备（仅主 superblock）
    ///
    /// 在写入前会自动更新校验和（如果启用）
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    ///
    /// # 注意
    ///
    /// 这个方法只写主 superblock，不写备份。
    /// 如果需要写入备份 superblock，请使用 `write_with_backups()`
    pub fn write<D: BlockDevice>(&mut self, bdev: &mut BlockDev<D>) -> Result<()> {
        write_superblock(bdev, &mut self.inner)
    }

    /// 将 superblock 写回块设备（包括所有备份）
    ///
    /// 在写入前会自动更新校验和（如果启用）
    ///
    /// 根据 SPARSE_SUPER 特性写入备份 superblock 到正确的块组
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    ///
    /// # 返回
    ///
    /// 成功返回 ()
    ///
    /// # 实现说明
    ///
    /// 这确保了文件系统的鲁棒性，即使主 superblock 损坏也能从备份恢复
    pub fn write_with_backups<D: BlockDevice>(&mut self, bdev: &mut BlockDev<D>) -> Result<()> {
        write_superblock_with_backups(bdev, &mut self.inner)
    }

    /// 更新空闲块数
    ///
    /// # 参数
    ///
    /// * `count` - 新的空闲块数
    pub fn set_free_blocks_count(&mut self, count: u64) {
        self.inner.free_blocks_count_lo = count as u32;
        self.inner.free_blocks_count_hi = (count >> 32) as u32;
    }

    /// 更新空闲 inode 数
    ///
    /// # 参数
    ///
    /// * `count` - 新的空闲 inode 数
    pub fn set_free_inodes_count(&mut self, count: u32) {
        self.inner.free_inodes_count = count;
    }

    /// 增加空闲块数
    ///
    /// # 参数
    ///
    /// * `delta` - 增加的数量
    pub fn add_free_blocks(&mut self, delta: u64) {
        let current = self.free_blocks_count();
        self.set_free_blocks_count(current + delta);
    }

    /// 减少空闲块数
    ///
    /// # 参数
    ///
    /// * `delta` - 减少的数量
    pub fn sub_free_blocks(&mut self, delta: u64) {
        let current = self.free_blocks_count();
        self.set_free_blocks_count(current.saturating_sub(delta));
    }

    /// 增加空闲 inode 数
    ///
    /// # 参数
    ///
    /// * `delta` - 增加的数量
    pub fn add_free_inodes(&mut self, delta: u32) {
        let current = self.free_inodes_count();
        self.set_free_inodes_count(current + delta);
    }

    /// 减少空闲 inode 数
    ///
    /// # 参数
    ///
    /// * `delta` - 减少的数量
    pub fn sub_free_inodes(&mut self, delta: u32) {
        let current = self.free_inodes_count();
        self.set_free_inodes_count(current.saturating_sub(delta));
    }

    /// 更新挂载计数
    ///
    /// 每次挂载文件系统时调用
    pub fn inc_mount_count(&mut self) {
        self.inner.mnt_count = self.inner.mnt_count.saturating_add(1);
    }

    /// 更新写入计数
    ///
    /// 每次执行写操作时调用
    pub fn inc_write_count(&mut self) {
        self.inner.wtime = current_timestamp();
    }

    /// 更新最后挂载时间
    pub fn update_mount_time(&mut self) {
        self.inner.mtime = current_timestamp();
    }

    /// 更新最后写入时间
    pub fn update_write_time(&mut self) {
        self.inner.wtime = current_timestamp();
    }

    /// 更新最后检查时间
    pub fn update_check_time(&mut self) {
        self.inner.lastcheck = current_timestamp();
    }

    /// 设置文件系统状态
    ///
    /// # 参数
    ///
    /// * `state` - 状态值（1 = 干净，2 = 有错误）
    pub fn set_state(&mut self, state: u16) {
        self.inner.state = state;
    }

    /// 标记文件系统为干净
    pub fn mark_clean(&mut self) {
        self.set_state(EXT4_SUPER_STATE_VALID);
    }

    /// 标记文件系统有错误
    pub fn mark_error(&mut self) {
        self.set_state(EXT4_SUPER_STATE_ERROR);
    }

    /// 更新校验和
    ///
    /// 如果文件系统启用了元数据校验和特性，需要在修改 superblock 后更新校验和
    ///
    /// 对应 lwext4 的 `ext4_sb_set_csum()`
    pub fn update_checksum(&mut self) {
        super::checksum::set_checksum(&mut self.inner);
    }

    /// 设置校验和（update_checksum 的别名）
    ///
    /// 对应 lwext4 的 `ext4_sb_set_csum()`
    pub fn set_checksum(&mut self) {
        self.update_checksum();
    }
}

/// 获取当前时间戳（Unix 时间）
///
/// 在 no_std 环境中，需要外部提供时间源
/// 这里提供一个默认实现（返回 0），实际使用时应该替换
fn current_timestamp() -> u32 {
    // TODO: 在实际使用时，应该从系统获取真实时间戳
    // 在 ArceOS 中可以调用 axhal::time::current_time()
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::{BlockDevice, BlockDev};
    use crate::error::Result;
    use crate::superblock::Superblock;

    struct MockDevice {
        block_size: u32,
        sector_size: u32,
        total_blocks: u64,
        storage: alloc::vec::Vec<u8>,
    }

    impl MockDevice {
        fn new() -> Self {
            let block_size = 4096;
            let sector_size = 512;
            let total_blocks = 1000;
            let storage = alloc::vec![0u8; (total_blocks * block_size as u64) as usize];
            Self {
                block_size,
                sector_size,
                total_blocks,
                storage,
            }
        }
    }

    impl BlockDevice for MockDevice {
        fn block_size(&self) -> u32 {
            self.block_size
        }

        fn sector_size(&self) -> u32 {
            self.sector_size
        }

        fn total_blocks(&self) -> u64 {
            self.total_blocks
        }

        fn read_blocks(&mut self, lba: u64, count: u32, buf: &mut [u8]) -> Result<usize> {
            let start = (lba * self.sector_size as u64) as usize;
            let len = (count * self.sector_size) as usize;
            buf[..len].copy_from_slice(&self.storage[start..start + len]);
            Ok(len)
        }

        fn write_blocks(&mut self, lba: u64, count: u32, buf: &[u8]) -> Result<usize> {
            let start = (lba * self.sector_size as u64) as usize;
            let len = (count * self.sector_size) as usize;
            self.storage[start..start + len].copy_from_slice(&buf[..len]);
            Ok(len)
        }
    }

    #[test]
    fn test_superblock_modification() {
        let device = MockDevice::new();
        let mut block_dev = BlockDev::new(device).unwrap();

        // 创建一个测试用的 superblock
        let mut sb = ext4_sblock::default();
        sb.magic = EXT4_SUPERBLOCK_MAGIC;
        sb.free_blocks_count_lo = 1000;
        sb.free_blocks_count_hi = 0;
        sb.free_inodes_count = 500;

        let mut superblock = Superblock { inner: sb };

        // 测试修改空闲块数
        assert_eq!(superblock.free_blocks_count(), 1000);
        superblock.add_free_blocks(100);
        assert_eq!(superblock.free_blocks_count(), 1100);
        superblock.sub_free_blocks(50);
        assert_eq!(superblock.free_blocks_count(), 1050);

        // 测试修改空闲 inode 数
        assert_eq!(superblock.free_inodes_count(), 500);
        superblock.add_free_inodes(50);
        assert_eq!(superblock.free_inodes_count(), 550);
        superblock.sub_free_inodes(100);
        assert_eq!(superblock.free_inodes_count(), 450);

        // 测试写入（不应该失败）
        superblock.write(&mut block_dev).unwrap();
    }

    #[test]
    fn test_superblock_state() {
        let mut superblock = Superblock {
            inner: ext4_sblock::default(),
        };

        superblock.mark_clean();
        assert_eq!(superblock.inner().state, EXT4_SUPER_STATE_VALID);

        superblock.mark_error();
        assert_eq!(superblock.inner().state, EXT4_SUPER_STATE_ERROR);
    }
}
