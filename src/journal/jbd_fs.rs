//! JBD 文件系统管理
//!
//! 对应 lwext4 的 `struct jbd_fs` 和相关操作

use super::{recovery, types::*, JournalError};
use crate::{
    block::{Block, BlockDev, BlockDevice},
    consts::*,
    error::{Error, Result},
    fs::InodeRef,
    superblock::Superblock,
};

/// JBD 文件系统实例
///
/// 对应 lwext4 的 `struct jbd_fs`
///
/// 管理 journal inode 和 journal superblock，提供 journal 块映射功能。
///
/// # lwext4 对应关系
///
/// ```c
/// struct jbd_fs {
///     struct ext4_blockdev *bdev;
///     struct ext4_inode_ref inode_ref;
///     struct jbd_sb sb;
///     bool dirty;
/// };
/// ```
///
/// # Rust 实现
///
/// 由于 Rust 的生命周期限制，我们不持有 InodeRef，
/// 而是存储 journal inode 编号，需要时临时创建 InodeRef。
#[derive(Debug)]
pub struct JbdFs {
    /// Journal inode 编号（通常是 EXT4_JOURNAL_INO = 8）
    pub inode: u32,

    /// Journal superblock
    pub sb: jbd_sb,

    /// Whether journal superblock is dirty
    pub dirty: bool,
}

impl JbdFs {
    /// 从文件系统加载 journal
    ///
    /// 对应 lwext4 的 `jbd_get_fs()`
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `superblock` - 文件系统 superblock
    ///
    /// # 返回
    ///
    /// 成功返回 JbdFs 实例
    ///
    /// # 实现说明
    ///
    /// 1. 检查 journal 特性是否启用
    /// 2. 获取 journal inode（通常是 inode 8）
    /// 3. 读取 journal superblock（位于 journal 的第一个块）
    /// 4. 验证 journal superblock
    pub fn get<D: BlockDevice>(
        bdev: &mut BlockDev<D>,
        superblock: &mut Superblock,
    ) -> Result<Self> {
        // 检查是否启用了 HAS_JOURNAL 特性
        if !superblock.has_compat_feature(EXT4_FEATURE_COMPAT_HAS_JOURNAL) {
            return Err(Error::from(JournalError::NoJournalInode));
        }

        // 获取 journal inode 编号（从 superblock 的 journal_inum 字段）
        let journal_inum = u32::from_le(superblock.inner().journal_inum);

        if journal_inum == 0 {
            return Err(Error::from(JournalError::NoJournalInode));
        }

        // 创建 InodeRef 获取 journal inode 并获取第一个块的物理地址
        let first_block = {
            let mut inode_ref = InodeRef::get(bdev, superblock, journal_inum)?;
            inode_ref.get_inode_dblk_idx(0, false)?
        };

        // 读取 journal superblock
        let mut block = Block::get(bdev, first_block)?;
        let jbd_sb = block.with_data(|data| {
            // Journal superblock 位于块的开头
            let sb = unsafe {
                core::ptr::read_unaligned(data.as_ptr() as *const jbd_sb)
            };

            // 验证 magic number
            if !sb.is_valid() {
                return Err(Error::from(JournalError::InvalidSuperblock));
            }

            Ok(sb)
        })??;

        Ok(Self {
            inode: journal_inum,
            sb: jbd_sb,
            dirty: false,
        })
    }

    /// 写回并清理 journal
    ///
    /// 对应 lwext4 的 `jbd_put_fs()`
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `superblock` - 文件系统 superblock
    pub fn put<D: BlockDevice>(
        &mut self,
        bdev: &mut BlockDev<D>,
        superblock: &mut Superblock,
    ) -> Result<()> {
        if self.dirty {
            self.write_sb(bdev, superblock)?;
            self.dirty = false;
        }
        Ok(())
    }

    /// 将 journal 的逻辑块号映射到物理块号
    ///
    /// 对应 lwext4 的 `jbd_inode_bmap()`
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `superblock` - 文件系统 superblock
    /// * `iblock` - journal 内的逻辑块号
    ///
    /// # 返回
    ///
    /// 物理块号
    pub fn inode_bmap<D: BlockDevice>(
        &self,
        bdev: &mut BlockDev<D>,
        superblock: &mut Superblock,
        iblock: u32,
    ) -> Result<u64> {
        // 创建临时 InodeRef
        let mut inode_ref = InodeRef::get(bdev, superblock, self.inode)?;

        // 使用 InodeRef 的块映射功能
        inode_ref.get_inode_dblk_idx(iblock, false)
    }

    /// 执行 journal 恢复
    ///
    /// 对应 lwext4 的 `jbd_recover()`
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `superblock` - 文件系统 superblock
    ///
    /// # 返回
    ///
    /// 恢复是否成功
    pub fn recover<D: BlockDevice>(
        &mut self,
        bdev: &mut BlockDev<D>,
        superblock: &mut Superblock,
    ) -> Result<()> {
        // 调用 recovery 模块执行实际恢复
        recovery::recover(self, bdev, superblock)
    }

    /// 写回 journal superblock
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `superblock` - 文件系统 superblock
    fn write_sb<D: BlockDevice>(
        &self,
        bdev: &mut BlockDev<D>,
        superblock: &mut Superblock,
    ) -> Result<()> {
        // 创建临时 InodeRef 并获取第一个块的物理地址
        let first_block = {
            let mut inode_ref = InodeRef::get(bdev, superblock, self.inode)?;
            inode_ref.get_inode_dblk_idx(0, false)?
        };

        // 写入 journal superblock
        let mut block = Block::get(bdev, first_block)?;
        block.with_data_mut(|data| {
            // 将 jbd_sb 写入块的开头
            unsafe {
                core::ptr::write_unaligned(
                    data.as_mut_ptr() as *mut jbd_sb,
                    self.sb,
                );
            }
        })?;

        Ok(())
    }

    /// 获取 journal inode 编号
    pub fn inode(&self) -> u32 {
        self.inode
    }

    /// 获取 journal superblock 引用
    pub fn sb(&self) -> &jbd_sb {
        &self.sb
    }

    /// 获取 journal superblock 可变引用
    pub fn sb_mut(&mut self) -> &mut jbd_sb {
        self.dirty = true;
        &mut self.sb
    }

    /// 标记 journal superblock 为脏
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// 检查 journal superblock 是否脏
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// 获取 journal 块大小
    pub fn block_size(&self) -> u32 {
        u32::from_be(self.sb.blocksize)
    }

    /// 获取 journal 最大长度（块数）
    pub fn max_len(&self) -> u32 {
        u32::from_be(self.sb.maxlen)
    }

    /// 获取 journal 第一个块号
    pub fn first(&self) -> u32 {
        u32::from_be(self.sb.first)
    }

    /// 获取 journal 起始序列号
    pub fn start(&self) -> u32 {
        u32::from_be(self.sb.start)
    }

    /// 设置 journal 起始序列号
    pub fn set_start(&mut self, start: u32) {
        self.sb.start = start.to_be();
        self.dirty = true;
    }

    /// 获取 journal 序列号
    pub fn sequence(&self) -> u32 {
        u32::from_be(self.sb.sequence)
    }

    /// 设置 journal 序列号
    pub fn set_sequence(&mut self, seq: u32) {
        self.sb.sequence = seq.to_be();
        self.dirty = true;
    }

    /// 检查是否支持某个兼容特性
    pub fn has_compat_feature(&self, feature: u32) -> bool {
        self.sb.has_compat_feature(feature)
    }

    /// 检查是否支持某个不兼容特性
    pub fn has_incompat_feature(&self, feature: u32) -> bool {
        self.sb.has_incompat_feature(feature)
    }

    /// 检查是否支持某个只读兼容特性
    pub fn has_ro_compat_feature(&self, feature: u32) -> bool {
        self.sb.has_ro_compat_feature(feature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jbd_fs_api() {
        // 这些测试需要实际的 ext4 文件系统
        // 主要验证 API 设计和编译
    }

    #[test]
    fn test_jbd_fs_accessors() {
        // 创建一个测试用的 jbd_sb
        let mut jbd_sb = jbd_sb::default();
        jbd_sb.header.magic = JBD_MAGIC_NUMBER.to_be();
        jbd_sb.blocksize = 4096u32.to_be();
        jbd_sb.maxlen = 1024u32.to_be();
        jbd_sb.first = 1u32.to_be();
        jbd_sb.sequence = 100u32.to_be();
        jbd_sb.start = 10u32.to_be();

        let jbd_fs = JbdFs {
            inode: 8,
            sb: jbd_sb,
            dirty: false,
        };

        assert_eq!(jbd_fs.inode(), 8);
        assert_eq!(jbd_fs.block_size(), 4096);
        assert_eq!(jbd_fs.max_len(), 1024);
        assert_eq!(jbd_fs.first(), 1);
        assert_eq!(jbd_fs.sequence(), 100);
        assert_eq!(jbd_fs.start(), 10);
        assert!(!jbd_fs.is_dirty());
    }

    #[test]
    fn test_jbd_fs_dirty_tracking() {
        let mut jbd_sb = jbd_sb::default();
        jbd_sb.header.magic = JBD_MAGIC_NUMBER.to_be();

        let mut jbd_fs = JbdFs {
            inode: 8,
            sb: jbd_sb,
            dirty: false,
        };

        assert!(!jbd_fs.is_dirty());

        jbd_fs.mark_dirty();
        assert!(jbd_fs.is_dirty());

        jbd_fs.set_sequence(200);
        assert!(jbd_fs.is_dirty());
        assert_eq!(jbd_fs.sequence(), 200);
    }
}
