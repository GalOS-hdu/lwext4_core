//! Inode 读取和操作

use crate::{
    block::{BlockDev, BlockDevice},
    block_group,
    consts::*,
    error::{Error, ErrorKind, Result},
    superblock::Superblock,
    types::ext4_inode,
};
use alloc::vec;

/// 从块设备读取 inode
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `inode_num` - inode 编号（从 1 开始）
///
/// # 返回
///
/// 成功返回 inode 结构
///
/// # 说明
///
/// inode 编号从 1 开始，0 表示无效 inode
pub fn read_inode<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &Superblock,
    inode_num: u32,
) -> Result<ext4_inode> {
    if inode_num == 0 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Invalid inode number (0)",
        ));
    }

    // 计算 inode 所在的块组
    let inodes_per_group = sb.inodes_per_group();
    let block_group = (inode_num - 1) / inodes_per_group;
    let index_in_group = (inode_num - 1) % inodes_per_group;

    // 读取块组描述符（使用公开的版本，支持 META_BG）
    let desc = block_group::read_block_group_desc(bdev, sb, block_group)?;

    // 获取 inode 表的位置
    let inode_table_block = desc.inode_table();
    let block_size = sb.block_size() as u64;
    let inode_size = sb.inode_size() as u64;

    // 计算 inode 的字节偏移
    let inode_offset = inode_table_block * block_size + (index_in_group as u64) * inode_size;

    // 读取 inode
    let mut inode_buf = vec![0u8; inode_size as usize];
    bdev.read_bytes(inode_offset, &mut inode_buf)?;

    let inode: ext4_inode = crate::bytes::read_struct(&inode_buf)?;

    Ok(inode)
}

/// Inode 包装器，提供高级操作
pub struct Inode {
    pub(super) inner: ext4_inode,
    pub(super) inode_num: u32,
}

impl Inode {
    /// 从块设备加载 inode
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `sb` - superblock 引用
    /// * `inode_num` - inode 编号
    pub fn load<D: BlockDevice>(
        bdev: &mut BlockDev<D>,
        sb: &Superblock,
        inode_num: u32,
    ) -> Result<Self> {
        let inner = read_inode(bdev, sb, inode_num)?;
        Ok(Self { inner, inode_num })
    }

    /// 从原始 inode 数据创建
    ///
    /// # 参数
    ///
    /// * `inner` - ext4_inode 结构
    /// * `inode_num` - inode 编号
    ///
    /// # 注意
    ///
    /// 此方法用于从已有的 ext4_inode 数据创建 Inode 实例，
    /// 主要用于临时需要 Inode 接口的场景（如 extent tree 操作）
    pub fn from_raw(inner: ext4_inode, inode_num: u32) -> Self {
        Self { inner, inode_num }
    }

    /// 获取 inode 编号
    pub fn inode_num(&self) -> u32 {
        self.inode_num
    }

    /// 获取内部 inode 结构的引用
    pub fn inner(&self) -> &ext4_inode {
        &self.inner
    }

    /// 获取文件大小
    pub fn file_size(&self) -> u64 {
        self.inner.file_size()
    }

    /// 获取文件模式（类型 + 权限）
    pub fn mode(&self) -> u16 {
        u16::from_le(self.inner.mode)
    }

    /// 检查是否是目录
    pub fn is_dir(&self) -> bool {
        self.inner.is_dir()
    }

    /// 检查是否是普通文件
    pub fn is_file(&self) -> bool {
        self.inner.is_file()
    }

    /// 检查是否是符号链接
    pub fn is_symlink(&self) -> bool {
        self.inner.is_symlink()
    }

    /// 获取链接计数
    pub fn links_count(&self) -> u16 {
        u16::from_le(self.inner.links_count)
    }

    /// 获取占用的块数（512 字节为单位）
    ///
    /// 支持 HUGE_FILE 特性，对应 lwext4 的 `ext4_inode_get_blocks_count()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    pub fn blocks_count_with_sb(&self, sb: &Superblock) -> u64 {
        // 1. 读取 32 位低位
        let mut cnt = u32::from_le(self.inner.blocks_count_lo) as u64;

        // 2. 检查是否启用了 HUGE_FILE 特性
        if sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_HUGE_FILE) {
            // 3. 扩展到 48 位
            cnt |= (u16::from_le(self.inner.blocks_high) as u64) << 32;

            // 4. 检查 inode 是否使用了 HUGE_FILE 标志
            if self.flags() & EXT4_INODE_FLAG_HUGE_FILE != 0 {
                // 5. 进行比例换算：从文件系统块单位转换为 512 字节单位
                let block_size = sb.block_size();
                let block_bits = super::write::inode_block_bits_count(block_size);
                return cnt << (block_bits - 9);
            }
        }

        cnt
    }

    /// 获取占用的块数（简化版本，不支持 HUGE_FILE）
    ///
    /// 仅返回 32 位块计数，如果需要正确处理 HUGE_FILE，
    /// 请使用 `blocks_count_with_sb()`
    pub fn blocks_count(&self) -> u64 {
        self.inner.blocks_count()
    }

    /// 获取标志
    pub fn flags(&self) -> u32 {
        u32::from_le(self.inner.flags)
    }

    /// 检查是否使用 extent
    pub fn has_extents(&self) -> bool {
        (self.flags() & EXT4_INODE_FLAG_EXTENTS) != 0
    }

    /// 检查是否是巨型文件
    pub fn is_huge_file(&self) -> bool {
        (self.flags() & EXT4_INODE_FLAG_HUGE_FILE) != 0
    }

    /// 检查是否使用索引（对目录）
    pub fn has_index(&self) -> bool {
        (self.flags() & EXT4_INODE_FLAG_INDEX) != 0
    }

    /// 获取直接块指针
    ///
    /// # 参数
    ///
    /// * `index` - 块索引（0-11）
    ///
    /// # 返回
    ///
    /// 块号，如果索引无效则返回 None
    pub fn get_direct_block(&self, index: usize) -> Option<u32> {
        if index < EXT4_INODE_DIRECT_BLOCKS {
            Some(u32::from_le(self.inner.blocks[index]))
        } else {
            None
        }
    }

    /// 获取间接块指针
    pub fn get_indirect_block(&self) -> u32 {
        u32::from_le(self.inner.blocks[EXT4_INODE_INDIRECT_BLOCK])
    }

    /// 获取二级间接块指针
    pub fn get_double_indirect_block(&self) -> u32 {
        u32::from_le(self.inner.blocks[EXT4_INODE_DOUBLE_INDIRECT_BLOCK])
    }

    /// 获取三级间接块指针
    pub fn get_triple_indirect_block(&self) -> u32 {
        u32::from_le(self.inner.blocks[EXT4_INODE_TRIPLE_INDIRECT_BLOCK])
    }

    /// 获取访问时间（秒）
    pub fn atime(&self) -> u32 {
        u32::from_le(self.inner.atime)
    }

    /// 获取创建时间（秒）
    pub fn ctime(&self) -> u32 {
        u32::from_le(self.inner.ctime)
    }

    /// 获取修改时间（秒）
    pub fn mtime(&self) -> u32 {
        u32::from_le(self.inner.mtime)
    }

    /// 获取删除时间（秒）
    pub fn dtime(&self) -> u32 {
        u32::from_le(self.inner.dtime)
    }

    /// 检查文件是否已删除
    pub fn is_deleted(&self) -> bool {
        self.dtime() != 0
    }

    /// 获取访问时间（别名）
    pub fn access_time(&self) -> u32 {
        self.atime()
    }

    /// 获取修改时间（别名）
    pub fn modification_time(&self) -> u32 {
        self.mtime()
    }

    /// 获取改变时间（别名）
    pub fn change_time(&self) -> u32 {
        self.ctime()
    }

    /// 获取 UID（用户 ID）
    pub fn uid(&self) -> u32 {
        (u16::from_le(self.inner.uid) as u32)
            | ((u16::from_le(self.inner.uid_high) as u32) << 16)
    }

    /// 获取 GID（组 ID）
    pub fn gid(&self) -> u32 {
        (u16::from_le(self.inner.gid) as u32)
            | ((u16::from_le(self.inner.gid_high) as u32) << 16)
    }

    /// 获取 ACL 块号
    ///
    /// 对应 lwext4 的 `ext4_inode_get_file_acl()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    ///
    /// # 返回
    ///
    /// ACL 块号（64位）
    pub fn get_file_acl(&self, sb: &Superblock) -> u64 {
        let acl_lo = u32::from_le(self.inner.file_acl_lo) as u64;

        if sb.inner().creator_os == EXT4_SUPERBLOCK_OS_LINUX.to_le() {
            let acl_hi = u16::from_le(self.inner.file_acl_high) as u64;
            acl_lo | (acl_hi << 32)
        } else {
            acl_lo
        }
    }

    /// 获取 inode 额外空间大小
    ///
    /// 对应 lwext4 的 `ext4_inode_get_extra_isize()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    ///
    /// # 返回
    ///
    /// 额外空间大小（字节）
    pub fn get_extra_isize(&self, sb: &Superblock) -> u16 {
        if sb.inode_size() <= EXT4_GOOD_OLD_INODE_SIZE as u16 {
            0
        } else {
            u16::from_le(self.inner.extra_isize)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inode_helpers() {
        let mut inode = ext4_inode::default();

        // 设置为普通文件
        inode.mode = EXT4_INODE_MODE_FILE.to_le();
        assert!(inode.is_file());
        assert!(!inode.is_dir());
        assert!(!inode.is_symlink());

        // 设置为目录
        inode.mode = EXT4_INODE_MODE_DIRECTORY.to_le();
        assert!(inode.is_dir());
        assert!(!inode.is_file());

        // 设置文件大小
        inode.size_lo = 1024u32.to_le();
        inode.size_hi = 0u32.to_le();
        assert_eq!(inode.file_size(), 1024);
    }

    #[test]
    fn test_inode_wrapper() {
        let mut inode_inner = ext4_inode::default();
        inode_inner.mode = EXT4_INODE_MODE_FILE.to_le();
        inode_inner.size_lo = 2048u32.to_le();
        inode_inner.flags = EXT4_INODE_FLAG_EXTENTS.to_le();

        let inode = Inode {
            inner: inode_inner,
            inode_num: 2,
        };

        assert_eq!(inode.inode_num(), 2);
        assert_eq!(inode.file_size(), 2048);
        assert!(inode.is_file());
        assert!(inode.has_extents());
    }

    #[test]
    fn test_invalid_inode_number() {
        // 这个测试需要一个实际的块设备，所以暂时跳过
        // 实际使用时，inode 编号为 0 应该返回错误
    }
}
