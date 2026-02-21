//! Inode 写入和更新操作
//!
//! 对应 lwext4 的 ext4_inode.c 中的 setter 函数

use crate::{
    block::{BlockDev, BlockDevice},
    block_group::get_block_group_desc_location,
    consts::*,
    error::Result,
    superblock::Superblock,
    types::{ext4_group_desc, ext4_inode},
};
use alloc::vec;

/// 写入 inode 到块设备
///
/// 对应 lwext4 中没有单独的函数，但是写回逻辑类似
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `inode_num` - inode 编号
/// * `inode` - inode 结构
pub fn write_inode<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &Superblock,
    inode_num: u32,
    inode: &ext4_inode,
) -> Result<()> {
    // 计算 inode 所在的块组
    let inodes_per_group = sb.inodes_per_group();
    let block_group = (inode_num - 1) / inodes_per_group;
    let index_in_group = (inode_num - 1) % inodes_per_group;

    // 使用统一的 GDT 定位函数读取块组描述符，支持 META_BG
    let block_size = sb.block_size() as u64;
    let (gdt_block, desc_offset_in_block) = get_block_group_desc_location(sb, block_group);
    let desc_offset = gdt_block * block_size + desc_offset_in_block;

    let mut desc_buf = vec![0u8; core::mem::size_of::<ext4_group_desc>()];
    bdev.read_bytes(desc_offset, &mut desc_buf)?;
    let desc: ext4_group_desc = crate::bytes::read_struct(&desc_buf)?;

    // 获取 inode 表的位置
    let inode_table_block = desc.inode_table();
    let inode_size = sb.inode_size() as u64;

    // 计算 inode 的字节偏移
    let inode_offset = inode_table_block * block_size + (index_in_group as u64) * inode_size;

    // 序列化 inode
    let inode_bytes = unsafe {
        core::slice::from_raw_parts(
            inode as *const ext4_inode as *const u8,
            inode_size as usize,
        )
    };

    // 写入 inode
    bdev.write_bytes(inode_offset, inode_bytes)?;

    Ok(())
}

/// Inode 更新操作
impl super::Inode {
    /// 获取可变的内部 inode 结构
    ///
    /// 允许直接修改 inode 字段
    pub fn inner_mut(&mut self) -> &mut ext4_inode {
        &mut self.inner
    }

    /// 将 inode 写回块设备
    ///
    /// 在写入前会自动更新校验和（如果启用）
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `sb` - superblock 引用
    pub fn write<D: BlockDevice>(&mut self, bdev: &mut BlockDev<D>, sb: &Superblock) -> Result<()> {
        // 在写入前更新校验和
        self.update_checksum(sb);

        write_inode(bdev, sb, self.inode_num, &self.inner)
    }

    /// 设置文件模式（类型 + 权限）
    ///
    /// 对应 lwext4 的 `ext4_inode_set_mode()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `mode` - 模式值（包含文件类型和权限位）
    pub fn set_mode(&mut self, _sb: &Superblock, mode: u32) {
        // 只使用低 16 位，ext4 标准 mode 是 16 位
        self.inner.mode = ((mode << 16) >> 16).to_le() as u16;

        // 注意：Hurd 系统的 mode_high 字段在当前简化的结构中不存在
        // 原始 lwext4 在 osd2.hurd2 中有 mode_high，但这里的实现没有包含该字段
    }

    /// 设置 UID（用户 ID）
    ///
    /// 对应 lwext4 的 `ext4_inode_set_uid()`
    ///
    /// # 参数
    ///
    /// * `uid` - 用户 ID
    pub fn set_uid(&mut self, uid: u32) {
        self.inner.uid = (uid as u16).to_le();
        self.inner.uid_high = (uid >> 16).to_le() as u16;
    }

    /// 设置 GID（组 ID）
    ///
    /// 对应 lwext4 的 `ext4_inode_set_gid()`
    ///
    /// # 参数
    ///
    /// * `gid` - 组 ID
    pub fn set_gid(&mut self, gid: u32) {
        self.inner.gid = (gid as u16).to_le();
        self.inner.gid_high = (gid >> 16).to_le() as u16;
    }

    /// 设置文件大小
    ///
    /// 对应 lwext4 的 `ext4_inode_set_size()`
    ///
    /// # 参数
    ///
    /// * `size` - 文件大小（字节）
    pub fn set_size(&mut self, size: u64) {
        self.inner.size_lo = ((size << 32) >> 32).to_le() as u32;
        self.inner.size_hi = (size >> 32).to_le() as u32;
    }

    /// 设置访问时间
    ///
    /// 对应 lwext4 的 `ext4_inode_set_access_time()`
    ///
    /// # 参数
    ///
    /// * `time` - Unix 时间戳（秒）
    pub fn set_access_time(&mut self, time: u32) {
        self.inner.atime = time.to_le();
    }

    /// 设置改变时间（ctime）
    ///
    /// 对应 lwext4 的 `ext4_inode_set_change_inode_time()`
    ///
    /// # 参数
    ///
    /// * `time` - Unix 时间戳（秒）
    pub fn set_change_time(&mut self, time: u32) {
        self.inner.ctime = time.to_le();
    }

    /// 设置修改时间
    ///
    /// 对应 lwext4 的 `ext4_inode_set_modif_time()`
    ///
    /// # 参数
    ///
    /// * `time` - Unix 时间戳（秒）
    pub fn set_modification_time(&mut self, time: u32) {
        self.inner.mtime = time.to_le();
    }

    /// 设置删除时间
    ///
    /// 对应 lwext4 的 `ext4_inode_set_del_time()`
    ///
    /// # 参数
    ///
    /// * `time` - Unix 时间戳（秒）
    pub fn set_deletion_time(&mut self, time: u32) {
        self.inner.dtime = time.to_le();
    }

    /// 设置硬链接计数
    ///
    /// 对应 lwext4 的 `ext4_inode_set_links_cnt()`
    ///
    /// # 参数
    ///
    /// * `count` - 链接计数
    pub fn set_links_count(&mut self, count: u16) {
        self.inner.links_count = count.to_le();
    }

    /// 设置标志
    ///
    /// 对应 lwext4 的 `ext4_inode_set_flags()`
    ///
    /// # 参数
    ///
    /// * `flags` - 标志位
    pub fn set_flags(&mut self, flags: u32) {
        self.inner.flags = flags.to_le();
    }

    /// 设置一个标志位
    ///
    /// 对应 lwext4 的 `ext4_inode_set_flag()`
    ///
    /// # 参数
    ///
    /// * `flag` - 要设置的标志位
    pub fn set_flag(&mut self, flag: u32) {
        let flags = self.flags();
        self.set_flags(flags | flag);
    }

    /// 清除一个标志位
    ///
    /// 对应 lwext4 的 `ext4_inode_clear_flag()`
    ///
    /// # 参数
    ///
    /// * `flag` - 要清除的标志位
    pub fn clear_flag(&mut self, flag: u32) {
        let flags = self.flags();
        self.set_flags(flags & !flag);
    }

    /// 设置代数（generation）
    ///
    /// 对应 lwext4 的 `ext4_inode_set_generation()`
    ///
    /// # 参数
    ///
    /// * `generation` - 代数值
    pub fn set_generation(&mut self, generation: u32) {
        self.inner.generation = generation.to_le();
    }

    /// 设置直接块指针
    ///
    /// 对应 lwext4 的 `ext4_inode_set_direct_block()`
    ///
    /// # 参数
    ///
    /// * `index` - 块索引（0-11）
    /// * `block` - 块号
    pub fn set_direct_block(&mut self, index: u32, block: u32) {
        if (index as usize) < EXT4_INODE_DIRECT_BLOCKS {
            self.inner.blocks[index as usize] = block.to_le();
        }
    }

    /// 设置间接块指针
    ///
    /// 对应 lwext4 的 `ext4_inode_set_indirect_block()`
    ///
    /// # 参数
    ///
    /// * `index` - 索引（0=单级，1=二级，2=三级）
    /// * `block` - 块号
    pub fn set_indirect_block(&mut self, index: u32, block: u32) {
        let idx = (EXT4_INODE_INDIRECT_BLOCK + index as usize).min(14);
        self.inner.blocks[idx] = block.to_le();
    }

    /// 设置额外 inode 大小
    ///
    /// 对应 lwext4 的 `ext4_inode_set_extra_isize()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `size` - 额外大小
    pub fn set_extra_isize(&mut self, sb: &Superblock, size: u16) {
        let inode_size = sb.inode_size();
        if inode_size > EXT4_GOOD_OLD_INODE_SIZE as u16 {
            self.inner.extra_isize = size.to_le();
        }
    }

    /// 设置 ACL 块号
    ///
    /// 对应 lwext4 的 `ext4_inode_set_file_acl()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `acl` - ACL 块号
    pub fn set_file_acl(&mut self, sb: &Superblock, acl: u64) {
        self.inner.file_acl_lo = ((acl << 32) >> 32).to_le() as u32;

        if sb.inner().creator_os == EXT4_SUPERBLOCK_OS_LINUX.to_le() {
            self.inner.file_acl_high = (acl >> 32).to_le() as u16;
        }
    }

    /// 设置设备号（用于设备文件）
    ///
    /// 对应 lwext4 的 `ext4_inode_set_dev()`
    ///
    /// # 参数
    ///
    /// * `dev` - 设备号
    pub fn set_dev(&mut self, dev: u32) {
        if dev & !0xFFFF != 0 {
            self.set_direct_block(1, dev);
        } else {
            self.set_direct_block(0, dev);
        }
    }

    /// 设置块计数（支持 HUGE_FILE）
    ///
    /// 对应 lwext4 的 `ext4_inode_set_blocks_count()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `count` - 块计数（以 512 字节为单位）
    ///
    /// # 返回
    ///
    /// 成功返回 `Ok(())`，如果count超出支持范围返回错误
    pub fn set_blocks_count(&mut self, sb: &Superblock, count: u64) -> Result<()> {
        use crate::error::{Error, ErrorKind};

        // 32 位最大值
        let max_32bit: u64 = 0xFFFFFFFF;

        if count <= max_32bit {
            // 可以用 32 位表示
            self.inner.blocks_count_lo = (count as u32).to_le();
            self.inner.blocks_high = 0;
            self.clear_flag(EXT4_INODE_FLAG_HUGE_FILE);
            return Ok(());
        }

        // 检查是否支持 HUGE_FILE
        if !sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_HUGE_FILE) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Filesystem does not support HUGE_FILE feature",
            ));
        }

        // 48 位最大值
        let max_48bit: u64 = 0xFFFFFFFFFFFF;

        if count <= max_48bit {
            // 可以用 48 位表示（不需要比例换算）
            self.inner.blocks_count_lo = (count as u32).to_le();
            self.inner.blocks_high = ((count >> 32) as u16).to_le();
            self.clear_flag(EXT4_INODE_FLAG_HUGE_FILE);
        } else {
            // 需要使用 HUGE_FILE 标志和比例换算
            let block_size = sb.block_size();
            let block_bits = inode_block_bits_count(block_size);

            self.set_flag(EXT4_INODE_FLAG_HUGE_FILE);

            // 从 512 字节单位转换为文件系统块单位
            let scaled_count = count >> (block_bits - 9);
            self.inner.blocks_count_lo = (scaled_count as u32).to_le();
            self.inner.blocks_high = ((scaled_count >> 32) as u16).to_le();
        }

        Ok(())
    }

    /// 获取 inode 校验和
    ///
    /// 对应 lwext4 的 `ext4_inode_get_csum()`
    pub fn get_checksum(&self, sb: &Superblock) -> u32 {
        super::checksum::get_checksum(sb, &self.inner)
    }

    /// 设置 inode 校验和
    ///
    /// 对应 lwext4 的 `ext4_inode_set_csum()`
    pub fn set_checksum(&mut self, sb: &Superblock, checksum: u32) {
        super::checksum::set_checksum(sb, &mut self.inner, checksum);
    }

    /// 计算 inode 校验和
    ///
    /// 计算当前 inode 的 CRC32C 校验和
    pub fn compute_checksum(&self, sb: &Superblock) -> u32 {
        super::checksum::compute_checksum(sb, self.inode_num, &self.inner)
    }

    /// 验证 inode 校验和
    ///
    /// 检查存储的校验和是否与计算的匹配
    pub fn verify_checksum(&self, sb: &Superblock) -> bool {
        super::checksum::verify_checksum(sb, self.inode_num, &self.inner)
    }

    /// 更新 inode 校验和
    ///
    /// 计算并设置 inode 的校验和（如果启用）
    pub fn update_checksum(&mut self, sb: &Superblock) {
        let csum = self.compute_checksum(sb);
        self.set_checksum(sb, csum);
    }
}

/// 计算块大小的位数
///
/// 对应 lwext4 的 `ext4_inode_block_bits_count()`
///
/// # 参数
///
/// * `block_size` - 块大小（字节）
///
/// # 返回
///
/// 块大小的位数（用于地址计算）
pub fn inode_block_bits_count(block_size: u32) -> u32 {
    let mut bits = 8;
    let mut size = block_size;

    while size > 256 {
        bits += 1;
        size >>= 1;
    }

    bits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ext4_sblock;

    #[test]
    fn test_setter_methods() {
        let mut inode_inner = ext4_inode::default();
        let mut inode = super::super::Inode {
            inner: inode_inner,
            inode_num: 2,
        };

        // 测试基础 setter
        inode.set_size(4096);
        assert_eq!(inode.file_size(), 4096);

        inode.set_links_count(5);
        assert_eq!(inode.links_count(), 5);

        inode.set_flags(EXT4_INODE_FLAG_EXTENTS);
        assert_eq!(inode.flags(), EXT4_INODE_FLAG_EXTENTS);
        assert!(inode.has_extents());
    }

    #[test]
    fn test_flag_operations() {
        let mut inode = super::super::Inode {
            inner: ext4_inode::default(),
            inode_num: 2,
        };

        // 设置标志
        inode.set_flag(EXT4_INODE_FLAG_EXTENTS);
        assert!(inode.flags() & EXT4_INODE_FLAG_EXTENTS != 0);

        // 再设置另一个标志
        inode.set_flag(EXT4_INODE_FLAG_IMMUTABLE);
        assert!(inode.flags() & EXT4_INODE_FLAG_EXTENTS != 0);
        assert!(inode.flags() & EXT4_INODE_FLAG_IMMUTABLE != 0);

        // 清除一个标志
        inode.clear_flag(EXT4_INODE_FLAG_EXTENTS);
        assert!(inode.flags() & EXT4_INODE_FLAG_EXTENTS == 0);
        assert!(inode.flags() & EXT4_INODE_FLAG_IMMUTABLE != 0);
    }

    #[test]
    fn test_block_pointers() {
        let mut inode = super::super::Inode {
            inner: ext4_inode::default(),
            inode_num: 2,
        };

        // 测试直接块指针
        inode.set_direct_block(0, 100);
        inode.set_direct_block(5, 200);
        assert_eq!(inode.get_direct_block(0), Some(100));
        assert_eq!(inode.get_direct_block(5), Some(200));

        // 测试间接块指针
        inode.set_indirect_block(0, 300);  // 单级间接
        assert_eq!(inode.get_indirect_block(), 300);
    }

    #[test]
    fn test_uid_gid() {
        let mut inode = super::super::Inode {
            inner: ext4_inode::default(),
            inode_num: 2,
        };

        // 测试 32 位 UID/GID
        inode.set_uid(0x12345678);
        inode.set_gid(0x87654321);

        assert_eq!(inode.uid(), 0x12345678);
        assert_eq!(inode.gid(), 0x87654321);
    }
}
