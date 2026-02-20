//! 块组描述符写入和修改操作

use crate::{
    block::{BlockDev, BlockDevice},
    consts::*,
    error::Result,
    superblock::Superblock,
    types::ext4_group_desc,
};

use super::{BlockGroup, get_block_group_desc_location};

/// 写入块组描述符到块设备
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `group_num` - 块组编号
/// * `desc` - 块组描述符引用
///
/// # 返回
///
/// 成功返回 Ok(())
pub fn write_block_group_desc<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &Superblock,
    group_num: u32,
    desc: &ext4_group_desc,
) -> Result<()> {
    let block_size = sb.block_size() as u64;

    // 使用统一的 GDT 定位函数，支持 META_BG
    let (gdt_block, desc_offset_in_block) = get_block_group_desc_location(sb, group_num);

    // 计算描述符的偏移
    let desc_offset = gdt_block * block_size + desc_offset_in_block;

    // 将描述符转换为字节数组
    let desc_bytes = unsafe {
        core::slice::from_raw_parts(
            desc as *const ext4_group_desc as *const u8,
            core::mem::size_of::<ext4_group_desc>(),
        )
    };

    // 写入块组描述符
    bdev.write_bytes(desc_offset, desc_bytes)?;

    Ok(())
}

impl BlockGroup {
    /// 获取内部块组描述符的可变引用
    pub(crate) fn inner_mut(&mut self) -> &mut ext4_group_desc {
        &mut self.inner
    }

    /// 设置块位图块号
    ///
    /// 对应 lwext4 的 `ext4_bg_set_block_bitmap()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `block` - 块位图块号
    pub fn set_block_bitmap(&mut self, sb: &Superblock, block: u64) {
        self.inner.block_bitmap_lo = (block as u32).to_le();

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            self.inner.block_bitmap_hi = ((block >> 32) as u32).to_le();
        }
    }

    /// 设置 inode 位图块号
    ///
    /// 对应 lwext4 的 `ext4_bg_set_inode_bitmap()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `block` - inode 位图块号
    pub fn set_inode_bitmap(&mut self, sb: &Superblock, block: u64) {
        self.inner.inode_bitmap_lo = (block as u32).to_le();

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            self.inner.inode_bitmap_hi = ((block >> 32) as u32).to_le();
        }
    }

    /// 设置 inode 表起始块号
    ///
    /// 对应 lwext4 的 `ext4_bg_set_inode_table_first_block()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `block` - inode 表起始块号
    pub fn set_inode_table_first_block(&mut self, sb: &Superblock, block: u64) {
        self.inner.inode_table_lo = (block as u32).to_le();

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            self.inner.inode_table_hi = ((block >> 32) as u32).to_le();
        }
    }

    /// 设置空闲块数
    ///
    /// 对应 lwext4 的 `ext4_bg_set_free_blocks_count()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `count` - 空闲块数
    pub fn set_free_blocks_count(&mut self, sb: &Superblock, count: u32) {
        self.inner.free_blocks_count_lo = (count as u16).to_le();

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            self.inner.free_blocks_count_hi = ((count >> 16) as u16).to_le();
        }
    }

    /// 设置空闲 inode 数
    ///
    /// 对应 lwext4 的 `ext4_bg_set_free_inodes_count()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `count` - 空闲 inode 数
    pub fn set_free_inodes_count(&mut self, sb: &Superblock, count: u32) {
        self.inner.free_inodes_count_lo = (count as u16).to_le();

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            self.inner.free_inodes_count_hi = ((count >> 16) as u16).to_le();
        }
    }

    /// 设置已使用的目录数
    ///
    /// 对应 lwext4 的 `ext4_bg_set_used_dirs_count()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `count` - 已使用的目录数
    pub fn set_used_dirs_count(&mut self, sb: &Superblock, count: u32) {
        self.inner.used_dirs_count_lo = (count as u16).to_le();

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            self.inner.used_dirs_count_hi = ((count >> 16) as u16).to_le();
        }
    }

    /// 设置未使用的 inode 数
    ///
    /// 对应 lwext4 的 `ext4_bg_set_itable_unused()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    /// * `count` - 未使用的 inode 数
    pub fn set_itable_unused(&mut self, sb: &Superblock, count: u32) {
        self.inner.itable_unused_lo = (count as u16).to_le();

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            self.inner.itable_unused_hi = ((count >> 16) as u16).to_le();
        }
    }

    /// 设置块组校验和
    ///
    /// 对应 lwext4 的 `ext4_bg_set_checksum()`
    ///
    /// # 参数
    ///
    /// * `checksum` - 校验和值
    pub fn set_checksum(&mut self, checksum: u16) {
        self.inner.checksum = checksum.to_le();
    }

    /// 设置块组标志
    ///
    /// 对应 lwext4 的 `ext4_bg_set_flag()`
    ///
    /// # 参数
    ///
    /// * `flag` - 要设置的标志
    pub fn set_flag(&mut self, flag: u16) {
        let mut flags = u16::from_le(self.inner.flags);
        flags |= flag;
        self.inner.flags = flags.to_le();
    }

    /// 清除块组标志
    ///
    /// 对应 lwext4 的 `ext4_bg_clear_flag()`
    ///
    /// # 参数
    ///
    /// * `flag` - 要清除的标志
    pub fn clear_flag(&mut self, flag: u16) {
        let mut flags = u16::from_le(self.inner.flags);
        flags &= !flag;
        self.inner.flags = flags.to_le();
    }

    /// 将块组描述符写回块设备
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `sb` - superblock 引用
    ///
    /// # 返回
    ///
    /// 成功返回 Ok(())
    pub fn write<D: BlockDevice>(
        &self,
        bdev: &mut BlockDev<D>,
        sb: &Superblock,
    ) -> Result<()> {
        write_block_group_desc(bdev, sb, self.group_num, &self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ext4_sblock;
    use crate::consts::{EXT4_FEATURE_INCOMPAT_64BIT, EXT4_GROUP_DESC_SIZE_64};

    #[test]
    fn test_block_group_setters() {
        let desc = ext4_group_desc::default();
        let mut bg = BlockGroup {
            inner: desc,
            group_num: 0,
        };

        // 创建测试用的 superblock
        let mut sb_inner = ext4_sblock::default();
        sb_inner.desc_size = (EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE as u16).to_le();
        let sb = Superblock::new(sb_inner);

        // 测试设置各种值
        bg.set_block_bitmap(&sb, 12345);
        assert_eq!(bg.get_block_bitmap(&sb), 12345);

        bg.set_inode_bitmap(&sb, 23456);
        assert_eq!(bg.get_inode_bitmap(&sb), 23456);

        bg.set_inode_table_first_block(&sb, 34567);
        assert_eq!(bg.get_inode_table_first_block(&sb), 34567);

        bg.set_free_blocks_count(&sb, 5000);
        assert_eq!(bg.get_free_blocks_count(&sb), 5000);

        bg.set_free_inodes_count(&sb, 6000);
        assert_eq!(bg.get_free_inodes_count(&sb), 6000);

        bg.set_used_dirs_count(&sb, 100);
        assert_eq!(bg.get_used_dirs_count(&sb), 100);

        bg.set_itable_unused(&sb, 500);
        assert_eq!(bg.get_itable_unused(&sb), 500);
    }

    #[test]
    fn test_block_group_flags_operations() {
        let desc = ext4_group_desc::default();
        let mut bg = BlockGroup {
            inner: desc,
            group_num: 0,
        };

        // 测试设置标志
        bg.set_flag(0x0001);
        assert!(bg.has_flag(0x0001));

        bg.set_flag(0x0004);
        assert!(bg.has_flag(0x0004));
        assert!(bg.has_flag(0x0001)); // 之前的标志应该保留

        // 测试清除标志
        bg.clear_flag(0x0001);
        assert!(!bg.has_flag(0x0001));
        assert!(bg.has_flag(0x0004)); // 其他标志应该保留
    }

    #[test]
    fn test_block_group_checksum() {
        let desc = ext4_group_desc::default();
        let mut bg = BlockGroup {
            inner: desc,
            group_num: 0,
        };

        bg.set_checksum(0x1234);
        assert_eq!(u16::from_le(bg.inner.checksum), 0x1234);
    }

    #[test]
    fn test_block_group_64bit_support() {
        let desc = ext4_group_desc::default();
        let mut bg = BlockGroup {
            inner: desc,
            group_num: 0,
        };

        // 创建一个64位描述符大小的 superblock
        let mut sb_inner = ext4_sblock::default();
        sb_inner.feature_incompat = (EXT4_FEATURE_INCOMPAT_64BIT as u32).to_le();
        sb_inner.desc_size = (EXT4_GROUP_DESC_SIZE_64 as u16).to_le();
        let sb = Superblock::new(sb_inner);

        // 测试大于32位的值
        let large_block = 0x1_0000_1234u64; // 超过32位
        bg.set_block_bitmap(&sb, large_block);
        assert_eq!(bg.get_block_bitmap(&sb), large_block);

        let large_inode_bitmap = 0x2_0000_5678u64;
        bg.set_inode_bitmap(&sb, large_inode_bitmap);
        assert_eq!(bg.get_inode_bitmap(&sb), large_inode_bitmap);
    }
}
