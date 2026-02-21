//! 块组描述符读取和查询操作

use crate::{
    block::{BlockDev, BlockDevice},
    consts::*,
    error::Result,
    superblock::Superblock,
    types::ext4_group_desc,
};
use alloc::vec;

/// 计算块组描述符的存储位置
///
/// 这个函数是所有读写块组描述符操作的基础，确保统一的 META_BG 支持
///
/// # 参数
///
/// * `sb` - superblock 引用
/// * `group_num` - 块组编号
///
/// # 返回
///
/// (块地址, 块内偏移) 元组
///
/// # 实现说明
///
/// 支持两种模式：
/// - 传统模式：所有块组描述符连续存储在 first_data_block + 1 位置
/// - META_BG 模式：块组描述符分散存储在各个 meta groups 中
///
/// 此函数被以下模块使用：
/// - `block_group/read.rs`: 读取块组描述符
/// - `block_group/write.rs`: 写入块组描述符
/// - `fs/block_group_ref.rs`: BlockGroupRef::get()
/// - `inode/write.rs`: write_inode()
pub fn get_block_group_desc_location(
    sb: &Superblock,
    group_num: u32,
) -> (u64, u64) {
    let block_size = sb.block_size() as u64;
    let desc_size = sb.group_desc_size() as u64;
    let first_data_block = sb.first_data_block() as u64;

    // 计算每个块可以容纳多少个描述符
    let desc_per_block = block_size / desc_size;

    // 检查是否启用 META_BG 特性
    let has_meta_bg = sb.has_incompat_feature(EXT4_FEATURE_INCOMPAT_META_BG);
    let first_meta_bg = u32::from_le(sb.inner().first_meta_bg);

    let gdt_block: u64;
    let desc_offset_in_block: u64;

    if has_meta_bg {
        // META_BG 模式
        let metagroup = (group_num as u64) / desc_per_block;

        // 检查是否在 META_BG 区域内
        if metagroup < first_meta_bg as u64 {
            // 在 first_meta_bg 之前，使用传统方式
            gdt_block = first_data_block + 1;
            desc_offset_in_block = (group_num as u64) * desc_size;
        } else {
            // 在 META_BG 区域内
            // META_BG 模式下，每个 metagroup 的块组描述符存储在该 metagroup 的特定位置
            // 描述符存储在 metagroup 中的第一个、第二个或最后一个块组

            let first_group_in_metagroup = metagroup * desc_per_block;
            let group_offset_in_metagroup = (group_num as u64) - first_group_in_metagroup;

            // 计算 metagroup 的起始块号
            let metagroup_start_block = first_group_in_metagroup * sb.blocks_per_group() as u64;

            // META_BG 的 GDT 存储位置：
            // - 第一个块组的描述符在 metagroup_start + 1
            // - 第二个块组的描述符在 metagroup_start + blocks_per_group + 1
            // - 最后一个块组的描述符在 metagroup_start + (desc_per_block - 1) * blocks_per_group + 1

            let gdt_offset_blocks = if group_offset_in_metagroup == 0 {
                1 // 第一个块组
            } else if group_offset_in_metagroup == 1 {
                sb.blocks_per_group() as u64 + 1 // 第二个块组
            } else if group_offset_in_metagroup == desc_per_block - 1 {
                (desc_per_block - 1) * sb.blocks_per_group() as u64 + 1 // 最后一个块组
            } else {
                // 其他块组的描述符存储在第一个块组的 GDT 中
                1
            };

            gdt_block = metagroup_start_block + gdt_offset_blocks;
            desc_offset_in_block = group_offset_in_metagroup * desc_size;
        }
    } else {
        // 传统模式：所有块组描述符连续存储
        gdt_block = first_data_block + 1 + ((group_num as u64) * desc_size) / block_size;
        desc_offset_in_block = ((group_num as u64) * desc_size) % block_size;
    }

    (gdt_block, desc_offset_in_block)
}

/// 读取块组描述符
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `group_num` - 块组编号
///
/// # 返回
///
/// 成功返回块组描述符
///
/// # 实现说明
///
/// 对应 lwext4 的 `ext4_fs_get_block_group_ref()`
///
/// 支持两种模式：
/// - 传统模式：所有块组描述符连续存储在 first_data_block + 1 位置
/// - META_BG 模式：块组描述符分散存储在各个 meta groups 中
pub fn read_block_group_desc<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &Superblock,
    group_num: u32,
) -> Result<ext4_group_desc> {
    let block_size = sb.block_size() as u64;

    // 使用统一的 GDT 定位函数
    let (gdt_block, desc_offset_in_block) = get_block_group_desc_location(sb, group_num);

    // 计算最终的字节偏移
    let desc_offset = gdt_block * block_size + desc_offset_in_block;

    // 读取块组描述符
    let mut desc_buf = vec![0u8; core::mem::size_of::<ext4_group_desc>()];
    bdev.read_bytes(desc_offset, &mut desc_buf)?;

    let desc: ext4_group_desc = crate::bytes::read_struct(&desc_buf)?;

    Ok(desc)
}

/// BlockGroup 包装器，提供高级操作
pub struct BlockGroup {
    pub(super) inner: ext4_group_desc,
    pub(super) group_num: u32,
}

impl BlockGroup {
    /// 从块设备加载块组描述符
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `sb` - superblock 引用
    /// * `group_num` - 块组编号
    pub fn load<D: BlockDevice>(
        bdev: &mut BlockDev<D>,
        sb: &Superblock,
        group_num: u32,
    ) -> Result<Self> {
        let inner = read_block_group_desc(bdev, sb, group_num)?;
        Ok(Self { inner, group_num })
    }

    /// 获取块组编号
    pub fn group_num(&self) -> u32 {
        self.group_num
    }

    /// 获取内部块组描述符结构的引用
    pub fn inner(&self) -> &ext4_group_desc {
        &self.inner
    }

    /// 获取块位图块号
    ///
    /// 对应 lwext4 的 `ext4_bg_get_block_bitmap()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    pub fn get_block_bitmap(&self, sb: &Superblock) -> u64 {
        let mut v = u32::from_le(self.inner.block_bitmap_lo) as u64;

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            v |= (u32::from_le(self.inner.block_bitmap_hi) as u64) << 32;
        }

        v
    }

    /// 获取 inode 位图块号
    ///
    /// 对应 lwext4 的 `ext4_bg_get_inode_bitmap()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    pub fn get_inode_bitmap(&self, sb: &Superblock) -> u64 {
        let mut v = u32::from_le(self.inner.inode_bitmap_lo) as u64;

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            v |= (u32::from_le(self.inner.inode_bitmap_hi) as u64) << 32;
        }

        v
    }

    /// 获取 inode 表起始块号
    ///
    /// 对应 lwext4 的 `ext4_bg_get_inode_table_first_block()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    pub fn get_inode_table_first_block(&self, sb: &Superblock) -> u64 {
        let mut v = u32::from_le(self.inner.inode_table_lo) as u64;

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            v |= (u32::from_le(self.inner.inode_table_hi) as u64) << 32;
        }

        v
    }

    /// 获取空闲块数
    ///
    /// 对应 lwext4 的 `ext4_bg_get_free_blocks_count()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    pub fn get_free_blocks_count(&self, sb: &Superblock) -> u32 {
        let mut v = u16::from_le(self.inner.free_blocks_count_lo) as u32;

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            v |= (u16::from_le(self.inner.free_blocks_count_hi) as u32) << 16;
        }

        v
    }

    /// 获取空闲 inode 数
    ///
    /// 对应 lwext4 的 `ext4_bg_get_free_inodes_count()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    pub fn get_free_inodes_count(&self, sb: &Superblock) -> u32 {
        let mut v = u16::from_le(self.inner.free_inodes_count_lo) as u32;

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            v |= (u16::from_le(self.inner.free_inodes_count_hi) as u32) << 16;
        }

        v
    }

    /// 获取已使用的目录数
    ///
    /// 对应 lwext4 的 `ext4_bg_get_used_dirs_count()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    pub fn get_used_dirs_count(&self, sb: &Superblock) -> u32 {
        let mut v = u16::from_le(self.inner.used_dirs_count_lo) as u32;

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            v |= (u16::from_le(self.inner.used_dirs_count_hi) as u32) << 16;
        }

        v
    }

    /// 获取未使用的 inode 数
    ///
    /// 对应 lwext4 的 `ext4_bg_get_itable_unused()`
    ///
    /// # 参数
    ///
    /// * `sb` - superblock 引用
    pub fn get_itable_unused(&self, sb: &Superblock) -> u32 {
        let mut v = u16::from_le(self.inner.itable_unused_lo) as u32;

        if sb.group_desc_size() > EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            v |= (u16::from_le(self.inner.itable_unused_hi) as u32) << 16;
        }

        v
    }

    /// 检查块组是否有指定标志
    ///
    /// 对应 lwext4 的 `ext4_bg_has_flag()`
    ///
    /// # 参数
    ///
    /// * `flag` - 要检查的标志
    pub fn has_flag(&self, flag: u16) -> bool {
        (u16::from_le(self.inner.flags) & flag) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_group_getters() {
        let mut desc = ext4_group_desc::default();

        // 设置测试数据
        desc.block_bitmap_lo = 100u32.to_le();
        desc.inode_bitmap_lo = 200u32.to_le();
        desc.inode_table_lo = 300u32.to_le();
        desc.free_blocks_count_lo = 1000u16.to_le();
        desc.free_inodes_count_lo = 2000u16.to_le();
        desc.used_dirs_count_lo = 50u16.to_le();
        desc.itable_unused_lo = 500u16.to_le();

        let bg = BlockGroup {
            inner: desc,
            group_num: 0,
        };

        // 创建一个测试用的 superblock（使用最小描述符大小）
        let mut sb_inner = crate::types::ext4_sblock::default();
        sb_inner.desc_size = (EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE as u16).to_le();
        let sb = Superblock::new(sb_inner);

        assert_eq!(bg.get_block_bitmap(&sb), 100);
        assert_eq!(bg.get_inode_bitmap(&sb), 200);
        assert_eq!(bg.get_inode_table_first_block(&sb), 300);
        assert_eq!(bg.get_free_blocks_count(&sb), 1000);
        assert_eq!(bg.get_free_inodes_count(&sb), 2000);
        assert_eq!(bg.get_used_dirs_count(&sb), 50);
        assert_eq!(bg.get_itable_unused(&sb), 500);
    }

    #[test]
    fn test_block_group_flags() {
        let mut desc = ext4_group_desc::default();
        desc.flags = 0x0005u16.to_le(); // 设置bit 0和bit 2

        let bg = BlockGroup {
            inner: desc,
            group_num: 0,
        };

        assert!(bg.has_flag(0x0001));
        assert!(!bg.has_flag(0x0002));
        assert!(bg.has_flag(0x0004));
        assert!(!bg.has_flag(0x0008));
    }
}
