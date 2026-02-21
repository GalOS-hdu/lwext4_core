//! Superblock 读取和验证

use crate::{
    block::{BlockDev, BlockDevice},
    consts::*,
    error::{Error, ErrorKind, Result},
    types::ext4_sblock,
};
use crate::consts::{
    EXT4_FEATURE_RO_COMPAT_METADATA_CSUM,
    EXT4_FEATURE_RO_COMPAT_SPARSE_SUPER,
    EXT4_FEATURE_INCOMPAT_META_BG,
    EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE,
    EXT4_MAX_BLOCK_GROUP_DESCRIPTOR_SIZE,
};
use alloc::vec;

/// 从块设备读取 superblock
///
/// # 参数
///
/// * `bdev` - 块设备引用
///
/// # 返回
///
/// 成功返回 superblock 结构
pub fn read_superblock<D: BlockDevice>(bdev: &mut BlockDev<D>) -> Result<ext4_sblock> {
    let mut sb_buf = vec![0u8; EXT4_SUPERBLOCK_SIZE];

    // 读取 superblock（从偏移 1024 开始）
    bdev.read_bytes(EXT4_SUPERBLOCK_OFFSET, &mut sb_buf)?;

    // 解析 superblock
    let sb: ext4_sblock = crate::bytes::read_struct(&sb_buf)?;

    // 验证魔数
    if !sb.is_valid() {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "Invalid ext4 superblock magic number",
        ));
    }

    Ok(sb)
}

/// Superblock 包装器，提供高级操作
pub struct Superblock {
    pub(super) inner: ext4_sblock,
}

impl Superblock {
    /// 从 ext4_sblock 创建 Superblock（主要用于测试）
    pub fn new(inner: ext4_sblock) -> Self {
        Self { inner }
    }

    /// 从块设备加载 superblock
    pub fn load<D: BlockDevice>(bdev: &mut BlockDev<D>) -> Result<Self> {
        let inner = read_superblock(bdev)?;
        Ok(Self { inner })
    }

    /// 获取内部 superblock 结构的引用
    pub fn inner(&self) -> &ext4_sblock {
        &self.inner
    }

    /// 获取块大小
    pub fn block_size(&self) -> u32 {
        self.inner.block_size()
    }

    /// 获取 inode 大小
    pub fn inode_size(&self) -> u16 {
        self.inner.inode_size()
    }

    /// 获取总块数
    pub fn blocks_count(&self) -> u64 {
        self.inner.blocks_count()
    }

    /// 获取空闲块数
    pub fn free_blocks_count(&self) -> u64 {
        self.inner.free_blocks_count()
    }

    /// 获取总 inode 数
    pub fn inodes_count(&self) -> u32 {
        u32::from_le(self.inner.inodes_count)
    }

    /// 获取空闲 inode 数
    pub fn free_inodes_count(&self) -> u32 {
        u32::from_le(self.inner.free_inodes_count)
    }

    /// 获取每组块数
    pub fn blocks_per_group(&self) -> u32 {
        u32::from_le(self.inner.blocks_per_group)
    }

    /// 获取每组 inode 数
    pub fn inodes_per_group(&self) -> u32 {
        u32::from_le(self.inner.inodes_per_group)
    }

    /// 获取块组数量
    pub fn block_group_count(&self) -> u32 {
        self.inner.block_group_count()
    }

    /// 获取第一个数据块
    pub fn first_data_block(&self) -> u32 {
        u32::from_le(self.inner.first_data_block)
    }

    /// 检查是否支持某个兼容特性
    pub fn has_compat_feature(&self, feature: u32) -> bool {
        (u32::from_le(self.inner.feature_compat) & feature) != 0
    }

    /// 检查是否支持某个不兼容特性
    pub fn has_incompat_feature(&self, feature: u32) -> bool {
        (u32::from_le(self.inner.feature_incompat) & feature) != 0
    }

    /// 检查是否支持某个只读兼容特性
    pub fn has_ro_compat_feature(&self, feature: u32) -> bool {
        (u32::from_le(self.inner.feature_ro_compat) & feature) != 0
    }

    /// 检查 superblock flags
    pub fn has_flag(&self, flag: u32) -> bool {
        (u32::from_le(self.inner.flags) & flag) != 0
    }

    /// 获取 hash seed（用于 HTree）
    pub fn hash_seed(&self) -> [u32; 4] {
        [
            u32::from_le(self.inner.hash_seed[0]),
            u32::from_le(self.inner.hash_seed[1]),
            u32::from_le(self.inner.hash_seed[2]),
            u32::from_le(self.inner.hash_seed[3]),
        ]
    }

    /// 检查是否使用 extent
    pub fn has_extents(&self) -> bool {
        self.has_incompat_feature(EXT4_FEATURE_INCOMPAT_EXTENTS)
    }

    /// 检查是否是 64 位文件系统
    pub fn is_64bit(&self) -> bool {
        self.has_incompat_feature(EXT4_FEATURE_INCOMPAT_64BIT)
    }

    /// 获取块组描述符大小
    pub fn group_desc_size(&self) -> usize {
        if self.is_64bit() {
            let size = u16::from_le(self.inner.desc_size) as usize;
            if size > 0 {
                size
            } else {
                EXT4_GROUP_DESC_SIZE_64
            }
        } else {
            EXT4_GROUP_DESC_SIZE
        }
    }

    /// 获取卷名称（UTF-8 字符串）
    pub fn volume_name(&self) -> Option<&str> {
        // 找到第一个 null 字节
        let len = self.inner.volume_name
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.inner.volume_name.len());

        core::str::from_utf8(&self.inner.volume_name[..len]).ok()
    }

    /// 获取 UUID
    ///
    /// 返回 16 字节的 UUID
    pub fn uuid(&self) -> &[u8; 16] {
        &self.inner.uuid
    }

    /// 检查是否启用元数据校验和
    ///
    /// 对应 EXT4_FEATURE_RO_COMPAT_METADATA_CSUM 特性
    pub fn has_metadata_csum(&self) -> bool {
        self.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM)
    }

    /// 验证文件系统状态
    pub fn is_clean(&self) -> bool {
        const EXT4_VALID_FS: u16 = 0x0001;
        (u16::from_le(self.inner.state) & EXT4_VALID_FS) != 0
    }

    /// 完整的 superblock 验证
    ///
    /// 对应 lwext4 的 `ext4_sb_check()`
    ///
    /// 检查所有关键字段的有效性，包括：
    /// - 魔数验证
    /// - 计数字段非零检查
    /// - 大小字段范围检查
    /// - 块组描述符大小验证
    /// - 校验和验证（如果启用）
    ///
    /// # 返回
    ///
    /// 如果所有检查都通过返回 `Ok(())`，否则返回错误
    pub fn check(&self) -> Result<()> {
        // 1. 检查魔数
        if !self.inner.is_valid() {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Invalid ext4 superblock magic number",
            ));
        }

        // 2. 检查 inodes_count 非零
        if self.inodes_count() == 0 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Superblock inodes_count is zero",
            ));
        }

        // 3. 检查 blocks_count 非零
        if self.blocks_count() == 0 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Superblock blocks_count is zero",
            ));
        }

        // 4. 检查 blocks_per_group 非零
        if self.blocks_per_group() == 0 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Superblock blocks_per_group is zero",
            ));
        }

        // 5. 检查 inodes_per_group 非零
        if self.inodes_per_group() == 0 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Superblock inodes_per_group is zero",
            ));
        }

        // 6. 检查 inode_size 最小值（至少 128 字节）
        if self.inode_size() < 128 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Superblock inode_size is less than 128",
            ));
        }

        // 7. 检查 first_inode 最小值（至少 11）
        if self.first_data_block() < 11 && self.inodes_count() > 10 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Superblock first_inode is less than 11",
            ));
        }

        // 8. 检查块组描述符大小范围
        let desc_size = self.group_desc_size();
        if desc_size < EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Block group descriptor size too small",
            ));
        }
        if desc_size > EXT4_MAX_BLOCK_GROUP_DESCRIPTOR_SIZE {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Block group descriptor size too large",
            ));
        }

        // 9. 校验和验证（如果启用了 METADATA_CSUM 特性）
        // 注意：这里调用 verify_checksum()，实际实现会在 Phase 2 完成
        // 已完成, 依赖crc32c
        if !self.verify_checksum() {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Superblock checksum verification failed",
            ));
        }

        Ok(())
    }

    /// 验证 superblock 校验和
    ///
    /// 对应 lwext4 的 `ext4_sb_verify_csum()`
    pub fn verify_checksum(&self) -> bool {
        super::checksum::verify_checksum(&self.inner)
    }

    /// 计算 superblock 校验和
    ///
    /// 对应 lwext4 的 `ext4_sb_csum()`
    pub fn compute_checksum(&self) -> u32 {
        super::checksum::compute_checksum(&self.inner)
    }

    /// 计算指定块组中的块数量
    ///
    /// 对应 lwext4 的 `ext4_blocks_in_group_cnt()`
    ///
    /// 最后一个块组的块数可能不足一个完整组，需要单独计算
    ///
    /// # 参数
    ///
    /// * `bgid` - 块组 ID
    pub fn blocks_in_group_cnt(&self, bgid: u32) -> u32 {
        let block_group_count = self.block_group_count();
        let blocks_per_group = self.blocks_per_group();
        let total_blocks = self.blocks_count();

        if bgid < block_group_count - 1 {
            blocks_per_group
        } else {
            // 最后一个块组，可能不足完整大小
            (total_blocks - ((block_group_count as u64 - 1) * blocks_per_group as u64)) as u32
        }
    }

    /// 计算指定块组中的 inode 数量
    ///
    /// 对应 lwext4 的 `ext4_inodes_in_group_cnt()`
    ///
    /// 最后一个块组的 inode 数可能不足一个完整组，需要单独计算
    ///
    /// # 参数
    ///
    /// * `bgid` - 块组 ID
    pub fn inodes_in_group_cnt(&self, bgid: u32) -> u32 {
        let block_group_count = self.block_group_count();
        let inodes_per_group = self.inodes_per_group();
        let total_inodes = self.inodes_count();

        if bgid < block_group_count - 1 {
            inodes_per_group
        } else {
            // 最后一个块组，可能不足完整大小
            total_inodes - ((block_group_count - 1) * inodes_per_group)
        }
    }

    /// 判断块组是否为稀疏超级块组
    ///
    /// 对应 lwext4 的 `ext4_sb_sparse()`
    ///
    /// 稀疏超级块特性：只在特定块组（0, 1, 和 3/5/7 的幂次）存储超级块备份
    ///
    /// # 参数
    ///
    /// * `group` - 块组号
    ///
    /// # 返回
    ///
    /// 如果是稀疏超级块组返回 `true`
    pub fn is_sparse_group(group: u32) -> bool {
        // 块组 0 和 1 总是包含超级块
        if group <= 1 {
            return true;
        }

        // 偶数组不包含超级块
        if (group & 1) == 0 {
            return false;
        }

        // 检查是否是 3、5、7 的幂次
        is_power_of(group, 7) || is_power_of(group, 5) || is_power_of(group, 3)
    }

    /// 判断超级块是否存在于指定的块组中
    ///
    /// 对应 lwext4 的 `ext4_sb_is_super_in_bg()`
    ///
    /// # 参数
    ///
    /// * `group` - 块组号
    ///
    /// # 返回
    ///
    /// 如果超级块存在于该块组返回 `true`
    pub fn has_super_in_bg(&self, group: u32) -> bool {
        // 检查是否启用了稀疏超级块特性
        if self.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_SPARSE_SUPER) {
            // 如果启用，只有稀疏组才有超级块
            Self::is_sparse_group(group)
        } else {
            // 如果未启用，所有块组都有超级块
            true
        }
    }

    /// 计算指定块组的 GDT 块数（META_BG 模式）
    ///
    /// 对应 lwext4 的 `ext4_bg_num_gdb_meta()`
    ///
    /// # 参数
    ///
    /// * `group` - 块组号
    fn num_gdb_meta(&self, group: u32) -> u32 {
        let dsc_per_block = self.block_size() / self.group_desc_size() as u32;
        let metagroup = group / dsc_per_block;
        let first = metagroup * dsc_per_block;
        let last = first + dsc_per_block - 1;

        if group == first || group == first + 1 || group == last {
            1
        } else {
            0
        }
    }

    /// 计算指定块组的 GDT 块数（非 META_BG 模式）
    ///
    /// 对应 lwext4 的 `ext4_bg_num_gdb_nometa()`
    ///
    /// # 参数
    ///
    /// * `group` - 块组号
    fn num_gdb_nometa(&self, group: u32) -> u32 {
        // 如果该块组不包含超级块，就没有 GDT
        if !self.has_super_in_bg(group) {
            return 0;
        }

        let dsc_per_block = self.block_size() / self.group_desc_size() as u32;
        let db_count = self.block_group_count().div_ceil(dsc_per_block);

        // 如果启用了 META_BG，返回 first_meta_bg
        if self.has_incompat_feature(EXT4_FEATURE_INCOMPAT_META_BG) {
            u32::from_le(self.inner.first_meta_bg)
        } else {
            db_count
        }
    }

    /// 计算指定块组的 GDT 块数
    ///
    /// 对应 lwext4 的 `ext4_bg_num_gdb()`
    ///
    /// # 参数
    ///
    /// * `group` - 块组号
    ///
    /// # 返回
    ///
    /// 该块组中 GDT 占用的块数
    pub fn num_gdb(&self, group: u32) -> u32 {
        let dsc_per_block = self.block_size() / self.group_desc_size() as u32;
        let first_meta_bg = u32::from_le(self.inner.first_meta_bg);
        let metagroup = group / dsc_per_block;

        // 检查是否使用 META_BG 以及当前组是否在 META_BG 区域
        if !self.has_incompat_feature(EXT4_FEATURE_INCOMPAT_META_BG) || metagroup < first_meta_bg {
            self.num_gdb_nometa(group)
        } else {
            self.num_gdb_meta(group)
        }
    }

    /// 计算指定块组中基础元数据簇数量
    ///
    /// 对应 lwext4 的 `ext4_num_base_meta_clusters()`
    ///
    /// 计算超级块 + GDT + 保留 GDT 占用的簇数
    ///
    /// # 参数
    ///
    /// * `block_group` - 块组号
    ///
    /// # 返回
    ///
    /// 占用的簇数量
    pub fn num_base_meta_clusters(&self, block_group: u32) -> u32 {
        let dsc_per_block = self.block_size() / self.group_desc_size() as u32;

        // 检查当前块组是否包含超级块
        let mut num = if self.has_super_in_bg(block_group) { 1 } else { 0 };

        // 计算 GDT 块数
        if !self.has_incompat_feature(EXT4_FEATURE_INCOMPAT_META_BG)
            || block_group < u32::from_le(self.inner.first_meta_bg) * dsc_per_block
        {
            // 传统模式或在 META_BG 之前
            if num > 0 {
                // 有超级块，加上 GDT 和保留 GDT
                num += self.num_gdb(block_group);
                num += u16::from_le(self.inner.reserved_gdt_blocks) as u32;
            }
        } else {
            // META_BG 模式
            num += self.num_gdb(block_group);
        }

        // 转换为簇数
        let log_cluster_size = u32::from_le(self.inner.log_cluster_size);
        let cluster_ratio = 1u32 << log_cluster_size;

        // 向上取整
        (num + cluster_ratio - 1) >> log_cluster_size
    }
}

/// 判断一个数是否为另一个数的幂
///
/// 对应 lwext4 的 `is_power_of()`
///
/// # 参数
///
/// * `a` - 被检查的数
/// * `b` - 底数
fn is_power_of(mut a: u32, b: u32) -> bool {
    loop {
        if a < b {
            return false;
        }
        if a == b {
            return true;
        }
        if (a % b) != 0 {
            return false;
        }
        a /= b;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_superblock_validation() {
        let mut sb = ext4_sblock::default();

        // 应该无效（魔数为 0）
        assert!(!sb.is_valid());

        // 设置正确的魔数
        sb.magic = EXT4_SUPERBLOCK_MAGIC.to_le();
        assert!(sb.is_valid());
    }

    #[test]
    fn test_superblock_helpers() {
        let mut sb = ext4_sblock::default();
        sb.magic = EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.log_block_size = 2u32.to_le(); // 4096 = 1024 << 2
        sb.blocks_count_lo = 1000u32.to_le();
        sb.blocks_per_group = 100u32.to_le();

        assert_eq!(sb.block_size(), 4096);
        assert_eq!(sb.blocks_count(), 1000);
        assert_eq!(sb.block_group_count(), 10);
    }

    #[test]
    fn test_blocks_in_group_cnt() {
        let mut sb = ext4_sblock::default();
        sb.magic = EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.log_block_size = 2u32.to_le();
        sb.blocks_count_lo = 950u32.to_le(); // 不能被 100 整除
        sb.blocks_per_group = 100u32.to_le();

        let superblock = Superblock { inner: sb };

        // 总共 10 个块组（950 / 100 = 9 余 50）
        assert_eq!(superblock.block_group_count(), 10);

        // 前 9 个块组都是完整的 100 块
        for bgid in 0..9 {
            assert_eq!(superblock.blocks_in_group_cnt(bgid), 100);
        }

        // 最后一个块组只有 50 块
        assert_eq!(superblock.blocks_in_group_cnt(9), 50);
    }

    #[test]
    fn test_inodes_in_group_cnt() {
        let mut sb = ext4_sblock::default();
        sb.magic = EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.log_block_size = 2u32.to_le();
        sb.blocks_count_lo = 1000u32.to_le();
        sb.blocks_per_group = 100u32.to_le();
        sb.inodes_count = 9050u32.to_le(); // 不能被 1000 整除
        sb.inodes_per_group = 1000u32.to_le();

        let superblock = Superblock { inner: sb };

        // 总共 10 个块组
        assert_eq!(superblock.block_group_count(), 10);

        // 前 9 个块组都是完整的 1000 个 inode
        for bgid in 0..9 {
            assert_eq!(superblock.inodes_in_group_cnt(bgid), 1000);
        }

        // 最后一个块组只有 50 个 inode (9050 - 9000)
        assert_eq!(superblock.inodes_in_group_cnt(9), 50);
    }
}
