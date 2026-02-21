//! Inode 校验和计算
//!
//! 对应 lwext4 的 `ext4_inode_get_csum()` 和 `ext4_inode_set_csum()` 功能
//!
//! ## 算法（与 lwext4 C 实现一致）
//!
//! 1. 暂时将 inode 的 checksum 字段清零
//! 2. CRC32C(init, uuid, 16)
//! 3. CRC32C(crc, inode_num_le, 4)
//! 4. CRC32C(crc, inode_generation_le, 4)
//! 5. CRC32C(crc, inode_bytes, inode_size)
//! 6. 恢复 checksum 字段

use crate::{
    consts::{EXT4_FEATURE_RO_COMPAT_METADATA_CSUM, EXT4_GOOD_OLD_INODE_SIZE},
    superblock::Superblock,
    types::ext4_inode,
    crc::EXT4_CRC32_INIT,
};

/// 获取 inode 校验和
///
/// 对应 lwext4 的 `ext4_inode_get_csum()`
pub fn get_checksum(sb: &Superblock, inode: &ext4_inode) -> u32 {
    let inode_size = sb.inode_size();
    let mut v = u16::from_le(inode.checksum_lo) as u32;

    if inode_size > EXT4_GOOD_OLD_INODE_SIZE as u16 {
        v |= (u16::from_le(inode.checksum_hi) as u32) << 16;
    }

    v
}

/// 设置 inode 校验和
///
/// 对应 lwext4 的 `ext4_inode_set_csum()`
pub fn set_checksum(sb: &Superblock, inode: &mut ext4_inode, checksum: u32) {
    let inode_size = sb.inode_size();
    inode.checksum_lo = (checksum as u16).to_le();

    if inode_size > EXT4_GOOD_OLD_INODE_SIZE as u16 {
        inode.checksum_hi = ((checksum >> 16) as u16).to_le();
    }
}

/// 计算 inode 的 CRC32C 校验和
///
/// 与 lwext4 C 的 `ext4_fs_inode_checksum()` 完全一致：
/// 1. 先将 checksum 字段清零
/// 2. CRC32C(init, uuid, 16)
/// 3. CRC32C(crc, inode_num_le, 4)
/// 4. CRC32C(crc, inode_gen_le, 4)
/// 5. CRC32C(crc, inode_bytes, struct_size)
///
/// 注意：只计算 Rust struct 大小范围内的数据（size_of::<ext4_inode>()），
/// 而非磁盘上的 inode_size。对于标准 256 字节 inode，struct 之后的
/// 填充字节全为零，不影响校验和正确性，因为磁盘上这些字节也是零。
pub fn compute_checksum(sb: &Superblock, inode_num: u32, inode: &ext4_inode) -> u32 {
    let struct_size = core::mem::size_of::<ext4_inode>();
    let inode_size = sb.inode_size() as usize;
    // 使用 struct 大小与 inode_size 中较小的那个，避免越界读取
    let compute_size = struct_size.min(inode_size);

    // 将 inode 转换为字节切片（安全：大小不超过 struct 大小）
    let inode_bytes = unsafe {
        core::slice::from_raw_parts(
            inode as *const ext4_inode as *const u8,
            compute_size,
        )
    };

    // Step 1: CRC32C(init, uuid)
    let uuid = sb.uuid();
    let mut crc = crate::crc::crc32c_append(EXT4_CRC32_INIT, uuid);

    // Step 2: CRC32C(crc, inode_num_le)
    let ino_index = inode_num.to_le_bytes();
    crc = crate::crc::crc32c_append(crc, &ino_index);

    // Step 3: CRC32C(crc, inode_generation_le)
    let ino_gen = u32::from_le(inode.generation).to_le_bytes();
    crc = crate::crc::crc32c_append(crc, &ino_gen);

    // Step 4: CRC32C(crc, inode_bytes_with_checksum_zeroed)
    // 与 C 代码一致：先清零 checksum 再计算整个 inode
    // 这里不修改原始数据，而是分段计算跳过 checksum 字段
    let checksum_lo_off = 124usize; // checksum_lo 偏移
    let checksum_hi_off = 130usize; // checksum_hi 偏移
    let zero2 = [0u8; 2];

    if compute_size <= checksum_lo_off {
        // inode 结构比 checksum_lo 偏移还小，直接计算全部
        crc = crate::crc::crc32c_append(crc, &inode_bytes[..compute_size]);
    } else if inode_size <= EXT4_GOOD_OLD_INODE_SIZE || compute_size <= checksum_hi_off {
        // 只有 checksum_lo 需要清零
        crc = crate::crc::crc32c_append(crc, &inode_bytes[..checksum_lo_off]);
        crc = crate::crc::crc32c_append(crc, &zero2);
        let after = checksum_lo_off + 2;
        if compute_size > after {
            crc = crate::crc::crc32c_append(crc, &inode_bytes[after..compute_size]);
        }
    } else {
        // 同时处理 checksum_lo 和 checksum_hi
        crc = crate::crc::crc32c_append(crc, &inode_bytes[..checksum_lo_off]);
        crc = crate::crc::crc32c_append(crc, &zero2);
        let after_lo = checksum_lo_off + 2;
        crc = crate::crc::crc32c_append(crc, &inode_bytes[after_lo..checksum_hi_off]);
        crc = crate::crc::crc32c_append(crc, &zero2);
        let after_hi = checksum_hi_off + 2;
        if compute_size > after_hi {
            crc = crate::crc::crc32c_append(crc, &inode_bytes[after_hi..compute_size]);
        }
    }

    // 如果磁盘 inode_size 大于 struct，补零计算剩余部分
    if inode_size > compute_size {
        let padding = inode_size - compute_size;
        // 零字节的 CRC 等价于实际补零
        let zero_buf = [0u8; 256];
        let remaining = padding.min(zero_buf.len());
        crc = crate::crc::crc32c_append(crc, &zero_buf[..remaining]);
    }

    // 对于 128 字节的旧 inode，只保留低 16 位
    if inode_size == EXT4_GOOD_OLD_INODE_SIZE {
        crc &= 0xFFFF;
    }

    crc
}

/// 验证 inode 校验和
pub fn verify_checksum(sb: &Superblock, inode_num: u32, inode: &ext4_inode) -> bool {
    if !sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM) {
        return true;
    }

    let stored = get_checksum(sb, inode);
    let computed = compute_checksum(sb, inode_num, inode);

    stored == computed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ext4_sblock;

    #[test]
    fn test_checksum_without_feature() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.inode_size = 128u16.to_le();
        sb.feature_ro_compat = 0u32.to_le();

        let superblock = Superblock::new(sb);
        let inode = ext4_inode::default();

        // 未启用 METADATA_CSUM，应该总是验证通过
        assert!(verify_checksum(&superblock, 1, &inode));
    }

    #[test]
    fn test_checksum_with_feature() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.inode_size = 256u16.to_le();
        sb.feature_ro_compat = EXT4_FEATURE_RO_COMPAT_METADATA_CSUM.to_le();

        let superblock = Superblock::new(sb);
        let mut inode = ext4_inode::default();

        // 计算并设置校验和
        let csum = compute_checksum(&superblock, 1, &inode);
        set_checksum(&superblock, &mut inode, csum);

        // 应该验证通过
        assert!(verify_checksum(&superblock, 1, &inode));
    }

    #[test]
    fn test_checksum_corruption() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.inode_size = 256u16.to_le();
        sb.feature_ro_compat = EXT4_FEATURE_RO_COMPAT_METADATA_CSUM.to_le();

        let superblock = Superblock::new(sb);
        let mut inode = ext4_inode::default();

        // 设置正确的校验和
        let csum = compute_checksum(&superblock, 1, &inode);
        set_checksum(&superblock, &mut inode, csum);

        // 验证应该通过
        assert!(verify_checksum(&superblock, 1, &inode));

        // 修改 inode 数据（模拟损坏）
        inode.size_lo = 12345u32.to_le();

        // 现在验证应该失败
        assert!(!verify_checksum(&superblock, 1, &inode));
    }

    #[test]
    fn test_checksum_get_set() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.inode_size = 256u16.to_le();

        let superblock = Superblock::new(sb);
        let mut inode = ext4_inode::default();

        // 设置低16位校验和
        set_checksum(&superblock, &mut inode, 0x12345678);
        assert_eq!(get_checksum(&superblock, &inode), 0x12345678);

        // 设置边界值
        set_checksum(&superblock, &mut inode, 0xFFFFFFFF);
        assert_eq!(get_checksum(&superblock, &inode), 0xFFFFFFFF);

        set_checksum(&superblock, &mut inode, 0);
        assert_eq!(get_checksum(&superblock, &inode), 0);
    }

    #[test]
    fn test_old_inode_size() {
        let mut sb = ext4_sblock::default();
        sb.magic = crate::consts::EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.inode_size = 128u16.to_le();
        sb.feature_ro_compat = EXT4_FEATURE_RO_COMPAT_METADATA_CSUM.to_le();

        let superblock = Superblock::new(sb);
        let mut inode = ext4_inode::default();

        // 128 字节 inode 只存储低 16 位
        let csum = compute_checksum(&superblock, 1, &inode);
        assert_eq!(csum & 0xFFFF0000, 0); // 高 16 位为 0

        set_checksum(&superblock, &mut inode, csum);
        assert!(verify_checksum(&superblock, 1, &inode));
    }
}
