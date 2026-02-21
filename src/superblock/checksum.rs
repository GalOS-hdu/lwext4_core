//! Superblock 校验和计算
//!
//! 对应 lwext4 的 `ext4_sb_csum()` 和 `ext4_sb_verify_csum()` 功能

use crate::{
    consts::{EXT4_FEATURE_RO_COMPAT_METADATA_CSUM, EXT4_CHECKSUM_CRC32C},
    types::ext4_sblock,
    crc::EXT4_CRC32_INIT,
};

/// 计算 superblock 的 CRC32C 校验和
///
/// 对应 lwext4 的 `ext4_sb_csum()`
///
/// # 参数
///
/// * `sb` - superblock 结构引用
///
/// # 返回
///
/// 32 位 CRC32C 校验和
pub fn compute_checksum(sb: &ext4_sblock) -> u32 {
    // 转换 superblock 为字节切片
    let sb_bytes = crate::bytes::as_bytes(sb);

    // 计算校验和的范围：从开始到 checksum 字段之前
    // checksum 字段在结构体末尾附近，我们需要计算到它之前的所有字段
    let checksum_offset = offset_of_checksum();
    let data_to_hash = &sb_bytes[..checksum_offset];

    // 使用 CRC32C 算法计算
    crate::crc::crc32c_append(EXT4_CRC32_INIT, data_to_hash)
}

/// 验证 superblock 校验和
///
/// 对应 lwext4 的 `ext4_sb_verify_csum()`
///
/// # 参数
///
/// * `sb` - superblock 结构引用
///
/// # 返回
///
/// 如果校验和正确或不需要校验返回 `true`，否则返回 `false`
pub fn verify_checksum(sb: &ext4_sblock) -> bool {
    // 检查是否启用了 METADATA_CSUM 特性
    let feature_ro_compat = u32::from_le(sb.feature_ro_compat);
    if (feature_ro_compat & EXT4_FEATURE_RO_COMPAT_METADATA_CSUM) == 0 {
        // 未启用校验和特性，直接返回 true
        return true;
    }

    // 检查校验和类型是否为 CRC32C
    if sb.checksum_type != EXT4_CHECKSUM_CRC32C {
        // 不支持的校验和类型
        return false;
    }

    // 计算实际校验和
    let computed = compute_checksum(sb);
    let stored = u32::from_le(sb.checksum);

    // 比较
    computed == stored
}

/// 设置 superblock 校验和
///
/// 对应 lwext4 的 `ext4_sb_set_csum()`
///
/// # 参数
///
/// * `sb` - 可变 superblock 结构引用
///
/// # 副作用
///
/// 更新 `sb.checksum` 字段
pub fn set_checksum(sb: &mut ext4_sblock) {
    // 检查是否启用了 METADATA_CSUM 特性
    let feature_ro_compat = u32::from_le(sb.feature_ro_compat);
    if (feature_ro_compat & EXT4_FEATURE_RO_COMPAT_METADATA_CSUM) == 0 {
        // 未启用校验和特性，不需要设置
        return;
    }

    // 计算校验和并设置
    let csum = compute_checksum(sb);
    sb.checksum = csum.to_le();
}

/// 获取 checksum 字段在 ext4_sblock 中的偏移量
///
/// 使用 `offset_of!` 宏（如果可用）或者手动计算
fn offset_of_checksum() -> usize {
    // checksum 字段的偏移量，需要根据 ext4_sblock 的实际定义来确定
    // 在标准 ext4 superblock 中，checksum 字段位于偏移 1020 (0x3FC)
    // 这是 superblock 的最后 4 个字节之一

    // 更安全的方法：使用 std::mem::offset_of 或手动计算
    // 这里我们暂时使用硬编码值，与 lwext4 保持一致
    1020
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::EXT4_SUPERBLOCK_MAGIC;

    #[test]
    fn test_checksum_without_feature() {
        let mut sb = ext4_sblock::default();
        sb.magic = EXT4_SUPERBLOCK_MAGIC.to_le();

        // 未启用 METADATA_CSUM 特性
        sb.feature_ro_compat = 0u32.to_le();

        // 应该总是验证通过
        assert!(verify_checksum(&sb));

        // 设置校验和应该不做任何事
        set_checksum(&mut sb);
        assert_eq!(sb.checksum, 0);
    }

    #[test]
    fn test_checksum_with_feature() {
        let mut sb = ext4_sblock::default();
        sb.magic = EXT4_SUPERBLOCK_MAGIC.to_le();

        // 启用 METADATA_CSUM 特性
        sb.feature_ro_compat = EXT4_FEATURE_RO_COMPAT_METADATA_CSUM.to_le();
        sb.checksum_type = EXT4_CHECKSUM_CRC32C;

        // 设置校验和
        set_checksum(&mut sb);

        // 应该验证通过
        assert!(verify_checksum(&sb));
    }

    #[test]
    fn test_checksum_corruption() {
        let mut sb = ext4_sblock::default();
        sb.magic = EXT4_SUPERBLOCK_MAGIC.to_le();

        // 启用 METADATA_CSUM 特性
        sb.feature_ro_compat = EXT4_FEATURE_RO_COMPAT_METADATA_CSUM.to_le();
        sb.checksum_type = EXT4_CHECKSUM_CRC32C;

        // 设置正确的校验和
        set_checksum(&mut sb);

        // 验证应该通过
        assert!(verify_checksum(&sb));

        // 修改 superblock 数据（模拟损坏）
        sb.blocks_count_lo = 12345u32.to_le();

        // 现在验证应该失败
        assert!(!verify_checksum(&sb));
    }

    #[test]
    fn test_compute_checksum_deterministic() {
        let mut sb = ext4_sblock::default();
        sb.magic = EXT4_SUPERBLOCK_MAGIC.to_le();
        sb.blocks_count_lo = 1000u32.to_le();

        // 多次计算应该得到相同结果
        let csum1 = compute_checksum(&sb);
        let csum2 = compute_checksum(&sb);
        assert_eq!(csum1, csum2);
    }
}
