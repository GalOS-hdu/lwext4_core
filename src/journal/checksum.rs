//! Journal 校验和计算
//!
//! 对应 lwext4 的 journal checksum 功能

use super::types::*;

/// 计算 journal block 的 CRC32C 校验和
///
/// 对应 lwext4 的 `jbd_block_csum()`
///
/// # 参数
///
/// * `uuid` - Journal UUID
/// * `data` - 块数据
/// * `sequence` - 序列号
///
/// # 返回
///
/// CRC32C 校验和
pub fn block_csum(uuid: &[u8; 16], data: &[u8], sequence: u32) -> u32 {
    let mut crc = crate::crc::crc32c(uuid);
    crc = crate::crc::crc32c_append(crc, &sequence.to_be_bytes());
    crc = crate::crc::crc32c_append(crc, data);
    crc
}

/// 验证 journal descriptor block 的校验和
///
/// # 参数
///
/// * `uuid` - Journal UUID
/// * `data` - 块数据
///
/// # 返回
///
/// 校验和是否有效
pub fn verify_descriptor_block(uuid: &[u8; 16], data: &[u8]) -> bool {
    let tail_size = core::mem::size_of::<jbd_block_tail>();
    if data.len() < core::mem::size_of::<jbd_bhdr>() + tail_size {
        return false;
    }

    // 读取块头获取序列号
    let header = unsafe {
        core::ptr::read_unaligned(data.as_ptr() as *const jbd_bhdr)
    };
    let sequence = u32::from_be(header.sequence);

    // 读取存储在块尾部的校验和
    let tail_offset = data.len() - tail_size;
    let tail = unsafe {
        core::ptr::read_unaligned(
            data.as_ptr().add(tail_offset) as *const jbd_block_tail
        )
    };
    let stored_csum = u32::from_be(tail.checksum);

    // 计算校验和（不包含尾部）
    let data_to_check = &data[..tail_offset];
    let calculated_csum = block_csum(uuid, data_to_check, sequence);

    stored_csum == calculated_csum
}

/// 验证 journal commit block 的校验和
///
/// # 参数
///
/// * `uuid` - Journal UUID
/// * `data` - 块数据
///
/// # 返回
///
/// 校验和是否有效
pub fn verify_commit_block(uuid: &[u8; 16], data: &[u8]) -> bool {
    if data.len() < core::mem::size_of::<jbd_commit_header>() {
        return false;
    }

    let commit_header = unsafe {
        core::ptr::read_unaligned(data.as_ptr() as *const jbd_commit_header)
    };

    // 获取存储的校验和
    let stored_csum = u32::from_be(commit_header.chksum[0]);

    // 计算校验和
    let sequence = u32::from_be(commit_header.header.sequence);
    let calculated_csum = block_csum(uuid, data, sequence);

    stored_csum == calculated_csum
}

/// 计算 journal descriptor block 的校验和
///
/// # 参数
///
/// * `uuid` - Journal UUID
/// * `data` - 块数据
///
/// # 返回
///
/// 计算得到的校验和
pub fn calculate_descriptor_csum(uuid: &[u8; 16], data: &[u8]) -> u32 {
    if data.len() < core::mem::size_of::<jbd_bhdr>() {
        return 0;
    }

    let header = unsafe {
        core::ptr::read_unaligned(data.as_ptr() as *const jbd_bhdr)
    };
    let sequence = u32::from_be(header.sequence);

    block_csum(uuid, data, sequence)
}

/// 计算 journal commit block 的校验和
///
/// # 参数
///
/// * `uuid` - Journal UUID
/// * `data` - 块数据
///
/// # 返回
///
/// 计算得到的校验和
pub fn calculate_commit_csum(uuid: &[u8; 16], data: &[u8]) -> u32 {
    if data.len() < core::mem::size_of::<jbd_commit_header>() {
        return 0;
    }

    let header = unsafe {
        core::ptr::read_unaligned(data.as_ptr() as *const jbd_commit_header)
    };
    let sequence = u32::from_be(header.header.sequence);

    block_csum(uuid, data, sequence)
}

/// 计算 revoke block 的校验和
///
/// # 参数
///
/// * `uuid` - Journal UUID
/// * `data` - 块数据
///
/// # 返回
///
/// 计算得到的校验和
pub fn calculate_revoke_csum(uuid: &[u8; 16], data: &[u8]) -> u32 {
    if data.len() < core::mem::size_of::<jbd_revoke_header>() {
        return 0;
    }

    let header = unsafe {
        core::ptr::read_unaligned(data.as_ptr() as *const jbd_revoke_header)
    };
    let sequence = u32::from_be(header.header.sequence);

    block_csum(uuid, data, sequence)
}

/// 验证 journal revoke block 的校验和
///
/// # 参数
///
/// * `uuid` - Journal UUID
/// * `data` - 块数据
///
/// # 返回
///
/// 校验和是否有效
pub fn verify_revoke_block(uuid: &[u8; 16], data: &[u8]) -> bool {
    let tail_size = core::mem::size_of::<jbd_revoke_tail>();
    if data.len() < core::mem::size_of::<jbd_revoke_header>() + tail_size {
        return false;
    }

    // 读取块头获取序列号
    let header = unsafe {
        core::ptr::read_unaligned(data.as_ptr() as *const jbd_revoke_header)
    };
    let sequence = u32::from_be(header.header.sequence);

    // 读取存储在块尾部的校验和
    let tail_offset = data.len() - tail_size;
    let tail = unsafe {
        core::ptr::read_unaligned(
            data.as_ptr().add(tail_offset) as *const jbd_revoke_tail
        )
    };
    let stored_csum = u32::from_be(tail.checksum);

    // 计算校验和（不包含尾部）
    let data_to_check = &data[..tail_offset];
    let calculated_csum = block_csum(uuid, data_to_check, sequence);

    stored_csum == calculated_csum
}

/// 验证 journal superblock 的校验和
///
/// # 参数
///
/// * `sb` - Journal superblock
///
/// # 返回
///
/// 校验和是否有效
pub fn verify_superblock_csum(sb: &jbd_sb) -> bool {
    // 检查是否启用了校验和特性
    let incompat = u32::from_be(sb.feature_incompat);
    if (incompat & (JBD_FEATURE_INCOMPAT_CSUM_V2 | JBD_FEATURE_INCOMPAT_CSUM_V3)) == 0 {
        // 未启用校验和，直接返回 true
        return true;
    }

    // 获取存储的校验和
    let stored_csum = u32::from_be(sb.checksum);

    // 计算校验和（需要将 checksum 字段置零）
    let mut sb_copy = *sb;
    sb_copy.checksum = 0;

    let data = unsafe {
        core::slice::from_raw_parts(
            &sb_copy as *const jbd_sb as *const u8,
            core::mem::size_of::<jbd_sb>(),
        )
    };

    let calculated_csum = crate::crc::crc32c(data);

    stored_csum == calculated_csum
}

/// 计算 journal superblock 的校验和
///
/// # 参数
///
/// * `sb` - Journal superblock（将被修改，写入计算得到的校验和）
pub fn calculate_superblock_csum(sb: &mut jbd_sb) {
    // 先将 checksum 字段置零
    sb.checksum = 0;

    // 计算校验和
    let data = unsafe {
        core::slice::from_raw_parts(
            sb as *const jbd_sb as *const u8,
            core::mem::size_of::<jbd_sb>(),
        )
    };

    let csum = crate::crc::crc32c(data);
    sb.checksum = csum.to_be();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_csum() {
        let uuid = [0u8; 16];
        let data = [1u8, 2, 3, 4, 5];
        let sequence = 100;

        let csum = block_csum(&uuid, &data, sequence);
        // 校验和应该是确定性的
        assert_ne!(csum, 0);

        // 相同输入应该产生相同输出
        let csum2 = block_csum(&uuid, &data, sequence);
        assert_eq!(csum, csum2);
    }

    #[test]
    fn test_superblock_csum() {
        let mut sb = jbd_sb::default();
        sb.header.magic = JBD_MAGIC_NUMBER.to_be();
        sb.blocksize = 4096u32.to_be();
        sb.feature_incompat = JBD_FEATURE_INCOMPAT_CSUM_V3.to_be();

        // 计算校验和
        calculate_superblock_csum(&mut sb);
        let checksum_value = sb.checksum;
        assert_ne!(checksum_value, 0);

        // 验证校验和
        assert!(verify_superblock_csum(&sb));

        // 修改数据后校验和应该无效
        let mut sb_modified = sb;
        sb_modified.blocksize = 8192u32.to_be();
        assert!(!verify_superblock_csum(&sb_modified));
    }

    #[test]
    fn test_csum_disabled() {
        // 未启用校验和特性时，验证应该总是返回 true
        let mut sb = jbd_sb::default();
        sb.header.magic = JBD_MAGIC_NUMBER.to_be();
        sb.feature_incompat = 0; // 未启用任何校验和特性

        assert!(verify_superblock_csum(&sb));
    }
}
