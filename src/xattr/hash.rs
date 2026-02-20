//! xattr 哈希和校验和计算
//!
//! 提供 entry 哈希、block 哈希和 CRC32C 校验和计算
//! issue: 该hash还没有真正派上用场， 之后应当完善， 用于实现数据去重（共享存储，新分配的块hash相同时，这些inode可以共享一个xattr block,当需要做修改时再拷贝）、提升查询效率 以及 保障元数据完整性。

use crate::{
    consts::*,
    superblock::Superblock,
    types::{ext4_xattr_entry, ext4_xattr_header},
};

/// 计算单个 xattr entry 的哈希值
///
/// 对应 lwext4 的 `ext4_xattr_compute_hash()`
///
/// 哈希算法：
/// 1. 对名称的每个字节执行滚动哈希
/// 2. 对值的每个 4 字节执行滚动哈希（如果值存在）
///
/// # 参数
///
/// * `entry` - xattr 条目
/// * `name` - 属性名称
/// * `value_data` - 可选的值数据（如果 entry 有值）
///
/// # 返回
///
/// 32 位哈希值
pub fn compute_entry_hash(
    entry: &ext4_xattr_entry,
    name: &[u8],
    value_data: Option<&[u8]>,
) -> u32 {
    let mut hash: u32 = 0;

    // 对名称计算哈希
    for &byte in name.iter().take(entry.e_name_len as usize) {
        hash = hash
            .wrapping_shl(NAME_HASH_SHIFT)
            .wrapping_add(hash.wrapping_shr(32 - NAME_HASH_SHIFT))
            .wrapping_add(byte as u32);
    }

    // 如果有值且值不在外部块（e_value_block == 0），对值计算哈希
    if entry.e_value_block == 0 && entry.value_size() != 0 {
        if let Some(value) = value_data {
            // 值按 4 字节对齐后处理
            let value_size_aligned = (entry.value_size() as usize + EXT4_XATTR_ROUND as usize)
                >> EXT4_XATTR_PAD_BITS;

            // 每次处理 4 字节
            for chunk_idx in 0..value_size_aligned {
                let offset = chunk_idx * 4;
                if offset + 4 <= value.len() {
                    // 读取 4 字节作为小端 u32
                    let val = u32::from_le_bytes([
                        value[offset],
                        value[offset + 1],
                        value[offset + 2],
                        value[offset + 3],
                    ]);

                    hash = hash
                        .wrapping_shl(VALUE_HASH_SHIFT)
                        .wrapping_add(hash.wrapping_shr(32 - VALUE_HASH_SHIFT))
                        .wrapping_add(val);
                }
            }
        }
    }

    hash
}

/// 重新计算整个 xattr block 的哈希值
///
/// 对应 lwext4 的 `ext4_xattr_rehash()`
///
/// 算法：
/// 1. 首先调用 `compute_entry_hash()` 更新给定 entry 的哈希
/// 2. 遍历所有 entry，对它们的 e_hash 字段执行滚动哈希
/// 3. 将结果写入 header 的 h_hash 字段
///
/// # 参数
///
/// * `header` - xattr block 头部（会修改 h_hash）
/// * `entry` - 刚修改的 entry（会修改 e_hash）
/// * `name` - entry 的名称
/// * `value_data` - entry 的值数据
/// * `all_entries` - block 中所有 entry 的数据（用于遍历）
///
/// # 返回
///
/// 计算出的 block 哈希值
pub fn rehash_block(
    header: &mut ext4_xattr_header,
    entry: &mut ext4_xattr_entry,
    name: &[u8],
    value_data: Option<&[u8]>,
    all_entries: &[u8], // 整个 entries 区域的数据
) -> u32 {
    // 1. 计算当前 entry 的哈希
    let entry_hash = compute_entry_hash(entry, name, value_data);
    entry.e_hash = entry_hash.to_le();

    // 2. 遍历所有 entry 计算 block 哈希
    let mut block_hash: u32 = 0;
    let mut offset = 0;

    loop {
        // 检查是否到达末尾（4 字节全 0）
        if offset + 4 > all_entries.len() {
            break;
        }

        let first_u32 = u32::from_le_bytes([
            all_entries[offset],
            all_entries[offset + 1],
            all_entries[offset + 2],
            all_entries[offset + 3],
        ]);

        if first_u32 == 0 {
            // 到达结束标记
            break;
        }

        // 读取 entry
        if offset + core::mem::size_of::<ext4_xattr_entry>() > all_entries.len() {
            break;
        }

        let e_name_len = all_entries[offset];
        let e_hash_offset = offset + 12; // e_hash 在结构体中的偏移

        if e_hash_offset + 4 <= all_entries.len() {
            let e_hash = u32::from_le_bytes([
                all_entries[e_hash_offset],
                all_entries[e_hash_offset + 1],
                all_entries[e_hash_offset + 2],
                all_entries[e_hash_offset + 3],
            ]);

            if e_hash == 0 {
                // 如果任何 entry 的哈希为 0，整个 block 不共享
                block_hash = 0;
                break;
            }

            // 滚动哈希
            block_hash = block_hash
                .wrapping_shl(BLOCK_HASH_SHIFT)
                .wrapping_add(block_hash.wrapping_shr(32 - BLOCK_HASH_SHIFT))
                .wrapping_add(e_hash);
        }

        // 移动到下一个 entry
        // entry 大小 = sizeof(ext4_xattr_entry) + 对齐后的名称长度
        let entry_len = core::mem::size_of::<ext4_xattr_entry>()
            + (((e_name_len as usize) + EXT4_XATTR_ROUND as usize) & !(EXT4_XATTR_ROUND as usize));

        offset += entry_len;
    }

    // 3. 写入 header
    header.h_hash = block_hash.to_le();

    block_hash
}

/// 计算 xattr block 的 CRC32C 校验和
///
/// 对应 lwext4 的 `ext4_xattr_block_checksum()`
///
/// 校验和计算方法：
/// 1. crc32c(uuid)
/// 2. crc32c(block_number)
/// 3. crc32c(block_data)
///
/// # 参数
///
/// * `sb` - superblock（包含 uuid）
/// * `block_num` - 块号
/// * `block_data` - 整个 block 的数据
///
/// # 返回
///
/// CRC32C 校验和
pub fn compute_block_checksum(sb: &Superblock, block_num: u64, block_data: &[u8]) -> u32 {
    // 检查是否启用元数据校验和特性
    if !sb.has_metadata_csum() {
        return 0;
    }

    let block_num_le = block_num.to_le_bytes();

    // 1. 首先对 UUID 计算 CRC32C
    let mut crc = crate::crc::crc32c_append(EXT4_CRC32_INIT, sb.uuid());

    // 2. 对块号计算 CRC32C
    crc = crate::crc::crc32c_append(crc, &block_num_le);

    // 3. 对整个块数据计算 CRC32C（注意：计算时 h_checksum 字段应为 0）
    // 假设调用者已经将 h_checksum 置为 0
    crc = crate::crc::crc32c_append(crc, block_data);

    crc
}

/// 设置 xattr block 的校验和
///
/// 对应 lwext4 的 `ext4_xattr_set_block_checksum()`
///
/// # 参数
///
/// * `sb` - superblock
/// * `block_num` - 块号
/// * `header` - xattr header（会修改 h_checksum）
/// * `block_data` - 整个 block 的数据
pub fn set_block_checksum(
    sb: &Superblock,
    block_num: u64,
    header: &mut ext4_xattr_header,
    block_data: &[u8],
) {
    if !sb.has_metadata_csum() {
        return;
    }

    // 临时保存原校验和
    let orig_checksum = header.h_checksum;

    // 计算时需要将校验和字段置为 0
    header.h_checksum = 0;

    let checksum = compute_block_checksum(sb, block_num, block_data);

    // 恢复并设置新校验和
    header.h_checksum = checksum.to_le();

    // 如果需要，这里可以恢复 orig_checksum（但通常我们就是要更新它）
    let _ = orig_checksum; // 避免未使用警告
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_entry_hash_name_only() {
        let mut entry = ext4_xattr_entry::default();
        entry.e_name_len = 7;
        entry.e_value_block = 0;
        entry.e_value_size = 0;

        let name = b"comment";
        let hash = compute_entry_hash(&entry, name, None);

        // 哈希应该非零
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_compute_entry_hash_with_value() {
        let mut entry = ext4_xattr_entry::default();
        entry.e_name_len = 4;
        entry.e_value_block = 0;
        entry.e_value_size = 5_u32.to_le(); // "hello"

        let name = b"test";
        let value = b"hello";

        let hash = compute_entry_hash(&entry, name, Some(value));

        // 哈希应该非零
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_compute_entry_hash_empty_name() {
        let entry = ext4_xattr_entry::default();
        let hash = compute_entry_hash(&entry, b"", None);

        // 空名称的哈希应该是 0
        assert_eq!(hash, 0);
    }
}
