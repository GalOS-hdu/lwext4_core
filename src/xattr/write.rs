//! xattr 写操作核心逻辑
//!
//! 提供在内存缓冲区中修改 xattr 的核心算法

use crate::{
    consts::*,
    error::{Error, ErrorKind, Result},
    types::ext4_xattr_entry,
};
use core::mem::size_of;

use super::search::XattrSearch;

/// 计算 entry 的总大小（包括名称和对齐）
///
/// 对应 C 宏 `EXT4_XATTR_LEN(name_len)`
#[inline]
fn entry_len(name_len: usize) -> usize {
    (name_len + EXT4_XATTR_ROUND as usize + size_of::<ext4_xattr_entry>())
        & !(EXT4_XATTR_ROUND as usize)
}

/// 计算 value 的对齐后大小
///
/// 对应 C 宏 `EXT4_XATTR_SIZE(value_len)`
#[inline]
fn value_size(value_len: usize) -> usize {
    (value_len + EXT4_XATTR_ROUND as usize) & !(EXT4_XATTR_ROUND as usize)
}

/// 在 xattr 数据区中设置 entry（核心内存操作）
///
/// 对应 lwext4 的 `ext4_xattr_set_entry()`
///
/// # 参数
///
/// * `data` - xattr 数据区（可变）
/// * `first_offset` - 第一个 entry 的偏移
/// * `end_offset` - 数据区结束偏移
/// * `name_index` - 命名空间索引
/// * `name` - 属性名称（不含前缀）
/// * `value` - 属性值（None 表示删除）
/// * `dry_run` - 是否只检查空间，不实际修改
///
/// # 返回
///
/// 成功返回 Ok(())，空间不足返回 ENOSPC 错误
///
/// # 算法步骤
///
/// 1. 查找 entry（可能存在或不存在）
/// 2. 计算可用空间
/// 3. 如果空间不足，返回 ENOSPC
/// 4. 删除旧 value（如果存在）
/// 5. 插入/更新 entry
/// 6. 写入新 value
pub fn set_entry_in_memory(
    data: &mut [u8],
    first_offset: usize,
    end_offset: usize,
    name_index: u8,
    name: &[u8],
    value: Option<&[u8]>,
    dry_run: bool,
) -> Result<()> {
    let name_len = name.len();

    // 1. 创建搜索上下文并查找 entry
    let search_data = &data[..end_offset];
    let mut search = XattrSearch::new(search_data, first_offset);
    let found = search.find_entry(name_index, name);

    let not_found = found.is_none();

    // 情况 1：删除不存在的属性（幂等操作）
    if value.is_none() && not_found {
        return Ok(());
    }

    // 2. 计算 min_offs（value 区的最小偏移）
    let mut min_offs = end_offset;
    let mut last_entry_offset = first_offset;

    let mut offset = first_offset;
    loop {
        if offset + 4 > end_offset {
            break;
        }

        // 检查是否是最后一个 entry
        let first_u32 = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        if first_u32 == 0 {
            last_entry_offset = offset;
            break;
        }

        if offset + size_of::<ext4_xattr_entry>() > end_offset {
            break;
        }

        // 读取 entry
        let entry_bytes = &data[offset..offset + size_of::<ext4_xattr_entry>()];
        let entry = unsafe {
            core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
        };

        let entry_value_size = entry.value_size();
        if entry_value_size > 0 {
            let value_offs = u16::from_le(entry.e_value_offs) as usize;
            if value_offs < min_offs {
                min_offs = value_offs;
            }
        }

        last_entry_offset = offset;
        let entry_name_len = entry.e_name_len as usize;
        offset += entry_len(entry_name_len);
    }

    // 3. 计算可用空间
    let mut free = min_offs
        .saturating_sub(last_entry_offset)
        .saturating_sub(size_of::<u32>()); // 减去结束标记

    // 如果是修改已有 entry，回收旧空间
    if !not_found {
        if let Some((entry_offset, _, old_value_size)) = found {
            let entry_bytes = &data[entry_offset..entry_offset + size_of::<ext4_xattr_entry>()];
            let entry = unsafe {
                core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
            };
            let old_name_len = entry.e_name_len as usize;

            free += value_size(old_value_size as usize);
            free += entry_len(old_name_len);
        }
    }

    // 4. 检查空间是否足够
    if let Some(new_value) = value {
        let required = value_size(new_value.len()) + entry_len(name_len);
        if free < required {
            return Err(Error::new(ErrorKind::NoSpace, "no space for xattr entry"));
        }
    }

    // 5. 如果是 dry_run，到此为止
    if dry_run {
        return Ok(());
    }

    // 6. 删除旧 value（如果 entry 已存在）
    if let Some((_entry_offset, old_value_offset, old_value_size)) = found {
        if old_value_offset > 0 && old_value_size > 0 {
            let old_value_size_aligned = value_size(old_value_size as usize);
            let first_value = min_offs;

            // 将 [first_value, old_value_offset) 的数据后移
            if old_value_offset > first_value {
                let src_start = first_value;
                let src_end = old_value_offset;
                let dst_start = first_value + old_value_size_aligned;

                // 使用临时缓冲区避免重叠问题
                let temp: alloc::vec::Vec<u8> = data[src_start..src_end].to_vec();
                data[dst_start..dst_start + temp.len()].copy_from_slice(&temp);
            }

            // 清零旧 value 区域
            for byte in &mut data[first_value..first_value + old_value_size_aligned] {
                *byte = 0;
            }

            // 更新 min_offs
            min_offs += old_value_size_aligned;

            // 更新其他 entry 的 e_value_offs
            let mut scan_offset = first_offset;
            loop {
                if scan_offset + 4 > end_offset {
                    break;
                }

                let first_u32 = u32::from_le_bytes([
                    data[scan_offset],
                    data[scan_offset + 1],
                    data[scan_offset + 2],
                    data[scan_offset + 3],
                ]);

                if first_u32 == 0 {
                    break;
                }

                if scan_offset + size_of::<ext4_xattr_entry>() > end_offset {
                    break;
                }

                // 读取 entry
                let entry_bytes = &data[scan_offset..scan_offset + size_of::<ext4_xattr_entry>()];
                let mut entry = unsafe {
                    core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
                };

                let entry_value_offs = u16::from_le(entry.e_value_offs) as usize;
                if entry_value_offs > 0 && entry_value_offs < old_value_offset {
                    // 调整偏移
                    let new_offs = entry_value_offs + old_value_size_aligned;
                    entry.e_value_offs = (new_offs as u16).to_le();

                    // 写回 entry
                    let entry_bytes = unsafe {
                        core::slice::from_raw_parts(
                            &entry as *const ext4_xattr_entry as *const u8,
                            size_of::<ext4_xattr_entry>(),
                        )
                    };
                    data[scan_offset..scan_offset + size_of::<ext4_xattr_entry>()]
                        .copy_from_slice(entry_bytes);
                }

                let scan_name_len = entry.e_name_len as usize;
                scan_offset += entry_len(scan_name_len);
            }
        }
    }

    // 7. 插入/更新 entry
    if let Some(new_value) = value {
        let new_value_len = new_value.len();
        let value_offs = if new_value_len > 0 {
            min_offs - value_size(new_value_len)
        } else {
            0
        };

        if let Some((entry_offset, _, _)) = found {
            // 修改已有 entry
            let entry_bytes = &mut data[entry_offset..entry_offset + size_of::<ext4_xattr_entry>()];
            let mut entry = unsafe {
                core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
            };

            entry.e_value_offs = (value_offs as u16).to_le();
            entry.e_value_size = (new_value_len as u32).to_le();

            // 写回 entry
            let entry_bytes_src = unsafe {
                core::slice::from_raw_parts(
                    &entry as *const ext4_xattr_entry as *const u8,
                    size_of::<ext4_xattr_entry>(),
                )
            };
            data[entry_offset..entry_offset + size_of::<ext4_xattr_entry>()]
                .copy_from_slice(entry_bytes_src);
        } else {
            // 插入新 entry（在 last_entry_offset 位置）
            let new_entry = ext4_xattr_entry {
                e_name_len: name_len as u8,
                e_name_index: name_index,
                e_value_offs: (value_offs as u16).to_le(),
                e_value_block: 0,
                e_value_size: (new_value_len as u32).to_le(),
                e_hash: 0, // 稍后计算
            };

            // 写入 entry
            let entry_bytes = unsafe {
                core::slice::from_raw_parts(
                    &new_entry as *const ext4_xattr_entry as *const u8,
                    size_of::<ext4_xattr_entry>(),
                )
            };
            data[last_entry_offset..last_entry_offset + size_of::<ext4_xattr_entry>()]
                .copy_from_slice(entry_bytes);

            // 写入名称
            let name_offset = last_entry_offset + size_of::<ext4_xattr_entry>();
            data[name_offset..name_offset + name_len].copy_from_slice(name);

            // 写入结束标记
            let next_offset = last_entry_offset + entry_len(name_len);
            if next_offset + 4 <= end_offset {
                data[next_offset..next_offset + 4].copy_from_slice(&[0, 0, 0, 0]);
            }
        }

        // 8. 写入 value
        if value_offs > 0 && new_value_len > 0 {
            data[value_offs..value_offs + new_value_len].copy_from_slice(new_value);

            // value 对齐补零
            let aligned_size = value_size(new_value_len);
            if aligned_size > new_value_len {
                for byte in &mut data[value_offs + new_value_len..value_offs + aligned_size] {
                    *byte = 0;
                }
            }
        }
    } else {
        // 9. 删除 entry
        if let Some((entry_offset, _, _)) = found {
            let entry_bytes = &data[entry_offset..entry_offset + size_of::<ext4_xattr_entry>()];
            let entry = unsafe {
                core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
            };
            let entry_name_len = entry.e_name_len as usize;
            let entry_total_len = entry_len(entry_name_len);

            // 计算需要移动的数据
            let next_offset = entry_offset + entry_total_len;
            let move_len = last_entry_offset + size_of::<u32>() - next_offset;

            if move_len > 0 {
                // 使用临时缓冲区
                let temp: alloc::vec::Vec<u8> = data[next_offset..next_offset + move_len].to_vec();
                data[entry_offset..entry_offset + temp.len()].copy_from_slice(&temp);
            }

            // 清零末尾
            for byte in &mut data[entry_offset + move_len..entry_offset + move_len + entry_total_len] {
                *byte = 0;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_entry_len() {
        // entry 大小 = 16 字节
        // 名称长度 4，对齐后 4
        // 总长度 = (4 + 3 + 16) & ~3 = 23 & ~3 = 20
        assert_eq!(entry_len(4), 20);

        // 名称长度 7，对齐后 8
        // 总长度 = (7 + 3 + 16) & ~3 = 26 & ~3 = 24
        assert_eq!(entry_len(7), 24);
    }

    #[test]
    fn test_value_size() {
        assert_eq!(value_size(5), 8);  // 5 -> 8
        assert_eq!(value_size(8), 8);  // 8 -> 8
        assert_eq!(value_size(9), 12); // 9 -> 12
    }

    #[test]
    fn test_set_entry_new() {
        // 创建空的 xattr 区域
        let mut data = vec![0u8; 256];
        let first_offset = 0;
        let end_offset = 256;

        // 插入新 entry
        let result = set_entry_in_memory(
            &mut data,
            first_offset,
            end_offset,
            1, // user namespace
            b"test",
            Some(b"hello"),
            false,
        );

        assert!(result.is_ok());

        // 验证 entry 被写入
        assert_eq!(data[0], 4); // e_name_len
        assert_eq!(data[1], 1); // e_name_index

        // 验证名称被写入
        assert_eq!(&data[16..20], b"test");
    }

    #[test]
    fn test_set_entry_no_space() {
        // 创建很小的 xattr 区域
        let mut data = vec![0u8; 32];
        let first_offset = 0;
        let end_offset = 32;

        // 尝试插入太大的 entry
        let result = set_entry_in_memory(
            &mut data,
            first_offset,
            end_offset,
            1,
            b"test",
            Some(&[0u8; 100]), // 太大
            false,
        );

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NoSpace);
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut data = vec![0u8; 256];
        let first_offset = 0;
        let end_offset = 256;

        // 删除不存在的 entry（幂等操作）
        let result = set_entry_in_memory(
            &mut data,
            first_offset,
            end_offset,
            1,
            b"nonexistent",
            None, // 删除
            false,
        );

        assert!(result.is_ok());
    }
}
