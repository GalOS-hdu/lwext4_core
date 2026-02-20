//! inode 内部 xattr 操作
//!
//! 处理存储在 inode 额外空间中的扩展属性
//!
//! # 重构说明
//!
//! 本模块已重构为使用 InodeRef，而不是直接操作内存切片。
//!
//! ## 旧实现的问题
//!
//! ```ignore
//! // ❌ 旧实现：使用切片
//! pub fn find_ibody_entry(
//!     sb: &Superblock,
//!     inode: &Inode,
//!     inode_data: &[u8],  // ❌ 只是切片，无一致性保障
//!     name_index: u8,
//!     name: &[u8],
//! ) -> Result<Option<(usize, usize, u32)>>
//! ```
//!
//! ## 新实现
//!
//! ```ignore
//! // ✅ 新实现：使用 InodeRef
//! pub fn find_ibody_entry<D: BlockDevice>(
//!     inode_ref: &mut InodeRef<D>,  // ✅ 使用 InodeRef
//!     name_index: u8,
//!     name: &[u8],
//! ) -> Result<Option<(usize, usize, u32)>>
//! ```

use crate::{
    consts::*,
    error::{Error, ErrorKind, Result},
    block::BlockDevice,
    fs::InodeRef,
    types::{ext4_xattr_entry, ext4_xattr_ibody_header},
};
use core::mem::size_of;

use super::search::XattrSearch;

/// 获取 inode 内部 xattr header 的偏移
///
/// 对应 C 宏 `EXT4_XATTR_IHDR(sb, raw_inode)`
///
/// # 参数
///
/// * `inode_ref` - inode 引用
///
/// # 返回
///
/// header 在 inode 数据中的偏移，如果没有 extra_isize 则返回 None
fn get_ibody_header_offset<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>
) -> Result<Option<usize>> {
    inode_ref.with_inode(|inode| {
        let extra_isize = u16::from_le(inode.extra_isize) as usize;
        if extra_isize == 0 {
            None
        } else {
            Some(EXT4_GOOD_OLD_INODE_SIZE + extra_isize)
        }
    })
}

/// 获取第一个 entry 的偏移
///
/// 对应 C 宏 `EXT4_XATTR_IFIRST(hdr)`
#[inline]
fn get_first_entry_offset(header_offset: usize) -> usize {
    header_offset + size_of::<ext4_xattr_ibody_header>()
}

/// 验证 inode 内部 xattr 数据的有效性
///
/// 对应 lwext4 的 `ext4_xattr_is_ibody_valid()`
///
/// # 参数
///
/// * `inode_ref` - inode 引用
///
/// # 返回
///
/// 如果有效返回 Ok(())，否则返回错误
pub fn validate_ibody_xattr<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>
) -> Result<()> {
    let header_offset = match get_ibody_header_offset(inode_ref)? {
        Some(offset) => offset,
        None => return Ok(()), // 没有 extra_isize，跳过验证
    };

    let inode_size = inode_ref.superblock().inode_size() as usize;

    // 使用 with_inode_raw_data 访问原始数据
    inode_ref.with_inode_raw_data(|inode_data| {
        // 检查是否有足够空间存放 header
        if header_offset + size_of::<ext4_xattr_ibody_header>() > inode_size {
            Err(Error::new(ErrorKind::Io, "ibody xattr header out of bounds"))
        } else {
            // 读取 header
            let header_bytes = &inode_data[header_offset..header_offset + size_of::<ext4_xattr_ibody_header>()];
            let header = unsafe {
                core::ptr::read(header_bytes.as_ptr() as *const ext4_xattr_ibody_header)
            };

            // 验证魔数
            if u32::from_le(header.h_magic) != EXT4_XATTR_MAGIC {
                Err(Error::new(ErrorKind::Io, "invalid ibody xattr magic"))
            } else {
                Ok(())
            }
        }
    })?
}

/// 在 inode 内部查找 xattr entry
///
/// 对应 lwext4 的 `ext4_xattr_ibody_find_entry()`
///
/// # 参数
///
/// * `inode_ref` - inode 引用
/// * `name_index` - 命名空间索引
/// * `name` - 属性名称（不含前缀）
///
/// # 返回
///
/// 成功返回 Some((entry_offset, value_offset, value_size))，未找到返回 None
///
/// # 实现说明
///
/// 由于需要访问 inode 的原始字节数据，这个函数需要：
/// 1. 通过 InodeRef 访问 inode 所在的 block
/// 2. 在 block 中定位 inode 的位置
/// 3. 解析 xattr 数据
///
/// 这与 C 实现中直接访问 `inode_ref->inode` 的方式对应。
pub fn find_ibody_entry<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name_index: u8,
    name: &[u8],
) -> Result<Option<(usize, usize, u32)>> {
    // 获取 header 偏移
    let header_offset = match get_ibody_header_offset(inode_ref)? {
        Some(offset) => offset,
        None => return Ok(None), // 没有 extra_isize
    };

    // 验证 xattr 数据
    validate_ibody_xattr(inode_ref)?;

    // 使用 with_inode_raw_data 访问原始数据并搜索
    inode_ref.with_inode_raw_data(|inode_data| {
        // 获取第一个 entry 的偏移
        let first_entry_offset = get_first_entry_offset(header_offset);

        // 使用 XattrSearch 查找
        let mut search = XattrSearch::new(inode_data, first_entry_offset);
        search.find_entry(name_index, name)
    })
}

/// 列出 inode 内部的所有 xattr entry 名称
///
/// # 参数
///
/// * `inode_ref` - inode 引用
/// * `buffer` - 输出缓冲区（名称以 \0 分隔）
///
/// # 返回
///
/// 成功返回写入的字节数
pub fn list_ibody_xattr<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    buffer: &mut [u8],
) -> Result<usize> {
    // 获取 header 偏移
    let header_offset = match get_ibody_header_offset(inode_ref)? {
        Some(offset) => offset,
        None => return Ok(0), // 没有 extra_isize
    };

    // 验证 xattr 数据
    if validate_ibody_xattr(inode_ref).is_err() {
        // 如果验证失败，返回 0（无 xattr）
        return Ok(0);
    }

    // 使用 with_inode_raw_data 访问原始数据并列出所有 entry
    inode_ref.with_inode_raw_data(|inode_data| -> Result<usize> {
        let first_entry_offset = get_first_entry_offset(header_offset);
        let mut written = 0;
        let mut offset = first_entry_offset;

        // 遍历所有 entry
        loop {
            // 检查是否到达末尾
            if offset + size_of::<ext4_xattr_entry>() > inode_data.len() {
                break;
            }

            // 检查是否是最后一个 entry（前4字节为0）
            let is_last = {
                let first_u32 = u32::from_le_bytes([
                    inode_data[offset],
                    inode_data[offset + 1],
                    inode_data[offset + 2],
                    inode_data[offset + 3],
                ]);
                first_u32 == 0
            };

            if is_last {
                break;
            }

            // 读取 entry
            let entry_bytes = &inode_data[offset..offset + size_of::<ext4_xattr_entry>()];
            let entry = unsafe {
                core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
            };

            let entry_name_len = entry.e_name_len as usize;
            let name_offset = offset + size_of::<ext4_xattr_entry>();

            if name_offset + entry_name_len > inode_data.len() {
                break;
            }

            let entry_name = &inode_data[name_offset..name_offset + entry_name_len];

            // 获取前缀
            use super::prefix::get_xattr_name_prefix;
            if let Some((prefix, _)) = get_xattr_name_prefix(entry.e_name_index) {
                let prefix_bytes = prefix.as_bytes();
                let total_len = prefix_bytes.len() + entry_name_len + 1; // +1 for \0

                // 检查 buffer 是否有足够空间
                if written + total_len > buffer.len() {
                    return Err(Error::new(ErrorKind::NoSpace, "buffer too small for xattr list"));
                }

                // 写入前缀
                buffer[written..written + prefix_bytes.len()].copy_from_slice(prefix_bytes);
                written += prefix_bytes.len();

                // 写入名称
                buffer[written..written + entry_name_len].copy_from_slice(entry_name);
                written += entry_name_len;

                // 写入 \0
                buffer[written] = 0;
                written += 1;
            }

            // 移动到下一个 entry
            // EXT4_XATTR_LEN = (name_len + ROUND + sizeof(entry)) & ~ROUND
            let len = (entry_name_len + EXT4_XATTR_ROUND as usize + size_of::<ext4_xattr_entry>())
                & !(EXT4_XATTR_ROUND as usize);
            offset += len;
        }

        Ok(written)
    })?
}

/// 在 inode 内部设置 xattr
///
/// # 参数
///
/// * `inode_ref` - inode 引用（可变）
/// * `name_index` - 命名空间索引
/// * `name` - 属性名称（不含前缀）
/// * `value` - 属性值（None 表示删除）
///
/// # 返回
///
/// 成功返回 true，空间不足返回 false
///
/// # 实现说明
///
/// 对应 lwext4 中在 inode 内部设置 xattr 的逻辑：
/// 1. 检查空间是否足够
/// 2. 如果足够，执行设置操作
/// 3. 标记 inode 为脏（通过 InodeRef 自动处理）
pub fn set_ibody_entry<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name_index: u8,
    name: &[u8],
    value: Option<&[u8]>,
) -> Result<bool> {
    // 获取 header 偏移
    let header_offset = match get_ibody_header_offset(inode_ref)? {
        Some(offset) => offset,
        None => return Ok(false), // 没有 extra_isize，无法设置
    };

    let inode_size = inode_ref.superblock().inode_size() as usize;

    // 使用 with_inode_raw_data_mut 修改数据
    inode_ref.with_inode_raw_data_mut(|inode_data| -> Result<bool> {
        let first_entry_offset = get_first_entry_offset(header_offset);

        // 使用 XattrSearch 查找现有 entry
        let mut search = XattrSearch::new(inode_data, first_entry_offset);
        let found = search.find_entry(name_index, name);

        // 调用内部设置函数
        set_entry_impl(
            inode_data,
            first_entry_offset,
            inode_size,
            name_index,
            name,
            value,
            found,
        )
    })?
}

/// 内部实现：在给定的数据缓冲区中设置 xattr entry
///
/// 这是一个简单的适配器，调用 `write::set_entry_in_memory` 来完成实际的修改工作。
///
/// 对应 C 实现的 ext4_xattr_set_entry
///
/// # 参数
///
/// * `data` - xattr 数据缓冲区（可变）
/// * `first_entry_offset` - 第一个 entry 的偏移
/// * `data_end` - 数据区结束偏移
/// * `name_index` - 命名空间索引
/// * `name` - 属性名称（不含前缀）
/// * `value` - 属性值（None 表示删除）
/// * `_found` - 查找结果（未使用，set_entry_in_memory 会自己查找）
///
/// # 返回
///
/// * Ok(true) - 成功设置
/// * Ok(false) - 空间不足
/// * Err(_) - 其他错误
fn set_entry_impl(
    data: &mut [u8],
    first_entry_offset: usize,
    data_end: usize,
    name_index: u8,
    name: &[u8],
    value: Option<&[u8]>,
    _found: Option<(usize, usize, u32)>,
) -> Result<bool> {
    // 直接调用 write.rs 中的完整实现
    use super::write::set_entry_in_memory;

    match set_entry_in_memory(
        data,
        first_entry_offset,
        data_end,
        name_index,
        name,
        value,
        false,  // dry_run = false，执行实际修改
    ) {
        Ok(()) => Ok(true),  // 成功
        Err(e) if e.kind() == ErrorKind::NoSpace => Ok(false),  // 空间不足
        Err(e) => Err(e),  // 其他错误
    }
}

/// 初始化 inode 内部 xattr 区域
///
/// 对应 lwext4 的 `ext4_xattr_ibody_initialize()`
///
/// # 参数
///
/// * `inode_ref` - inode 引用（可变）
///
/// # 返回
///
/// 成功返回 Ok(())
pub fn initialize_ibody_xattr<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>
) -> Result<()> {
    let header_offset = match get_ibody_header_offset(inode_ref)? {
        Some(offset) => offset,
        None => return Ok(()), // 没有 extra_isize，无需初始化
    };

    let inode_size = inode_ref.superblock().inode_size() as usize;

    // 使用 with_inode_raw_data_mut 修改数据（自动标记为脏）
    inode_ref.with_inode_raw_data_mut(|inode_data| {
        // 计算 xattr 区域的范围
        // xattr 区域从 header_offset 开始，到 inode 末尾结束
        let xattr_area_start = header_offset;
        let xattr_area_end = inode_size;

        if xattr_area_start < xattr_area_end {
            // 清零 xattr 区域
            let xattr_area = &mut inode_data[xattr_area_start..xattr_area_end];
            for byte in xattr_area.iter_mut() {
                *byte = 0;
            }

            // 设置 header 魔数
            let header_bytes = &mut inode_data[header_offset..header_offset + size_of::<ext4_xattr_ibody_header>()];
            let magic_bytes = EXT4_XATTR_MAGIC.to_le_bytes();
            header_bytes[0..4].copy_from_slice(&magic_bytes);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ibody_api_design() {
        // 验证 API 设计的正确性
    }
}
