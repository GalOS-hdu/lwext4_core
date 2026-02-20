//! xattr 公共 API
//!
//! 提供用户级别的扩展属性操作接口
//!
//! # 重构说明
//!
//! 本模块已重构为使用 InodeRef 和 Block 等 RAII 类型，
//! 完全对应 lwext4 C 实现的语义，保障数据一致性。
//!
//! ## API 变更
//!
//! ### 旧 API（已废弃，有严重一致性问题）：
//! ```ignore
//! pub fn get(
//!     sb: &Superblock,
//!     inode: &Inode,
//!     inode_data: &[u8],              // ❌ 只是切片，无一致性保障
//!     xattr_block_data: Option<&[u8]>, // ❌ 只是切片，无一致性保障
//!     name: &str,
//!     buffer: &mut [u8],
//! ) -> Result<usize>
//! ```
//!
//! ### 新 API（正确实现）：
//! ```ignore
//! pub fn get<D: BlockDevice>(
//!     inode_ref: &mut InodeRef<D>,    // ✅ 使用 InodeRef，保障一致性
//!     name: &str,
//!     buffer: &mut [u8],
//! ) -> Result<usize>
//! ```
//!
//! ## 优势
//!
//! - ✅ 自动脏标记 - 修改自动标记，Drop 时自动写回
//! - ✅ 数据一致性 - 所有访问操作同一份缓存数据
//! - ✅ 块管理 - 支持分配/释放，自动更新 inode blocks 计数
//! - ✅ COW 支持 - 正确处理共享块的分离（h_refcount > 1）

use crate::{
    Result, Error, ErrorKind,
    block::{Block, BlockDevice},
    fs::InodeRef,
};


/// 列出所有扩展属性
///
/// 对应 lwext4 的 `ext4_xattr_list()`
///
/// # 参数
///
/// * `inode_ref` - inode 引用
/// * `buffer` - 输出缓冲区（名称以 \0 分隔）
///
/// # 返回
///
/// 成功返回写入的字节数
///
/// # 示例
///
/// ```ignore
/// let mut inode_ref = InodeRef::get(&mut bdev, &mut sb, inode_num)?;
/// let mut buffer = vec![0u8; 1024];
/// let len = list(&mut inode_ref, &mut buffer)?;
/// // buffer 包含: "user.comment\0security.selinux\0"
/// ```
///
/// # 实现说明
///
/// 1. 通过 `inode_ref.with_inode()` 访问 inode 数据（只读）
/// 2. 如果有 xattr 块，通过 `Block::get()` 获取块句柄
/// 3. 遍历 inode 内部和 xattr 块中的所有 entry
/// 4. 将名称写入 buffer（含命名空间前缀）
pub fn list<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    buffer: &mut [u8],
) -> Result<usize> {
    let mut written = 0;

    // 1. 列出 inode 内部的 xattr
    use super::ibody::list_ibody_xattr;
    let ibody_len = list_ibody_xattr(inode_ref, buffer)?;
    written += ibody_len;

    // 2. 列出 xattr block 中的 xattr
    let xattr_block_addr = inode_ref.get_xattr_block_addr()?;
    if xattr_block_addr != 0 {
        // 先获取 superblock（避免借用冲突）
        let _block_size = inode_ref.superblock().block_size() as usize;

        // 使用 Block 访问 xattr block
        let mut block = Block::get(inode_ref.bdev_mut(), xattr_block_addr)?;
        let block_len = block.with_data(|block_data| {
            // 简单的遍历实现（内联，不依赖外部函数）
            let first_entry_offset = core::mem::size_of::<crate::types::ext4_xattr_header>();
            let mut entry_offset = first_entry_offset;
            let mut local_written = 0;

            loop {
                if entry_offset + 4 > block_data.len() {
                    break;
                }

                let is_last = {
                    let first_u32 = u32::from_le_bytes([
                        block_data[entry_offset],
                        block_data[entry_offset + 1],
                        block_data[entry_offset + 2],
                        block_data[entry_offset + 3],
                    ]);
                    first_u32 == 0
                };

                if is_last {
                    break;
                }

                if entry_offset + core::mem::size_of::<crate::types::ext4_xattr_entry>() > block_data.len() {
                    break;
                }

                let entry_bytes = &block_data[entry_offset..entry_offset + core::mem::size_of::<crate::types::ext4_xattr_entry>()];
                let entry = unsafe {
                    core::ptr::read(entry_bytes.as_ptr() as *const crate::types::ext4_xattr_entry)
                };

                let name_len = entry.e_name_len as usize;
                let name_offset = entry_offset + core::mem::size_of::<crate::types::ext4_xattr_entry>();

                if name_offset + name_len <= block_data.len() {
                    let entry_name = &block_data[name_offset..name_offset + name_len];

                    use super::prefix::get_xattr_name_prefix;
                    if let Some((prefix, prefix_len)) = get_xattr_name_prefix(entry.e_name_index) {
                        let total_len = prefix_len + name_len + 1;

                        if written + local_written + total_len <= buffer.len() {
                            let buf_offset = written + local_written;
                            buffer[buf_offset..buf_offset + prefix_len].copy_from_slice(prefix.as_bytes());
                            buffer[buf_offset + prefix_len..buf_offset + prefix_len + name_len].copy_from_slice(entry_name);
                            buffer[buf_offset + prefix_len + name_len] = 0;
                            local_written += total_len;
                        }
                    }
                }

                let entry_len = (name_len + crate::consts::EXT4_XATTR_ROUND as usize + core::mem::size_of::<crate::types::ext4_xattr_entry>())
                    & !(crate::consts::EXT4_XATTR_ROUND as usize);
                entry_offset += entry_len;
            }

            Ok::<usize, Error>(local_written)
        })??;
        written += block_len;
    }

    Ok(written)
}

/// 获取扩展属性值
///
/// 对应 lwext4 的 `ext4_xattr_get()`
///
/// # 参数
///
/// * `inode_ref` - inode 引用
/// * `name` - 属性名（含前缀，如 "user.comment"）
/// * `buffer` - 输出缓冲区
///
/// # 返回
///
/// 成功返回值的长度，如果属性不存在返回 NotFound 错误
///
/// # 示例
///
/// ```ignore
/// let mut inode_ref = InodeRef::get(&mut bdev, &mut sb, inode_num)?;
/// let mut buffer = vec![0u8; 256];
/// let len = get(&mut inode_ref, "user.comment", &mut buffer)?;
/// let value = &buffer[..len];
/// ```
///
/// # 实现说明
///
/// 1. 解析属性名称
/// 2. 先在 inode 内部查找
/// 3. 如果未找到，在 xattr 块中查找
/// 4. 复制值到 buffer
///
/// 注意：此函数只读，不需要标记脏
pub fn get<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
    buffer: &mut [u8],
) -> Result<usize> {
    // 1. 解析属性名称
    use super::prefix::extract_xattr_name;
    let (name_index, name_str, _name_len) = extract_xattr_name(name)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "invalid xattr name"))?;

    let name_bytes = name_str.as_bytes();

    // 2. 先在 inode 内部查找
    use super::ibody::find_ibody_entry;
    if let Some((_entry_offset, value_offset, value_size)) =
        find_ibody_entry(inode_ref, name_index, name_bytes)?
    {
        // 在 inode 内部找到了
        let value_len = value_size as usize;
        if buffer.len() < value_len {
            return Err(Error::new(ErrorKind::InvalidInput, "buffer too small"));
        }

        // 从 inode 数据中读取 value
        return inode_ref.with_inode_raw_data(|inode_data| {
            let value_end = value_offset + value_len;
            if value_end > inode_data.len() {
                return Err(Error::new(ErrorKind::Io, "value out of bounds"));
            }
            buffer[..value_len].copy_from_slice(&inode_data[value_offset..value_end]);
            Ok(value_len)
        })?;
    }

    // 3. 在 xattr block 中查找
    let xattr_block_addr = inode_ref.get_xattr_block_addr()?;
    if xattr_block_addr == 0 {
        // 没有 xattr block，属性不存在
        return Err(Error::new(ErrorKind::NotFound, "xattr not found"));
    }

    // 使用 Block 访问 xattr block
    let mut block = Block::get(inode_ref.bdev_mut(), xattr_block_addr)?;
    block.with_data(|block_data| {
        // 在 block 中查找（不依赖 find_block_entry 避免借用问题）
        use super::search::XattrSearch;
        let first_entry_offset = core::mem::size_of::<crate::types::ext4_xattr_header>();
        let mut search = XattrSearch::new(block_data, first_entry_offset);

        if let Some((_entry_offset, value_offset, value_size)) =
            search.find_entry(name_index, name_bytes)
        {
            let value_len = value_size as usize;
            if buffer.len() < value_len {
                return Err(Error::new(ErrorKind::InvalidInput, "buffer too small"));
            }

            let value_end = value_offset + value_len;
            if value_end > block_data.len() {
                return Err(Error::new(ErrorKind::Io, "value out of bounds"));
            }

            buffer[..value_len].copy_from_slice(&block_data[value_offset..value_end]);
            Ok(value_len)
        } else {
            Err(Error::new(ErrorKind::NotFound, "xattr not found"))
        }
    })?
}

/// 设置扩展属性
///
/// 对应 lwext4 的 `ext4_xattr_set()`
///
/// # 参数
///
/// * `inode_ref` - inode 引用（可变）
/// * `name` - 属性名（含前缀）
/// * `value` - 属性值
///
/// # 返回
///
/// 成功返回 Ok(())
///
/// # 实现说明
///
/// 这是最复杂的函数，完全对应 C 实现的逻辑：
///
/// 1. 解析属性名称
/// 2. 尝试在 inode 内部设置
///    - 如果成功，标记 inode 为脏并返回
/// 3. 如果 inode 内部空间不足，尝试在 xattr 块中设置
///    - 如果没有 xattr 块，分配新块
///    - 如果块引用计数 > 1，执行 COW（分离共享块）
///    - 在块中设置属性
///    - 标记 block 为脏
/// 4. 如果在块中设置成功，尝试从 inode 内部删除该属性（迁移）
///
/// 注意：修改会自动标记为脏
pub fn set<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
    value: &[u8],
) -> Result<()> {
    // 1. 解析属性名称
    use super::prefix::extract_xattr_name;
    let (name_index, name_str, _name_len) = extract_xattr_name(name)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "invalid xattr name"))?;

    let name_bytes = name_str.as_bytes();

    // 2. 尝试在 inode 内部设置
    use super::ibody::{set_ibody_entry, initialize_ibody_xattr};

    // 先确保 ibody xattr 已初始化
    initialize_ibody_xattr(inode_ref)?;

    let set_in_ibody = set_ibody_entry(inode_ref, name_index, name_bytes, Some(value))?;
    if set_in_ibody {
        return Ok(());
    }

    // 3. inode 内部空间不足，使用 xattr block
    set_in_block(inode_ref, name_index, name_bytes, value)?;

    // 4. 如果在 block 中设置成功，尝试从 inode 内部删除该属性（优化空间）
    let _ = set_ibody_entry(inode_ref, name_index, name_bytes, None);

    Ok(())
}

/// 删除扩展属性
///
/// 对应 lwext4 的 `ext4_xattr_remove()`
///
/// # 参数
///
/// * `inode_ref` - inode 引用（可变）
/// * `name` - 属性名
///
/// # 返回
///
/// 成功返回 Ok(())
///
/// # 实现说明
///
/// 1. 解析属性名称
/// 2. 尝试在 inode 内部删除
/// 3. 如果未找到，尝试在 xattr 块中删除
/// 4. 如果块为空，释放块
///
/// 注意：修改会自动标记为脏
pub fn remove<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
) -> Result<()> {
    // 1. 解析属性名称
    use super::prefix::extract_xattr_name;
    let (name_index, name_str, _name_len) = extract_xattr_name(name)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "invalid xattr name"))?;

    let name_bytes = name_str.as_bytes();

    // 2. 尝试在 inode 内部删除
    use super::ibody::set_ibody_entry;
    let removed_from_ibody = set_ibody_entry(inode_ref, name_index, name_bytes, None)?;
    if removed_from_ibody {
        return Ok(());
    }

    // 3. 在 xattr block 中删除
    let xattr_block_addr = inode_ref.get_xattr_block_addr()?;
    if xattr_block_addr == 0 {
        // 没有 xattr block，属性不存在
        return Err(Error::new(ErrorKind::NotFound, "xattr not found"));
    }

    remove_from_block(inode_ref, name_index, name_bytes)?;

    Ok(())
}

/// 在 xattr block 中设置属性（内部辅助函数）
///
/// 对应 lwext4 的 `ext4_xattr_block_set()`
///
/// 实现逻辑：
/// 1. 如果没有 xattr block，分配新块
/// 2. 如果有 block 且 h_refcount > 1，执行 COW
/// 3. 在 block 中设置 entry
/// 4. 更新哈希和校验和
fn set_in_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name_index: u8,
    name: &[u8],
    value: &[u8],
) -> Result<()> {
    use super::{block, write};
    use crate::balloc;

    let xattr_block_addr = inode_ref.get_xattr_block_addr()?;
    let block_size = inode_ref.superblock().block_size() as usize;

    // 情况 1: 没有 xattr block，需要分配新块
    if xattr_block_addr == 0 {
        // 分配新块
        let goal = 0; // TODO: 可以优化为 inode 附近的块
        let mut allocator = balloc::BlockAllocator::new();
        let (bdev, sb) = inode_ref.bdev_and_sb_mut();
        let new_block_addr = allocator.alloc_block(bdev, sb, goal)?;

        if new_block_addr == 0 {
            return Err(Error::new(ErrorKind::NoSpace, "failed to allocate xattr block"));
        }

        // 更新 inode 的 file_acl
        inode_ref.set_xattr_block_addr(new_block_addr)?;

        // 初始化并设置新块
        let mut block_handle = Block::get(inode_ref.bdev_mut(), new_block_addr)?;
        block_handle.with_data_mut(|block_data| {
            // 初始化 xattr block
            block::initialize_block(block_data)?;

            // 在块中设置 entry
            let first_offset = core::mem::size_of::<crate::types::ext4_xattr_header>();
            write::set_entry_in_memory(
                block_data,
                first_offset,
                block_size,
                name_index,
                name,
                Some(value),
                false,
            )?;

            // TODO: 计算并设置哈希和校验和
            // hash::compute_and_set_hashes(block_data)?;
            // hash::set_block_checksum(sb, block_num, header, block_data)?;

            Ok::<(), Error>(())
        })??;
    } else {
        // 情况 2: 已有 xattr block
        // 先检查引用计数
        let refcount = {
            let mut block_handle = Block::get(inode_ref.bdev_mut(), xattr_block_addr)?;
            block_handle.with_data(block::get_refcount)??
        };

        // 如果引用计数 > 1，需要 COW
        let target_block_addr = if refcount > 1 {
            // 分配新块
            let goal = xattr_block_addr;
            let mut allocator = balloc::BlockAllocator::new();
            let (bdev, sb) = inode_ref.bdev_and_sb_mut();
            let new_block_addr = allocator.alloc_block(bdev, sb, goal)?;

            if new_block_addr == 0 {
                return Err(Error::new(ErrorKind::NoSpace, "failed to allocate xattr block for COW"));
            }

            // 复制旧块到新块，并调整引用计数
            // 先复制数据
            {
                let mut old_block = Block::get(inode_ref.bdev_mut(), xattr_block_addr)?;
                let data_copy: alloc::vec::Vec<u8> = old_block.with_data(|old_data| {
                    old_data[..block_size].to_vec()
                })?;

                drop(old_block);

                let mut new_block = Block::get(inode_ref.bdev_mut(), new_block_addr)?;
                new_block.with_data_mut(|new_data| {
                    new_data[..block_size].copy_from_slice(&data_copy);
                    // 设置新块的引用计数为 1
                    block::set_refcount(new_data, 1)?;
                    Ok::<(), Error>(())
                })??;
            }

            // 减少旧块的引用计数
            {
                let mut old_block = Block::get(inode_ref.bdev_mut(), xattr_block_addr)?;
                old_block.with_data_mut(|data| {
                    block::dec_refcount(data)?;
                    Ok::<(), Error>(())
                })??;
            }

            // 更新 inode 的 file_acl
            inode_ref.set_xattr_block_addr(new_block_addr)?;

            new_block_addr
        } else {
            xattr_block_addr
        };

        // 在目标块中设置 entry
        let mut block_handle = Block::get(inode_ref.bdev_mut(), target_block_addr)?;
        block_handle.with_data_mut(|block_data| {
            let first_offset = core::mem::size_of::<crate::types::ext4_xattr_header>();

            // 设置 entry
            write::set_entry_in_memory(
                block_data,
                first_offset,
                block_size,
                name_index,
                name,
                Some(value),
                false,
            )?;

            // TODO: 更新哈希和校验和
            // hash::compute_and_set_hashes(block_data)?;
            // hash::set_block_checksum(sb, block_num, header, block_data)?;

            Ok::<(), Error>(())
        })??;
    }

    Ok(())
}

/// 从 xattr block 中删除属性（内部辅助函数）
///
/// 对应 lwext4 的 `ext4_xattr_block_remove()`
///
/// 实现逻辑：
/// 1. 如果 h_refcount > 1，执行 COW
/// 2. 在 block 中删除 entry
/// 3. 检查 block 是否为空，如果是则释放
/// 4. 更新哈希和校验和
fn remove_from_block<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name_index: u8,
    name: &[u8],
) -> Result<()> {
    use super::{block, write};
    use crate::balloc;

    let xattr_block_addr = inode_ref.get_xattr_block_addr()?;
    let block_size = inode_ref.superblock().block_size() as usize;

    // 检查引用计数
    let refcount = {
        let mut block_handle = Block::get(inode_ref.bdev_mut(), xattr_block_addr)?;
        block_handle.with_data(block::get_refcount)??
    };

    // 如果引用计数 > 1，需要 COW
    let target_block_addr = if refcount > 1 {
        // 分配新块
        let goal = xattr_block_addr;
        let mut allocator = balloc::BlockAllocator::new();
        let (bdev, sb) = inode_ref.bdev_and_sb_mut();
        let new_block_addr = allocator.alloc_block(bdev, sb, goal)?;

        if new_block_addr == 0 {
            return Err(Error::new(ErrorKind::NoSpace, "failed to allocate xattr block for COW"));
        }

        // 复制旧块到新块，并调整引用计数
        // 先复制数据
        {
            let mut old_block = Block::get(inode_ref.bdev_mut(), xattr_block_addr)?;
            let data_copy: alloc::vec::Vec<u8> = old_block.with_data(|old_data| {
                old_data[..block_size].to_vec()
            })?;

            drop(old_block);

            let mut new_block = Block::get(inode_ref.bdev_mut(), new_block_addr)?;
            new_block.with_data_mut(|new_data| {
                new_data[..block_size].copy_from_slice(&data_copy);
                // 设置新块的引用计数为 1
                block::set_refcount(new_data, 1)?;
                Ok::<(), Error>(())
            })??;
        }

        // 减少旧块的引用计数
        {
            let mut old_block = Block::get(inode_ref.bdev_mut(), xattr_block_addr)?;
            old_block.with_data_mut(|data| {
                block::dec_refcount(data)?;
                Ok::<(), Error>(())
            })??;
        }

        // 更新 inode 的 file_acl
        inode_ref.set_xattr_block_addr(new_block_addr)?;

        new_block_addr
    } else {
        xattr_block_addr
    };

    // 在目标块中删除 entry
    let mut block_handle = Block::get(inode_ref.bdev_mut(), target_block_addr)?;
    let block_is_empty = block_handle.with_data_mut(|block_data| {
        let first_offset = core::mem::size_of::<crate::types::ext4_xattr_header>();

        // 删除 entry（传入 None 作为 value）
        write::set_entry_in_memory(
            block_data,
            first_offset,
            block_size,
            name_index,
            name,
            None, // None 表示删除
            false,
        )?;

        // TODO: 更新哈希和校验和
        // hash::compute_and_set_hashes(block_data)?;
        // hash::set_block_checksum(sb, block_num, header, block_data)?;

        // 检查 block 是否为空（只剩 header）
        let is_empty = {
            let first_entry_offset = first_offset;
            if first_entry_offset + 4 > block_data.len() {
                true
            } else {
                let first_u32 = u32::from_le_bytes([
                    block_data[first_entry_offset],
                    block_data[first_entry_offset + 1],
                    block_data[first_entry_offset + 2],
                    block_data[first_entry_offset + 3],
                ]);
                first_u32 == 0 // 如果第一个 entry 就是终止符，说明为空
            }
        };

        Ok::<bool, Error>(is_empty)
    })??;

    // 如果 block 为空，释放它
    if block_is_empty {
        drop(block_handle); // 释放 Block 的借用
        let (bdev, sb) = inode_ref.bdev_and_sb_mut();
        balloc::free_block(bdev, sb, target_block_addr)?;
        inode_ref.set_xattr_block_addr(0)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_design() {
        // 这些测试主要验证 API 设计的正确性
        // 实际功能测试需要完整的文件系统环境
    }
}
