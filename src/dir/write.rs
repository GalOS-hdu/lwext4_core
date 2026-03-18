//! 目录写操作
//!
//! 提供目录项的添加、删除等写操作功能。
//! 目录初始化操作见 [`super::write_init`]。
//!
//! 对应 lwext4 的 ext4_dir.c 中的写操作部分

use crate::{
    block::{Block, BlockDevice},
    bytes::{read_struct_at, write_struct_at},
    consts::*,
    dir::{checksum, htree},
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    superblock::Superblock,
    types::{ext4_dir_entry, ext4_dir_entry_tail},
};

/// 目录项类型常量
pub const EXT4_DE_UNKNOWN: u8 = 0;
pub const EXT4_DE_REG_FILE: u8 = 1;
pub const EXT4_DE_DIR: u8 = 2;
pub const EXT4_DE_CHRDEV: u8 = 3;
pub const EXT4_DE_BLKDEV: u8 = 4;
pub const EXT4_DE_FIFO: u8 = 5;
pub const EXT4_DE_SOCK: u8 = 6;
pub const EXT4_DE_SYMLINK: u8 = 7;

/// 向目录添加新条目
///
/// 对应 lwext4 的 `ext4_dir_add_entry()`
pub fn add_entry<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    name: &str,
    child_inode: u32,
    file_type: u8,
) -> Result<()> {
    if name.is_empty() || name.len() > 255 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Directory entry name too long or empty",
        ));
    }

    let is_htree = htree::is_indexed(inode_ref)?;

    if is_htree {
        add_entry_htree(inode_ref, sb, name, child_inode, file_type)
    } else {
        add_entry_linear(inode_ref, sb, name, child_inode, file_type)
    }
}

/// 向普通目录（线性扫描）添加条目
fn add_entry_linear<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    name: &str,
    child_inode: u32,
    file_type: u8,
) -> Result<()> {
    let name_len = name.len();
    let required_len = calculate_entry_len(name_len as u8);

    let mut block_idx = 0_u32;
    loop {
        let block_addr = match inode_ref.get_inode_dblk_idx(block_idx, false) {
            Ok(addr) => addr,
            Err(_) => {
                return super::write_init::append_new_block(
                    inode_ref,
                    sb,
                    name,
                    child_inode,
                    file_type,
                    required_len,
                );
            }
        };

        {
            let has_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
            let block_size = inode_ref.sb().block_size() as usize;
            let uuid = inode_ref.sb().inner().uuid;
            let inode_index = inode_ref.index();
            let inode_generation = inode_ref.generation()?;

            let bdev = inode_ref.bdev();
            let mut block = Block::get(bdev, block_addr)?;

            let insert_result = block.with_data_mut(|data| {
                let result = find_and_insert_entry(
                    data,
                    name,
                    child_inode,
                    file_type,
                    required_len,
                );

                if result {
                    update_dir_block_checksum(
                        has_csum,
                        &uuid,
                        inode_index,
                        inode_generation,
                        data,
                        block_size,
                    );
                }

                result
            })?;

            drop(block);

            if insert_result {
                return Ok(());
            }
        }

        block_idx += 1;
    }
}

/// 向 HTree 索引目录添加条目
///
/// 对应 lwext4 的 `ext4_dir_dx_add_entry()`
fn add_entry_htree<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    sb: &mut Superblock,
    name: &str,
    child_inode: u32,
    file_type: u8,
) -> Result<()> {
    let hash_info = htree::init_hash_info(inode_ref, name)?;
    let path = htree::get_leaf_with_path(inode_ref, &hash_info)?;
    let leaf_block_idx = path.leaf_block;

    let block_addr = inode_ref.get_inode_dblk_idx(leaf_block_idx, false)?;
    let required_len = calculate_entry_len(name.len() as u8);

    let has_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
    let block_size = inode_ref.sb().block_size() as usize;
    let uuid = inode_ref.sb().inner().uuid;
    let inode_index = inode_ref.index();
    let inode_generation = inode_ref.generation()?;

    let bdev = inode_ref.bdev();
    let mut block = Block::get(bdev, block_addr)?;

    let insert_result = block.with_data_mut(|data| {
        let result = find_and_insert_entry(
            data,
            name,
            child_inode,
            file_type,
            required_len,
        );

        if result {
            update_dir_block_checksum(
                has_csum,
                &uuid,
                inode_index,
                inode_generation,
                data,
                block_size,
            );
        }

        result
    })?;

    drop(block);

    if !insert_result {
        // 叶子块满了，需要分裂
        super::htree_split::handle_leaf_split(
            inode_ref,
            sb,
            &hash_info,
            &path,
            block_addr,
            name,
            child_inode,
            file_type,
        )?;
    }

    Ok(())
}

/// 在块中查找空闲空间并插入条目
///
/// 成功插入返回 true，空间不足返回 false
pub(super) fn find_and_insert_entry(
    data: &mut [u8],
    name: &str,
    child_inode: u32,
    file_type: u8,
    required_len: u16,
) -> bool {
    let mut offset = 0;
    let mut entries_checked = 0;

    log::trace!(
        "[find_and_insert_entry] START: name='{}', required_len={}, block_size={}",
        name,
        required_len,
        data.len()
    );

    while offset < data.len() {
        entries_checked += 1;
        if offset + core::mem::size_of::<ext4_dir_entry>() > data.len() {
            break;
        }

        let entry: ext4_dir_entry = match read_struct_at(data, offset) {
            Ok(e) => e,
            Err(_) => break,
        };

        let rec_len = u16::from_le(entry.rec_len);

        if rec_len == 0 {
            break;
        }

        let entry_inode = u32::from_le(entry.inode);
        let actual_len = if entry_inode != 0 {
            calculate_entry_len(entry.name_len)
        } else {
            0
        };

        let free_space = match rec_len.checked_sub(actual_len) {
            Some(space) => space,
            None => {
                offset += rec_len as usize;
                continue;
            }
        };

        if free_space >= required_len {
            log::trace!(
                "[find_and_insert_entry] FOUND SPACE: offset={offset}, rec_len={rec_len}, actual_len={actual_len}, free_space={free_space}, required_len={required_len}, entry_inode={entry_inode}, entries_checked={entries_checked}"
            );
            if entry_inode != 0 && actual_len > 0 {
                split_entry_and_insert(
                    data,
                    offset,
                    actual_len,
                    name,
                    child_inode,
                    file_type,
                    required_len,
                );
            } else {
                write_entry(
                    data,
                    offset,
                    name,
                    child_inode,
                    file_type,
                    rec_len,
                );
            }
            return true;
        }

        offset += rec_len as usize;
    }

    log::trace!(
        "[find_and_insert_entry] NO SPACE: name='{name}', entries_checked={entries_checked}, final_offset={offset}"
    );
    false
}

/// 分裂现有条目并插入新条目
fn split_entry_and_insert(
    data: &mut [u8],
    offset: usize,
    actual_len: u16,
    name: &str,
    child_inode: u32,
    file_type: u8,
    _required_len: u16,
) {
    // 读取原条目的 rec_len
    let old_entry: ext4_dir_entry = match read_struct_at(data, offset) {
        Ok(e) => e,
        Err(_) => return,
    };
    let total_len = u16::from_le(old_entry.rec_len);

    // 更新原条目的 rec_len 为实际长度
    let new_old_entry = ext4_dir_entry {
        rec_len: actual_len.to_le(),
        ..old_entry
    };
    let _ = write_struct_at(data, offset, &new_old_entry);

    // 在原条目后面写入新条目
    let new_offset = offset + actual_len as usize;
    let new_rec_len = total_len - actual_len;

    write_entry(
        data,
        new_offset,
        name,
        child_inode,
        file_type,
        new_rec_len,
    );
}

/// 写入目录项
pub(super) fn write_entry(
    data: &mut [u8],
    offset: usize,
    name: &str,
    inode: u32,
    file_type: u8,
    rec_len: u16,
) {
    let entry = ext4_dir_entry {
        inode: inode.to_le(),
        rec_len: rec_len.to_le(),
        name_len: name.len() as u8,
        file_type,
    };
    let _ = write_struct_at(data, offset, &entry);

    // 写入名称
    let name_bytes = name.as_bytes();
    let name_offset = offset + core::mem::size_of::<ext4_dir_entry>();
    data[name_offset..name_offset + name_bytes.len()].copy_from_slice(name_bytes);
}

/// 计算目录项所需长度（4字节对齐）
pub(super) fn calculate_entry_len(name_len: u8) -> u16 {
    let base_len = core::mem::size_of::<ext4_dir_entry>() + name_len as usize;
    ((base_len + 3) & !3) as u16
}

/// 更新目录块校验和（不需要 InodeRef 的版本）
///
/// 接受提前提取的标量数据，避免与 bdev() 的可变借用冲突
pub(super) fn update_dir_block_checksum(
    has_csum: bool,
    uuid: &[u8; 16],
    inode_index: u32,
    inode_generation: u32,
    data: &mut [u8],
    block_size: usize,
) {
    if !has_csum {
        return;
    }

    #[cfg(feature = "metadata-csum")]
    {
        const EXT4_CRC32_INIT: u32 = 0xFFFFFFFF;

        let tail_offset = block_size - core::mem::size_of::<ext4_dir_entry_tail>();

        let mut csum = crate::crc::crc32c_append(EXT4_CRC32_INIT, uuid);

        let ino_index = inode_index.to_le_bytes();
        csum = crate::crc::crc32c_append(csum, &ino_index);

        let ino_gen = inode_generation.to_le_bytes();
        csum = crate::crc::crc32c_append(csum, &ino_gen);

        let dirent_data = &data[..tail_offset];
        csum = crate::crc::crc32c_append(csum, dirent_data);

        if let Some(tail) = checksum::get_tail_mut(data, block_size) {
            tail.set_checksum(csum);
        }
    }

    #[cfg(not(feature = "metadata-csum"))]
    {
        let _ = (uuid, inode_index, inode_generation, data, block_size);
    }
}

/// 删除目录条目
///
/// 对应 lwext4 的 `ext4_dir_remove_entry()`
///
/// issue: 直接遍历所有逻辑块查找，有待优化为使用 hashinfo
pub fn remove_entry<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
    name: &str,
) -> Result<()> {
    let mut block_idx = 0_u32;
    loop {
        let block_addr = match inode_ref.get_inode_dblk_idx(block_idx, false) {
            Ok(addr) => addr,
            Err(_) => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "Directory entry not found",
                ));
            }
        };

        let has_csum = inode_ref.sb().has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_METADATA_CSUM);
        let block_size = inode_ref.sb().block_size() as usize;
        let uuid = inode_ref.sb().inner().uuid;
        let inode_index = inode_ref.index();
        let inode_generation = inode_ref.generation()?;

        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, block_addr)?;

        let found = block.with_data_mut(|data| {
            let result = remove_entry_from_block(data, name);

            if result {
                update_dir_block_checksum(
                    has_csum,
                    &uuid,
                    inode_index,
                    inode_generation,
                    data,
                    block_size,
                );
            }

            result
        })?;

        drop(block);

        if found {
            return Ok(());
        }

        block_idx += 1;
    }
}

/// 从块中删除条目
fn remove_entry_from_block(data: &mut [u8], name: &str) -> bool {
    let mut prev_offset: Option<usize> = None;
    let mut offset = 0;

    while offset < data.len() {
        if offset + core::mem::size_of::<ext4_dir_entry>() > data.len() {
            break;
        }

        let entry: ext4_dir_entry = match read_struct_at(data, offset) {
            Ok(e) => e,
            Err(_) => break,
        };

        let rec_len = u16::from_le(entry.rec_len);
        if rec_len == 0 {
            break;
        }

        let entry_inode = u32::from_le(entry.inode);

        if entry_inode != 0 {
            let name_offset = offset + core::mem::size_of::<ext4_dir_entry>();
            let entry_name_len = entry.name_len as usize;

            if name_offset + entry_name_len <= data.len() {
                let entry_name = &data[name_offset..name_offset + entry_name_len];

                if entry_name == name.as_bytes() {
                    if let Some(prev_off) = prev_offset {
                        // 合并到前一个条目
                        let prev_entry: ext4_dir_entry = match read_struct_at(data, prev_off) {
                            Ok(e) => e,
                            Err(_) => return false,
                        };
                        let prev_rec_len = u16::from_le(prev_entry.rec_len);
                        let new_prev = ext4_dir_entry {
                            rec_len: (prev_rec_len + rec_len).to_le(),
                            ..prev_entry
                        };
                        let _ = write_struct_at(data, prev_off, &new_prev);
                    } else {
                        // 第一个条目，标记为删除
                        let deleted = ext4_dir_entry {
                            inode: 0_u32.to_le(),
                            ..entry
                        };
                        let _ = write_struct_at(data, offset, &deleted);
                    }

                    return true;
                }
            }
        }

        prev_offset = Some(offset);
        offset += rec_len as usize;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_entry_len() {
        assert_eq!(calculate_entry_len(1), 12);
        assert_eq!(calculate_entry_len(2), 12);
        assert_eq!(calculate_entry_len(4), 12);
        assert_eq!(calculate_entry_len(5), 16);
        assert_eq!(calculate_entry_len(8), 16);
    }

    #[test]
    fn test_dir_entry_constants() {
        assert_eq!(EXT4_DE_REG_FILE, 1);
        assert_eq!(EXT4_DE_DIR, 2);
        assert_eq!(EXT4_DE_SYMLINK, 7);
    }
}
