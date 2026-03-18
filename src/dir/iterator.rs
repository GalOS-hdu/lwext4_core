//! 目录迭代器
//!
//! 对应 lwext4 的 `ext4_dir_iter` 相关功能
//!
//! ## 设计说明
//!
//! 与旧的 `entry.rs` 实现不同，新的设计遵循以下原则：
//!
//! 1. **不持有引用** - 迭代器只保存状态（偏移、位置等），不持有 InodeRef 或 Block
//! 2. **按需访问** - 每次操作时传入所需的 InodeRef 和 BlockDev
//! 3. **使用 Block handle** - 通过 Block::get() 直接访问缓存的块，零拷贝
//! 4. **符合 lwext4 设计** - 与 C 实现的逻辑保持一致

use crate::{
    block::{Block, BlockDevice},
    consts::*,
    error::{Error, ErrorKind, Result},
    fs::InodeRef,
    types::ext4_dir_entry,
};
use alloc::string::String;

/// 目录迭代器状态
///
/// 对应 lwext4 的 `struct ext4_dir_iter`
///
/// ## 与 lwext4 的差异
///
/// lwext4 保存 `inode_ref` 指针和 `curr_blk` 块引用。
/// Rust 由于借用规则限制，迭代器只保存状态，不持有引用。
pub struct DirIterator {
    /// 当前文件偏移（字节）
    curr_off: u64,
    /// 当前块的逻辑块号
    current_block_idx: u32,
    /// 当前块内的偏移
    offset_in_block: usize,
    /// 目录的总大小
    total_size: u64,
    /// 是否已初始化
    #[allow(dead_code)]
    initialized: bool,
}

impl DirIterator {
    /// 创建新的目录迭代器
    ///
    /// 对应 lwext4 的 `ext4_dir_iterator_init()`
    ///
    /// # 参数
    ///
    /// * `inode_ref` - 目录的 inode 引用
    /// * `pos` - 起始位置（字节偏移）
    pub fn new<D: BlockDevice>(inode_ref: &mut InodeRef<D>, pos: u64) -> Result<Self> {
        // 检查是否是目录
        let is_dir = inode_ref.with_inode(|inode| {
            (u16::from_le(inode.mode) & EXT4_INODE_MODE_TYPE_MASK) == EXT4_INODE_MODE_DIRECTORY
        })?;

        if !is_dir {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Inode is not a directory",
            ));
        }

        let total_size = inode_ref.size()?;
        let block_size = inode_ref.sb().block_size();

        Ok(Self {
            curr_off: pos,
            current_block_idx: (pos / block_size as u64) as u32,
            offset_in_block: (pos % block_size as u64) as usize,
            total_size,
            initialized: false,
        })
    }

    /// 定位到指定偏移
    ///
    /// 对应 lwext4 的 `ext4_dir_iterator_seek()`
    ///
    /// # 参数
    ///
    /// * `inode_ref` - 目录的 inode 引用
    /// * `pos` - 目标位置（字节偏移）
    pub fn seek<D: BlockDevice>(&mut self, inode_ref: &mut InodeRef<D>, pos: u64) -> Result<()> {
        // 如果超出文件大小，只更新偏移
        if pos >= self.total_size {
            self.curr_off = pos;
            return Ok(());
        }

        let block_size = inode_ref.sb().block_size() as u64;
        let new_block_idx = (pos / block_size) as u32;

        self.curr_off = pos;
        self.current_block_idx = new_block_idx;
        self.offset_in_block = (pos % block_size) as usize;

        Ok(())
    }

    /// 获取当前偏移
    pub fn current_offset(&self) -> u64 {
        self.curr_off
    }

    /// 是否到达目录末尾
    pub fn is_at_end(&self) -> bool {
        self.curr_off >= self.total_size
    }

    /// 获取下一个目录项
    ///
    /// 对应 lwext4 的 `ext4_dir_iterator_next()` 和相关逻辑
    /// 从该inode的逻辑块0开始遍历
    /// # 参数
    ///
    /// * `inode_ref` - 目录的 inode 引用
    ///
    /// # 返回
    ///
    /// - `Ok(Some(DirEntry))` - 成功获取下一个目录项
    /// - `Ok(None)` - 已到达目录末尾
    /// - `Err(_)` - 发生错误
    pub fn next<D: BlockDevice>(
        &mut self,
        inode_ref: &mut InodeRef<D>,
    ) -> Result<Option<DirEntry>> {
        let block_size = inode_ref.sb().block_size() as usize;

        loop {
            // 检查是否到达末尾
            if self.is_at_end() {
                return Ok(None);
            }

            // 如果跨越块边界，移动到下一个块
            if self.offset_in_block >= block_size {
                self.current_block_idx += 1;
                self.offset_in_block = 0;
                self.curr_off = self.current_block_idx as u64 * block_size as u64;

                if self.is_at_end() {
                    return Ok(None);
                }
            }

            // 读取当前目录项
            let entry_result = self.read_current_entry(inode_ref)?;

            if let Some((entry, rec_len)) = entry_result {
                // 移动到下一个目录项
                self.offset_in_block += rec_len as usize;
                self.curr_off += rec_len as u64;

                // 跳过已删除的目录项（inode == 0）
                if entry.inode == 0 {
                    continue;
                }

                return Ok(Some(entry));
            } else {
                // rec_len 为 0，表示目录结束
                return Ok(None);
            }
        }
    }

    /// 读取当前位置的目录项
    ///
    /// 对应 lwext4 的 `ext4_dir_iterator_set()` 和目录项读取逻辑
    ///
    /// # 返回
    ///
    /// - `Ok(Some((DirEntry, rec_len)))` - 成功读取目录项及其长度
    /// - `Ok(None)` - 遇到 rec_len == 0（目录结束）
    /// - `Err(_)` - 格式错误或 I/O 错误
    fn read_current_entry<D: BlockDevice>(
        &self,
        inode_ref: &mut InodeRef<D>,
    ) -> Result<Option<(DirEntry, u16)>> {
        let block_size = inode_ref.sb().block_size() as usize;

        // 检查 4 字节对齐（lwext4 的检查）
        if self.offset_in_block % 4 != 0 {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Directory entry not 4-byte aligned",
            ));
        }

        // 检查是否还有足够空间容纳目录项头部（8 字节）
        if self.offset_in_block + EXT4_DIR_ENTRY_MIN_LEN > block_size {
            return Err(Error::new(
                ErrorKind::Corrupted,
                "Directory entry header extends beyond block",
            ));
        }

        // 获取当前块的物理地址
        let physical_block = inode_ref.get_inode_dblk_idx(self.current_block_idx, false)?;

        // 通过 Block handle 读取块
        let bdev = inode_ref.bdev();
        let mut block = Block::get(bdev, physical_block)?;

        block.with_data(|data| {
            // 读取目录项头部
            let entry_header: ext4_dir_entry = crate::bytes::read_struct(&data[self.offset_in_block..])?;

            let rec_len = u16::from_le(entry_header.rec_len);

            // rec_len 为 0 表示目录结束
            if rec_len == 0 {
                return Ok(None);
            }

            // 检查 rec_len 是否越界
            if self.offset_in_block + rec_len as usize > block_size {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Directory entry rec_len extends beyond block",
                ));
            }

            let name_len = entry_header.name_len as usize;

            // 检查 name_len 是否合法（lwext4 的检查）
            if name_len > rec_len as usize - 8 {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Directory entry name_len too large",
                ));
            }

            // 如果 inode 为 0，返回空项（已删除）
            let inode = u32::from_le(entry_header.inode);
            if inode == 0 {
                return Ok(Some((
                    DirEntry {
                        inode: 0,
                        name: String::new(),
                        file_type: entry_header.file_type,
                    },
                    rec_len,
                )));
            }

            // 读取文件名（紧跟在固定 8 字节头部之后）
            if name_len == 0 || name_len > EXT4_NAME_MAX {
                return Ok(Some((
                    DirEntry {
                        inode,
                        name: String::new(),
                        file_type: entry_header.file_type,
                    },
                    rec_len,
                )));
            }

            let name_start = self.offset_in_block + 8;
            let name_end = name_start + name_len;

            if name_end > block_size {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Directory entry name extends beyond block",
                ));
            }

            let name_bytes = &data[name_start..name_end];
            let name = String::from_utf8_lossy(name_bytes).into_owned();

            Ok(Some((
                DirEntry {
                    inode,
                    name,
                    file_type: entry_header.file_type,
                },
                rec_len,
            )))
        })?
    }
}

/// 目录项
///
/// 表示一个目录中的条目
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Inode 编号
    pub inode: u32,
    /// 文件名
    pub name: String,
    /// 文件类型
    pub file_type: u8,
}

impl DirEntry {
    /// 检查是否是目录
    pub fn is_dir(&self) -> bool {
        self.file_type == EXT4_DE_DIR
    }

    /// 检查是否是普通文件
    pub fn is_file(&self) -> bool {
        self.file_type == EXT4_DE_REG_FILE
    }

    /// 检查是否是符号链接
    pub fn is_symlink(&self) -> bool {
        self.file_type == EXT4_DE_SYMLINK
    }
}

/// 便捷函数：读取目录中的所有条目
///
/// # 参数
///
/// * `inode_ref` - 目录的 inode 引用
///
/// # 返回
///
/// 目录项列表
pub fn read_dir<D: BlockDevice>(
    inode_ref: &mut InodeRef<D>,
) -> Result<alloc::vec::Vec<DirEntry>> {
    let mut entries = alloc::vec::Vec::new();
    let mut iter = DirIterator::new(inode_ref, 0)?;

    while let Some(entry) = iter.next(inode_ref)? {
        entries.push(entry);
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_entry_type_checks() {
        let mut entry = DirEntry {
            inode: 2,
            name: "test".into(),
            file_type: EXT4_DE_DIR,
        };

        assert!(entry.is_dir());
        assert!(!entry.is_file());
        assert!(!entry.is_symlink());

        entry.file_type = EXT4_DE_REG_FILE;
        assert!(!entry.is_dir());
        assert!(entry.is_file());
        assert!(!entry.is_symlink());

        entry.file_type = EXT4_DE_SYMLINK;
        assert!(!entry.is_dir());
        assert!(!entry.is_file());
        assert!(entry.is_symlink());
    }
}
