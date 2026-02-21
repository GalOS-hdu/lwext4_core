//! 目录项解析

use crate::{
    block::BlockDev,
    block::BlockDevice,
    consts::*,
    error::{Error, ErrorKind, Result},
    extent::ExtentTree,
    inode::Inode,
    superblock::Superblock,
    types::ext4_dir_entry,
};
use alloc::{string::String, vec::Vec};

/// 目录项包装器
///
/// 提供对 ext4_dir_entry 的高级访问
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

/// 目录迭代器
///
/// 用于遍历目录中的所有条目
pub struct DirIterator<'a, D: BlockDevice> {
    extent_tree: ExtentTree<'a, D>,
    inode: &'a Inode,
    sb: &'a Superblock,
    current_block: u32,
    block_data: Vec<u8>,
    offset_in_block: usize,
    total_size: u64,
    bytes_read: u64,
}

impl<'a, D: BlockDevice> DirIterator<'a, D> {
    /// 创建新的目录迭代器
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `sb` - superblock 引用
    /// * `inode` - 目录 inode
    pub fn new(
        bdev: &'a mut BlockDev<D>,
        sb: &'a Superblock,
        inode: &'a Inode,
    ) -> Result<Self> {
        if !inode.is_dir() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Inode is not a directory",
            ));
        }

        let block_size = sb.block_size() as usize;
        let extent_tree = ExtentTree::new(bdev, sb.block_size());

        Ok(Self {
            extent_tree,
            inode,
            sb,
            current_block: 0,
            block_data: alloc::vec![0u8; block_size],
            offset_in_block: 0,
            total_size: inode.file_size(),
            bytes_read: 0,
        })
    }

    /// 读取下一个块
    fn load_next_block(&mut self) -> Result<bool> {
        let block_size = self.sb.block_size() as u64;

        // 检查是否已经读取完所有数据
        if self.bytes_read >= self.total_size {
            return Ok(false);
        }

        // 读取下一个块
        self.extent_tree
            .read_block(self.inode.inner(), self.current_block, &mut self.block_data)?;

        self.current_block += 1;
        self.offset_in_block = 0;
        self.bytes_read += block_size;

        Ok(true)
    }

    /// 获取下一个目录项
    pub fn next_entry(&mut self) -> Result<Option<DirEntry>> {
        loop {
            let block_size = self.sb.block_size() as usize;

            // 如果当前块已读完，加载下一个块
            if self.offset_in_block >= block_size
                && !self.load_next_block()? {
                    return Ok(None);
                }

            // 如果是第一次，加载第一个块
            if self.current_block == 0 && self.offset_in_block == 0
                && !self.load_next_block()? {
                    return Ok(None);
                }

            // 检查是否还有足够的数据读取目录项头部
            if self.offset_in_block + EXT4_DIR_ENTRY_MIN_LEN > block_size {
                // 跳到下一个块
                self.offset_in_block = block_size;
                continue;
            }

            // 读取目录项
            let entry: ext4_dir_entry = crate::bytes::read_struct(&self.block_data[self.offset_in_block..])?;

            let inode = u32::from_le(entry.inode);
            let rec_len = u16::from_le(entry.rec_len) as usize;
            let name_len = entry.name_len as usize;

            // rec_len 为 0 表示目录结束
            if rec_len == 0 {
                return Ok(None);
            }

            // 移动到下一个目录项
            self.offset_in_block += rec_len;

            // inode 为 0 表示已删除的目录项，跳过
            if inode == 0 {
                continue;
            }

            // 检查名称长度是否有效
            if name_len == 0 || name_len > EXT4_NAME_MAX {
                continue;
            }

            // 读取文件名
            // name 字段紧跟在 ext4_dir_entry 的固定部分之后（8 字节）
            let name_offset = 8; // sizeof(inode) + sizeof(rec_len) + sizeof(name_len) + sizeof(file_type)
            let name_start = self.offset_in_block - rec_len + name_offset;
            let name_end = name_start + name_len;

            if name_end > block_size {
                return Err(Error::new(
                    ErrorKind::Corrupted,
                    "Directory entry name extends beyond block",
                ));
            }

            let name_bytes = &self.block_data[name_start..name_end];
            let name = String::from_utf8_lossy(name_bytes).into_owned();

            return Ok(Some(DirEntry {
                inode,
                name,
                file_type: entry.file_type,
            }));
        }
    }
}

/// 读取目录中的所有条目
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 引用
/// * `inode` - 目录 inode
///
/// # 返回
///
/// 目录项列表
pub fn read_dir<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &Superblock,
    inode: &Inode,
) -> Result<Vec<DirEntry>> {
    let mut entries = Vec::new();
    let mut iter = DirIterator::new(bdev, sb, inode)?;

    while let Some(entry) = iter.next_entry()? {
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
