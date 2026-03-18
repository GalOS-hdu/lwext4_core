//! VFS 风格的基于 inode 的操作
//!
//! 提供 with_inode_ref, read_at_inode, write_at_inode, write_at_inode_batch,
//! get_inode_attr, lookup_in_dir, create_in_dir, read_dir_from_inode,
//! unlink_from_dir, rename_inode, link_inode, drop_inode 等操作。

use crate::{
    block::BlockDevice,
    dir::{read_dir, DirEntry},
    error::{Error, ErrorKind, Result},
};
use alloc::vec::Vec;

use super::{inode_ref::InodeRef, metadata::FileMetadata, Ext4FileSystem};

impl<D: BlockDevice> Ext4FileSystem<D> {
    /// 使用闭包访问 InodeRef
    ///
    /// 提供灵活的 inode 访问方式，自动管理 InodeRef 的生命周期和写回
    pub fn with_inode_ref<F, R>(&mut self, inode_num: u32, f: F) -> Result<R>
    where
        F: FnOnce(&mut InodeRef<D>) -> Result<R>,
    {
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        f(&mut inode_ref)
    }

    /// 从指定 inode 的指定偏移量读取数据
    ///
    /// # 参数
    ///
    /// * `inode_num` - inode 编号
    /// * `buf` - 目标缓冲区
    /// * `offset` - 读取起始偏移量（字节）
    ///
    /// # 返回
    ///
    /// 实际读取的字节数
    pub fn read_at_inode(&mut self, inode_num: u32, buf: &mut [u8], offset: u64) -> Result<usize> {
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 检查 EOF
        let file_size = inode_ref.size()?;
        if offset >= file_size {
            return Ok(0);
        }

        inode_ref.read_extent_file(offset, buf)
    }

    /// 向指定 inode 的指定偏移量写入数据
    ///
    /// 此方法一次最多写入一个块内的数据
    pub fn write_at_inode(&mut self, inode_num: u32, buf: &[u8], offset: u64) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let block_size = self.sb.block_size() as u64;
        let logical_block = (offset / block_size) as u32;
        let offset_in_block = (offset % block_size) as usize;

        let remaining_in_block = block_size as usize - offset_in_block;
        let write_len = buf.len().min(remaining_in_block);

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        let current_size = inode_ref.size()?;

        // 获取或分配物理块
        let physical_block = inode_ref.get_inode_dblk_idx(logical_block, true)?;

        if physical_block == 0 {
            return Err(Error::new(
                ErrorKind::NoSpace,
                "Failed to allocate block for write",
            ));
        }

        let bdev = inode_ref.bdev_mut();

        let mut block_buf = alloc::vec![0u8; block_size as usize];
        let is_full_block_write = offset_in_block == 0 && write_len == block_size as usize;

        if !is_full_block_write {
            bdev.read_block(physical_block, &mut block_buf)?;
        }

        block_buf[offset_in_block..offset_in_block + write_len]
            .copy_from_slice(&buf[..write_len]);

        bdev.write_block(physical_block, &block_buf)?;

        // 更新文件大小
        let new_end = offset + write_len as u64;
        if new_end > current_size {
            inode_ref.set_size(new_end)?;
            inode_ref.mark_dirty()?;
        }

        Ok(write_len)
    }

    /// 批量写入数据到指定 inode（性能优化版本）
    ///
    /// 与 write_at_inode 不同，此方法可以一次写入多个块，
    /// 避免重复获取 InodeRef，显著提升大文件写入性能。
    pub fn write_at_inode_batch(&mut self, inode_num: u32, buf: &[u8], offset: u64) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let block_size = self.sb.block_size() as u64;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        let current_size = inode_ref.size()?;

        let mut bytes_written = 0;
        let mut current_offset = offset;

        let mut block_buf = alloc::vec![0u8; block_size as usize];

        while bytes_written < buf.len() {
            let logical_block = (current_offset / block_size) as u32;
            let offset_in_block = (current_offset % block_size) as usize;
            let remaining_in_block = block_size as usize - offset_in_block;
            let write_len = (buf.len() - bytes_written).min(remaining_in_block);

            let physical_block = inode_ref.get_inode_dblk_idx(logical_block, true)?;
            if physical_block == 0 {
                return Err(Error::new(ErrorKind::NoSpace, "Failed to allocate block"));
            }

            let bdev = inode_ref.bdev_mut();

            let is_full_block = offset_in_block == 0 && write_len == block_size as usize;

            if !is_full_block {
                bdev.read_block(physical_block, &mut block_buf)?;
            }

            block_buf[offset_in_block..offset_in_block + write_len]
                .copy_from_slice(&buf[bytes_written..bytes_written + write_len]);

            bdev.write_block(physical_block, &block_buf)?;

            bytes_written += write_len;
            current_offset += write_len as u64;
        }

        // 更新文件大小
        let new_end = offset + bytes_written as u64;
        if new_end > current_size {
            inode_ref.set_size(new_end)?;
            inode_ref.mark_dirty()?;
        }

        Ok(bytes_written)
    }

    /// 获取 inode 的属性（元数据）
    pub fn get_inode_attr(&mut self, inode_num: u32) -> Result<FileMetadata> {
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        let mode = inode_ref.with_inode(|inode| u16::from_le(inode.mode))?;
        let size = inode_ref.size()?;
        let links_count = inode_ref.with_inode(|inode| u16::from_le(inode.links_count))?;

        let (uid, gid) = inode_ref.with_inode(|inode| {
            let uid = (u16::from_le(inode.uid) as u32) | ((u16::from_le(inode.uid_high) as u32) << 16);
            let gid = (u16::from_le(inode.gid) as u32) | ((u16::from_le(inode.gid_high) as u32) << 16);
            (uid, gid)
        })?;

        let (atime, mtime, ctime) = inode_ref.with_inode(|inode| {
            (
                u32::from_le(inode.atime) as i64,
                u32::from_le(inode.mtime) as i64,
                u32::from_le(inode.ctime) as i64,
            )
        })?;

        use crate::consts::*;
        let file_type = match mode & EXT4_INODE_MODE_TYPE_MASK {
            EXT4_INODE_MODE_FILE => super::metadata::FileType::RegularFile,
            EXT4_INODE_MODE_DIRECTORY => super::metadata::FileType::Directory,
            EXT4_INODE_MODE_SOFTLINK => super::metadata::FileType::Symlink,
            _ => super::metadata::FileType::Unknown,
        };

        let blocks_count = inode_ref.blocks_count()?;

        Ok(FileMetadata {
            inode_num,
            file_type,
            size,
            permissions: mode & 0o7777,
            links_count,
            uid,
            gid,
            atime,
            mtime,
            ctime,
            blocks_count,
        })
    }

    /// 在指定目录 inode 中查找子项
    ///
    /// # 返回
    ///
    /// 找到的子项的 inode 编号
    pub fn lookup_in_dir(&mut self, parent_inode: u32, name: &str) -> Result<u32> {
        let entries = {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, parent_inode)?;
            if !inode_ref.is_dir()? {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Parent inode is not a directory",
                ));
            }
            read_dir(&mut inode_ref)?
        };

        for entry in entries {
            if entry.name == name {
                return Ok(entry.inode);
            }
        }

        Err(Error::new(
            ErrorKind::NotFound,
            "Entry not found in directory",
        ))
    }

    /// 在指定目录 inode 中创建新条目
    ///
    /// 此方法会：
    /// 1. 分配新 inode
    /// 2. 初始化 inode（设置类型、权限、时间戳）
    /// 3. 在父目录中添加目录条目
    /// 4. 如果是目录，初始化 "." 和 ".." 条目
    pub fn create_in_dir(
        &mut self,
        parent_inode: u32,
        name: &str,
        file_type: u8,
        mode: u16,
    ) -> Result<u32> {
        use crate::consts::*;
        use crate::dir::write::{EXT4_DE_DIR, EXT4_DE_REG_FILE, EXT4_DE_SYMLINK};

        // 验证父 inode 是目录
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, parent_inode)?;
            if !inode_ref.is_dir()? {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Parent inode is not a directory",
                ));
            }
        }

        // 检查名称是否已存在
        if self.lookup_in_dir(parent_inode, name).is_ok() {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "Entry already exists",
            ));
        }

        let is_dir = file_type == EXT4_DE_DIR;

        // 分配新 inode
        let new_inode = self.alloc_inode(is_dir)?;

        // 初始化 inode
        {
            use crate::extent::tree_init;

            let inode_mode = match file_type {
                EXT4_DE_REG_FILE => EXT4_INODE_MODE_FILE,
                EXT4_DE_DIR => EXT4_INODE_MODE_DIRECTORY,
                EXT4_DE_SYMLINK => EXT4_INODE_MODE_SOFTLINK,
                _ => EXT4_INODE_MODE_FILE,
            };

            // 读取 superblock 的 extra_isize 配置
            let inode_size = self.sb.inode_size();
            let extra_isize = if inode_size > EXT4_GOOD_OLD_INODE_SIZE as u16 {
                let want_extra_isize = u16::from_le(self.sb.inner().want_extra_isize);
                if want_extra_isize > 0 {
                    want_extra_isize
                } else {
                    32u16
                }
            } else {
                0u16
            };

            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, new_inode)?;

            inode_ref.with_inode_mut(|inode| {
                inode.mode = (inode_mode | mode).to_le();
                inode.links_count = 1u16.to_le();

                let now = 0u32; // TODO: 获取当前时间
                inode.atime = now.to_le();
                inode.mtime = now.to_le();
                inode.ctime = now.to_le();

                if extra_isize > 0 {
                    inode.extra_isize = extra_isize.to_le();
                }
            })?;

            // 设置 EXTENTS 标志
            inode_ref.with_inode_mut(|inode| {
                let flags = u32::from_le(inode.flags);
                inode.flags = (flags | EXT4_INODE_FLAG_EXTENTS).to_le();
            })?;

            inode_ref.set_size(0)?;

            // 初始化 extent 树
            tree_init(&mut inode_ref)?;

            inode_ref.mark_dirty()?;

            // 如果是目录，初始化目录结构
            if is_dir {
                crate::dir::write_init::dir_init(&mut inode_ref, parent_inode)?;
            }
        }

        // 在父目录中添加条目
        self.add_dir_entry(parent_inode, name, new_inode, file_type)?;

        Ok(new_inode)
    }

    /// 读取指定目录 inode 的所有条目
    pub fn read_dir_from_inode(&mut self, dir_inode: u32) -> Result<Vec<DirEntry>> {
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dir_inode)?;
        if !inode_ref.is_dir()? {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Inode is not a directory",
            ));
        }

        read_dir(&mut inode_ref)
    }

    /// 从指定目录 inode 中删除条目
    ///
    /// 此方法只删除目录条目，不会减少链接计数或释放 inode
    ///
    /// # 返回
    ///
    /// 被删除条目的 inode 编号
    pub fn unlink_from_dir(&mut self, parent_inode: u32, name: &str) -> Result<u32> {
        // 验证父 inode 是目录
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, parent_inode)?;
            if !inode_ref.is_dir()? {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Parent inode is not a directory",
                ));
            }
        }

        // 查找要删除的条目
        let target_inode = self.lookup_in_dir(parent_inode, name)?;

        // 删除目录条目
        self.remove_dir_entry(parent_inode, name)?;

        Ok(target_inode)
    }

    /// 基于 inode 的重命名操作 (VFS 风格)
    pub fn rename_inode(
        &mut self,
        src_dir_ino: u32,
        src_name: &str,
        dst_dir_ino: u32,
        dst_name: &str,
    ) -> Result<()> {
        use crate::dir::write::{EXT4_DE_DIR, EXT4_DE_REG_FILE};

        // 1. 查找目标 inode
        let target_inode = self.lookup_in_dir(src_dir_ino, src_name)?;

        // 2. 获取目标的文件类型
        let (is_dir, file_type) = {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, target_inode)?;
            let is_dir = inode_ref.is_dir()?;
            let file_type = if is_dir {
                EXT4_DE_DIR
            } else {
                EXT4_DE_REG_FILE
            };
            (is_dir, file_type)
        };

        // 3. 如果目标名字已存在，先完整删除（POSIX 语义）
        match self.lookup_in_dir(dst_dir_ino, dst_name) {
            Ok(old_target_inode) => {
                self.remove_dir_entry(dst_dir_ino, dst_name)?;

                let (old_is_dir, _new_links) = {
                    let mut old_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, old_target_inode)?;
                    let old_is_dir = old_inode_ref.is_dir()?;

                    let current_links = old_inode_ref.with_inode(|inode| {
                        u16::from_le(inode.links_count)
                    })?;

                    let new_links = current_links.saturating_sub(1);
                    old_inode_ref.with_inode_mut(|inode| {
                        inode.links_count = new_links.to_le();
                    })?;
                    old_inode_ref.mark_dirty()?;

                    if new_links == 0 {
                        log::info!(
                            "[RENAME] inode {old_target_inode} i_nlink=0, marked for deferred deletion"
                        );
                    }

                    (old_is_dir, new_links)
                };

                if old_is_dir {
                    let mut dst_parent_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dst_dir_ino)?;
                    dst_parent_ref.with_inode_mut(|inode| {
                        let links = u16::from_le(inode.links_count);
                        inode.links_count = links.saturating_sub(1).to_le();
                    })?;
                    dst_parent_ref.mark_dirty()?;
                }
            }
            Err(_) => {
                // 目标不存在，正常情况
            }
        }

        // 4. 在目标目录添加条目
        self.add_dir_entry(dst_dir_ino, dst_name, target_inode, file_type)?;

        // 5. 如果是目录且移动到新父目录，增加新父目录的链接计数
        if is_dir && src_dir_ino != dst_dir_ino {
            let mut dst_parent_inode_ref =
                InodeRef::get(&mut self.bdev, &mut self.sb, dst_dir_ino)?;

            dst_parent_inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links + 1).to_le();
            })?;
            dst_parent_inode_ref.mark_dirty()?;
        }

        // 6. 从源目录删除条目
        self.remove_dir_entry(src_dir_ino, src_name)?;

        // 7. 如果是目录且移动到新父目录，减少旧父目录的链接计数
        if is_dir && src_dir_ino != dst_dir_ino {
            let mut src_parent_inode_ref =
                InodeRef::get(&mut self.bdev, &mut self.sb, src_dir_ino)?;

            src_parent_inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links.saturating_sub(1)).to_le();
            })?;
            src_parent_inode_ref.mark_dirty()?;
        }

        // 8. 如果是目录且移动到新父目录，更新 ".." 条目
        if is_dir && src_dir_ino != dst_dir_ino {
            self.remove_dir_entry(target_inode, "..")?;
            self.add_dir_entry(target_inode, "..", dst_dir_ino, EXT4_DE_DIR)?;
        }

        Ok(())
    }

    /// 创建硬链接 (VFS 风格)
    ///
    /// 在指定目录中创建指向已存在 inode 的新目录条目
    pub fn link_inode(
        &mut self,
        dir_ino: u32,
        name: &str,
        child_ino: u32,
    ) -> Result<()> {
        use crate::dir::write::EXT4_DE_REG_FILE;

        // 1. 验证 dir_ino 是目录
        {
            let mut dir_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dir_ino)?;
            if !dir_inode_ref.is_dir()? {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "dir_ino is not a directory",
                ));
            }
        }

        // 2. 验证 child_ino 不是目录
        let file_type = {
            let mut child_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, child_ino)?;
            let is_dir = child_inode_ref.is_dir()?;

            if is_dir {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Cannot create hard link to directory",
                ));
            }

            child_inode_ref.with_inode(|inode| {
                let mode = u16::from_le(inode.mode);
                let type_bits = mode & crate::consts::EXT4_INODE_MODE_TYPE_MASK;

                match type_bits {
                    crate::consts::EXT4_INODE_MODE_FILE => crate::dir::write::EXT4_DE_REG_FILE,
                    crate::consts::EXT4_INODE_MODE_SOFTLINK => crate::dir::write::EXT4_DE_SYMLINK,
                    crate::consts::EXT4_INODE_MODE_CHARDEV => crate::dir::write::EXT4_DE_CHRDEV,
                    crate::consts::EXT4_INODE_MODE_BLOCKDEV => crate::dir::write::EXT4_DE_BLKDEV,
                    crate::consts::EXT4_INODE_MODE_FIFO => crate::dir::write::EXT4_DE_FIFO,
                    crate::consts::EXT4_INODE_MODE_SOCKET => crate::dir::write::EXT4_DE_SOCK,
                    _ => EXT4_DE_REG_FILE,
                }
            })?
        };

        // 3. 在目录中添加条目
        self.add_dir_entry(dir_ino, name, child_ino, file_type)?;

        // 4. 增加 child_ino 的链接计数
        {
            let mut child_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, child_ino)?;
            child_inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links + 1).to_le();
            })?;
            child_inode_ref.mark_dirty()?;
        }

        Ok(())
    }

    /// Deferred deletion: 当VFS层释放最后一个对inode的引用时调用
    /// 如果 i_nlink == 0，则释放inode的所有资源
    pub fn drop_inode(&mut self, ino: u32) -> Result<()> {
        let (nlink, is_dir) = {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, ino)?;
            let nlink = inode_ref.with_inode(|inode| {
                u16::from_le(inode.links_count)
            })?;
            let is_dir = inode_ref.is_dir()?;
            (nlink, is_dir)
        };

        if nlink == 0 {
            log::info!("[DROP_INODE] inode {ino} has nlink=0, freeing resources");

            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, ino)?;
            inode_ref.set_size(0)?;
            drop(inode_ref);

            self.free_inode(ino, is_dir)?;
        } else {
            log::debug!("[DROP_INODE] inode {ino} still has nlink={nlink}, not freeing");
        }

        Ok(())
    }
}
