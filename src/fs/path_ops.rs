//! 基于路径的文件系统操作
//!
//! 包含 open, read_dir, metadata, exists, is_dir, is_file,
//! 元数据修改 (set_mode/owner/atime/mtime/ctime),
//! xattr 操作, 以及 create_file, create_dir, flink, fsymlink,
//! readlink, remove_file, remove_dir, rename 等操作。

use crate::{
    block::BlockDevice,
    dir::{lookup_path, read_dir, DirEntry},
    error::{Error, ErrorKind, Result},
    inode::Inode,
};
use alloc::vec::Vec;

use super::{file::File, inode_ref::InodeRef, metadata::FileMetadata, Ext4FileSystem};

impl<D: BlockDevice> Ext4FileSystem<D> {
    /// 打开文件
    ///
    /// # 参数
    ///
    /// * `path` - 文件路径（绝对路径）
    ///
    /// # 返回
    ///
    /// 成功返回文件句柄
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let mut file = fs.open("/etc/passwd")?;
    /// let mut buf = vec![0u8; 1024];
    /// let n = file.read(&mut buf)?;
    /// ```
    pub fn open(&mut self, path: &str) -> Result<File<D>> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        // 检查是否是普通文件
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        if !inode_ref.is_file()? {
            return Err(Error::new(ErrorKind::InvalidInput, "Not a regular file"));
        }
        drop(inode_ref); // 明确释放

        File::new(&mut self.bdev, &self.sb, inode_num)
    }

    /// 读取目录内容
    ///
    /// # 参数
    ///
    /// * `path` - 目录路径（绝对路径）
    ///
    /// # 返回
    ///
    /// 目录项列表
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let entries = fs.read_dir("/bin")?;
    /// for entry in entries {
    ///     println!("{} (inode: {})", entry.name, entry.inode);
    /// }
    /// ```
    pub fn read_dir(&mut self, path: &str) -> Result<Vec<DirEntry>> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        if !inode_ref.is_dir()? {
            return Err(Error::new(ErrorKind::InvalidInput, "Not a directory"));
        }

        read_dir(&mut inode_ref)
    }

    /// 获取文件元数据
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    ///
    /// # 返回
    ///
    /// 文件元数据
    pub fn metadata(&mut self, path: &str) -> Result<FileMetadata> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let inode = Inode::load(&mut self.bdev, &self.sb, inode_num)?;

        Ok(FileMetadata::from_inode(&inode, inode_num))
    }

    /// 检查路径是否存在
    pub fn exists(&mut self, path: &str) -> bool {
        lookup_path(&mut self.bdev, &mut self.sb, path).is_ok()
    }

    /// 检查路径是否是目录
    pub fn is_dir(&mut self, path: &str) -> Result<bool> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        inode_ref.is_dir()
    }

    /// 检查路径是否是普通文件
    pub fn is_file(&mut self, path: &str) -> Result<bool> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        inode_ref.is_file()
    }

    // ========== Metadata Write Operations ==========

    /// 修改文件/目录权限
    pub fn set_mode(&mut self, path: &str, mode: u16) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_mode(mode)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    /// 修改文件/目录所有者
    pub fn set_owner(&mut self, path: &str, uid: u32, gid: u32) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_owner(uid, gid)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    /// 修改访问时间
    pub fn set_atime(&mut self, path: &str, atime: u32) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_atime(atime)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    /// 修改修改时间
    pub fn set_mtime(&mut self, path: &str, mtime: u32) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_mtime(mtime)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    /// 修改变更时间
    pub fn set_ctime(&mut self, path: &str, ctime: u32) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_ctime(ctime)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    // ========== Extended Attributes (xattr) API ==========

    /// 列出文件/目录的所有扩展属性
    pub fn listxattr(&mut self, path: &str) -> Result<Vec<alloc::string::String>> {
        use crate::xattr;
        use alloc::string::String;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        let mut buffer = alloc::vec![0u8; 4096];
        let len = xattr::list(&mut inode_ref, &mut buffer)?;

        let mut result = Vec::new();
        let mut start = 0;
        for i in 0..len {
            if buffer[i] == 0 {
                if i > start {
                    let name = String::from_utf8_lossy(&buffer[start..i]).into_owned();
                    result.push(name);
                }
                start = i + 1;
            }
        }

        Ok(result)
    }

    /// 获取扩展属性的值
    pub fn getxattr(&mut self, path: &str, name: &str) -> Result<Vec<u8>> {
        use crate::xattr;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        let mut buffer = alloc::vec![0u8; 65536];
        let len = xattr::get(&mut inode_ref, name, &mut buffer)?;

        buffer.truncate(len);
        Ok(buffer)
    }

    /// 设置扩展属性
    pub fn setxattr(&mut self, path: &str, name: &str, value: &[u8]) -> Result<()> {
        use crate::xattr;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        xattr::set(&mut inode_ref, name, value)?;

        Ok(())
    }

    /// 删除扩展属性
    pub fn removexattr(&mut self, path: &str, name: &str) -> Result<()> {
        use crate::xattr;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        xattr::remove(&mut inode_ref, name)?;

        Ok(())
    }

    // ========== 高级文件操作 API ==========

    /// 创建新文件
    ///
    /// # 参数
    ///
    /// * `parent_path` - 父目录路径
    /// * `name` - 文件名
    /// * `mode` - 文件权限（Unix 权限位，如 0o644）
    ///
    /// # 返回
    ///
    /// 新文件的 inode 编号
    pub fn create_file(&mut self, parent_path: &str, name: &str, mode: u16) -> Result<u32> {
        use crate::{consts::*, dir::write::EXT4_DE_REG_FILE, extent::tree_init};

        // 1. 分配新 inode
        let inode_num = self.alloc_inode(false)?;

        // 2. 初始化 inode
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

            // 设置文件模式（类型 + 权限）
            let file_mode = EXT4_INODE_MODE_FILE as u32 | mode as u32;
            inode_ref.with_inode_mut(|inode| {
                inode.mode = (file_mode as u16).to_le();
            })?;

            // 设置初始大小为 0
            inode_ref.set_size(0)?;

            // 设置链接计数为 1
            inode_ref.with_inode_mut(|inode| {
                inode.links_count = 1u16.to_le();
            })?;

            // 设置时间戳
            let now = 0u32; // TODO: 获取当前时间
            inode_ref.with_inode_mut(|inode| {
                inode.atime = now.to_le();
                inode.ctime = now.to_le();
                inode.mtime = now.to_le();
            })?;

            // 设置 EXTENTS 标志
            inode_ref.with_inode_mut(|inode| {
                let flags = u32::from_le(inode.flags);
                inode.flags = (flags | EXT4_INODE_FLAG_EXTENTS).to_le();
            })?;

            // 初始化 extent 树
            tree_init(&mut inode_ref)?;

            inode_ref.mark_dirty()?;
        }

        // 3. 查找父目录并添加条目
        let parent_inode = lookup_path(&mut self.bdev, &mut self.sb, parent_path)?;

        // 4. 添加到父目录
        self.add_dir_entry(parent_inode, name, inode_num, EXT4_DE_REG_FILE)?;

        Ok(inode_num)
    }

    /// 创建新目录
    ///
    /// # 参数
    ///
    /// * `parent_path` - 父目录路径
    /// * `name` - 目录名
    /// * `mode` - 目录权限（Unix 权限位，如 0o755）
    ///
    /// # 返回
    ///
    /// 新目录的 inode 编号
    pub fn create_dir(&mut self, parent_path: &str, name: &str, mode: u16) -> Result<u32> {
        use crate::{consts::*, dir::write::EXT4_DE_DIR, extent::tree_init};

        // 1. 分配新 inode
        let inode_num = self.alloc_inode(true)?;

        // 2. 查找父目录 inode
        let parent_inode = lookup_path(&mut self.bdev, &mut self.sb, parent_path)?;

        // 3. 初始化目录 inode
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

            // 设置目录模式（类型 + 权限）
            let dir_mode = EXT4_INODE_MODE_DIRECTORY as u32 | mode as u32;
            inode_ref.with_inode_mut(|inode| {
                inode.mode = (dir_mode as u16).to_le();
            })?;

            // 设置初始大小为 0
            inode_ref.set_size(0)?;

            // 设置链接计数为 2（自己 + "." 条目）
            inode_ref.with_inode_mut(|inode| {
                inode.links_count = 2u16.to_le();
            })?;

            // 设置时间戳
            let now = 0u32; // TODO: 获取当前时间
            inode_ref.with_inode_mut(|inode| {
                inode.atime = now.to_le();
                inode.ctime = now.to_le();
                inode.mtime = now.to_le();
            })?;

            // 设置 EXTENTS 标志
            inode_ref.with_inode_mut(|inode| {
                let flags = u32::from_le(inode.flags);
                inode.flags = (flags | EXT4_INODE_FLAG_EXTENTS).to_le();
            })?;

            // 初始化 extent 树
            tree_init(&mut inode_ref)?;

            inode_ref.mark_dirty()?;
        }

        // 4. 添加 "." 和 ".." 条目到新目录
        self.add_dir_entry(inode_num, ".", inode_num, EXT4_DE_DIR)?;
        self.add_dir_entry(inode_num, "..", parent_inode, EXT4_DE_DIR)?;

        // 5. 添加到父目录
        self.add_dir_entry(parent_inode, name, inode_num, EXT4_DE_DIR)?;

        // 6. 增加父目录的链接计数
        {
            let mut parent_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, parent_inode)?;

            parent_inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links + 1).to_le();
            })?;

            parent_inode_ref.mark_dirty()?;
        }

        Ok(inode_num)
    }

    /// 创建硬链接
    ///
    /// 为现有文件创建一个新的硬链接。
    ///
    /// # 参数
    ///
    /// * `src_path` - 源文件的完整路径
    /// * `dst_dir` - 目标目录路径
    /// * `dst_name` - 新链接的名称
    pub fn flink(&mut self, src_path: &str, dst_dir: &str, dst_name: &str) -> Result<()> {
        use crate::dir::write::EXT4_DE_REG_FILE;

        // 1. 查找源文件 inode
        let src_inode = lookup_path(&mut self.bdev, &mut self.sb, src_path)?;

        // 2. 验证源是普通文件
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, src_inode)?;
            if !inode_ref.is_file()? {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Cannot create hard link to non-regular file",
                ));
            }
        }

        // 3. 查找目标目录 inode
        let dst_dir_inode = lookup_path(&mut self.bdev, &mut self.sb, dst_dir)?;

        // 4. 增加源文件的链接计数
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, src_inode)?;
            inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links + 1).to_le();
            })?;
            inode_ref.mark_dirty()?;
        }

        // 5. 在目标目录添加新的目录项
        self.add_dir_entry(dst_dir_inode, dst_name, src_inode, EXT4_DE_REG_FILE)?;

        Ok(())
    }

    /// 创建符号链接
    ///
    /// # 参数
    ///
    /// * `target` - 符号链接指向的目标路径
    /// * `link_dir` - 符号链接所在目录的路径
    /// * `link_name` - 符号链接的名称
    ///
    /// # 返回
    ///
    /// 新创建的符号链接的 inode 编号
    pub fn fsymlink(&mut self, target: &str, link_dir: &str, link_name: &str) -> Result<u32> {
        use crate::{consts::*, dir::write::EXT4_DE_SYMLINK, extent::tree_init};

        // 1. 分配新 inode
        let inode_num = self.alloc_inode(false)?;

        // 提取 block_size（避免借用冲突）
        let block_size = self.sb.block_size();

        // 2. 初始化符号链接 inode
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

            // 设置符号链接类型和权限
            let symlink_mode = EXT4_INODE_MODE_SOFTLINK | 0o777;
            inode_ref.with_inode_mut(|inode| {
                inode.mode = symlink_mode.to_le();
                inode.links_count = 1u16.to_le();
            })?;

            // 设置大小为目标路径长度
            inode_ref.set_size(target.len() as u64)?;

            // 设置时间戳
            let now = 0u32; // TODO: 获取当前时间
            inode_ref.with_inode_mut(|inode| {
                inode.atime = now.to_le();
                inode.ctime = now.to_le();
                inode.mtime = now.to_le();
            })?;

            // 存储目标路径
            let target_bytes = target.as_bytes();
            if target.len() < 60 {
                // 快速符号链接：存储在 inode.block 中
                inode_ref.with_inode_mut(|inode| {
                    let block_slice = inode.extent_root_data_mut();
                    block_slice[..target_bytes.len()].copy_from_slice(target_bytes);
                })?;
            } else {
                // 慢速符号链接：需要分配块存储
                inode_ref.with_inode_mut(|inode| {
                    let flags = u32::from_le(inode.flags);
                    inode.flags = (flags | EXT4_INODE_FLAG_EXTENTS).to_le();
                })?;

                // 初始化 extent 树
                tree_init(&mut inode_ref)?;

                // 分配块并写入目标路径
                let block_addr = inode_ref.get_inode_dblk_idx(0, true)?;
                if block_addr == 0 {
                    return Err(Error::new(ErrorKind::NoSpace, "Failed to allocate block for symlink"));
                }

                inode_ref.mark_dirty()?;
                drop(inode_ref);

                // 写入目标路径到块
                let mut block_buf = alloc::vec![0u8; block_size as usize];
                block_buf[..target_bytes.len()].copy_from_slice(target_bytes);
                self.bdev.write_block(block_addr, &block_buf)?;

                let dir_inode = lookup_path(&mut self.bdev, &mut self.sb, link_dir)?;
                self.add_dir_entry(dir_inode, link_name, inode_num, EXT4_DE_SYMLINK)?;
                return Ok(inode_num);
            }

            inode_ref.mark_dirty()?;
        }

        // 3. 在目录中添加符号链接条目
        let dir_inode = lookup_path(&mut self.bdev, &mut self.sb, link_dir)?;
        self.add_dir_entry(dir_inode, link_name, inode_num, EXT4_DE_SYMLINK)?;

        Ok(inode_num)
    }

    /// 读取符号链接的目标路径
    pub fn readlink(&mut self, link_path: &str) -> Result<alloc::string::String> {
        use crate::consts::*;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, link_path)?;

        let block_size = self.sb.block_size();

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 验证是符号链接
        let mode = inode_ref.with_inode(|inode| u16::from_le(inode.mode))?;
        if (mode & EXT4_INODE_MODE_TYPE_MASK) != EXT4_INODE_MODE_SOFTLINK {
            return Err(Error::new(ErrorKind::InvalidInput, "Not a symlink"));
        }

        let size = inode_ref.size()? as usize;
        if size == 0 {
            return Ok(alloc::string::String::new());
        }

        let target_bytes = if size < 60 {
            // 快速符号链接
            inode_ref.with_inode(|inode| {
                let block_slice = &inode.extent_root_data()[..size];
                block_slice.to_vec()
            })?
        } else {
            // 慢速符号链接
            let block_addr = inode_ref.get_inode_dblk_idx(0, false)?;
            if block_addr == 0 {
                return Err(Error::new(ErrorKind::NotFound, "Symlink data block not found"));
            }

            drop(inode_ref);

            let mut block_buf = alloc::vec![0u8; block_size as usize];
            self.bdev.read_block(block_addr, &mut block_buf)?;
            block_buf[..size].to_vec()
        };

        alloc::string::String::from_utf8(target_bytes)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid UTF-8 in symlink target"))
    }

    /// 删除文件
    ///
    /// # 参数
    ///
    /// * `parent_path` - 父目录路径
    /// * `name` - 文件名
    pub fn remove_file(&mut self, parent_path: &str, name: &str) -> Result<()> {
        use crate::consts::{EXT4_INODE_MODE_TYPE_MASK, EXT4_INODE_MODE_SOFTLINK};

        // 1. 查找父目录
        let parent_inode = lookup_path(&mut self.bdev, &mut self.sb, parent_path)?;

        // 2. 构造完整路径查找文件 inode
        let full_path = if parent_path.ends_with('/') {
            alloc::format!("{parent_path}{name}")
        } else {
            alloc::format!("{parent_path}/{name}")
        };
        let file_inode = lookup_path(&mut self.bdev, &mut self.sb, &full_path)?;

        // 3. 检查是否是普通文件或符号链接
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, file_inode)?;
            let is_dir = inode_ref.is_dir()?;
            if is_dir {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Cannot remove directory with remove_file (use remove_dir)",
                ));
            }
        }

        // 4. 从父目录删除条目
        self.remove_dir_entry(parent_inode, name)?;

        // 5. 减少链接计数
        let (should_free, is_fast_symlink) = {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, file_inode)?;
            inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links.saturating_sub(1)).to_le();
            })?;
            inode_ref.mark_dirty()?;

            let links = inode_ref.with_inode(|inode| {
                u16::from_le(inode.links_count)
            })?;

            let mode = inode_ref.with_inode(|inode| u16::from_le(inode.mode))?;
            let size = inode_ref.size()?;
            let is_symlink = (mode & EXT4_INODE_MODE_TYPE_MASK) == EXT4_INODE_MODE_SOFTLINK;
            let is_fast = is_symlink && size < 60;

            (links == 0, is_fast)
        };

        // 6. 如果链接计数为 0，释放 inode 和数据块
        if should_free {
            if !is_fast_symlink {
                self.truncate_file(file_inode, 0)?;
            }

            self.free_inode(file_inode, false)?;
        }

        Ok(())
    }

    /// 删除目录
    ///
    /// 只能删除空目录（只包含 "." 和 ".." 条目）
    ///
    /// # 参数
    ///
    /// * `parent_path` - 父目录路径
    /// * `name` - 目录名
    pub fn remove_dir(&mut self, parent_path: &str, name: &str) -> Result<()> {
        use crate::dir::iterator::DirIterator;

        // 1. 查找父目录
        let parent_inode = lookup_path(&mut self.bdev, &mut self.sb, parent_path)?;

        // 2. 构造完整路径查找目录 inode
        let full_path = if parent_path.ends_with('/') {
            alloc::format!("{parent_path}{name}")
        } else {
            alloc::format!("{parent_path}/{name}")
        };
        let dir_inode = lookup_path(&mut self.bdev, &mut self.sb, &full_path)?;

        // 3. 检查是否是目录
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dir_inode)?;
            if !inode_ref.is_dir()? {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Not a directory",
                ));
            }
        }

        // 4. 检查目录是否为空
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dir_inode)?;
            let mut iter = DirIterator::new(&mut inode_ref, 0)?;
            let mut entry_count = 0;

            while let Some(entry) = iter.next(&mut inode_ref)? {
                let name = &entry.name;
                if name != "." && name != ".." {
                    return Err(Error::new(
                        ErrorKind::NotEmpty,
                        "Directory not empty",
                    ));
                }
                entry_count += 1;
            }

            if entry_count < 2 {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Invalid directory structure",
                ));
            }
        }

        // 5. 从父目录删除条目并更新父目录链接计数
        self.remove_dir_entry(parent_inode, name)?;

        {
            let mut parent_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, parent_inode)?;

            parent_inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links.saturating_sub(1)).to_le();
            })?;

            parent_inode_ref.mark_dirty()?;
        }

        // 6. 释放目录 inode 和数据块
        self.truncate_file(dir_inode, 0)?;
        self.free_inode(dir_inode, true)?;

        Ok(())
    }

    /// 重命名文件或目录
    ///
    /// # 参数
    ///
    /// * `old_parent_path` - 旧的父目录路径
    /// * `old_name` - 旧名称
    /// * `new_parent_path` - 新的父目录路径
    /// * `new_name` - 新名称
    pub fn rename(
        &mut self,
        old_parent_path: &str,
        old_name: &str,
        new_parent_path: &str,
        new_name: &str,
    ) -> Result<()> {
        use crate::dir::write::{EXT4_DE_DIR, EXT4_DE_REG_FILE};

        // 1. 查找旧父目录
        let old_parent_inode = lookup_path(&mut self.bdev, &mut self.sb, old_parent_path)?;

        // 2. 查找新父目录
        let new_parent_inode = lookup_path(&mut self.bdev, &mut self.sb, new_parent_path)?;

        // 3. 构造完整路径查找文件/目录 inode
        let old_full_path = if old_parent_path.ends_with('/') {
            alloc::format!("{old_parent_path}{old_name}")
        } else {
            alloc::format!("{old_parent_path}/{old_name}")
        };
        let target_inode = lookup_path(&mut self.bdev, &mut self.sb, &old_full_path)?;

        // 4. 获取文件类型
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

        // 5. 在新父目录添加条目
        self.add_dir_entry(new_parent_inode, new_name, target_inode, file_type)?;

        // 如果是目录且移动到新父目录，增加新父目录的链接计数
        if is_dir && old_parent_inode != new_parent_inode {
            let mut new_parent_inode_ref =
                InodeRef::get(&mut self.bdev, &mut self.sb, new_parent_inode)?;

            new_parent_inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links + 1).to_le();
            })?;
            new_parent_inode_ref.mark_dirty()?;
        }

        // 6. 从旧父目录删除条目
        self.remove_dir_entry(old_parent_inode, old_name)?;

        // 如果是目录且移动到新父目录，减少旧父目录的链接计数
        if is_dir && old_parent_inode != new_parent_inode {
            let mut old_parent_inode_ref =
                InodeRef::get(&mut self.bdev, &mut self.sb, old_parent_inode)?;

            old_parent_inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links.saturating_sub(1)).to_le();
            })?;
            old_parent_inode_ref.mark_dirty()?;
        }

        // 7. 如果是目录且移动到新父目录，更新 ".." 条目
        if is_dir && old_parent_inode != new_parent_inode {
            self.remove_dir_entry(target_inode, "..")?;
            self.add_dir_entry(target_inode, "..", new_parent_inode, EXT4_DE_DIR)?;
        }

        Ok(())
    }
}
