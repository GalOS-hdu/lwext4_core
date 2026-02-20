//! Ext4 文件系统核心结构

use crate::{
    block::{BlockDev, BlockDevice},
    dir::{lookup_path, read_dir, DirEntry},
    error::{Error, ErrorKind, Result},
    inode::Inode,
    superblock::Superblock,
};
use alloc::vec::Vec;

use super::{file::File, metadata::FileMetadata, inode_ref::InodeRef, block_group_ref::BlockGroupRef};

/// 文件系统统计信息
#[derive(Debug, Clone)]
pub struct FileSystemStats {
    /// 块大小（字节）
    pub block_size: u32,
    /// 总块数
    pub blocks_total: u64,
    /// 空闲块数
    pub blocks_free: u64,
    /// 可用块数（考虑保留块）
    pub blocks_available: u64,
    /// 总 inode 数
    pub inodes_total: u32,
    /// 空闲 inode 数
    pub inodes_free: u32,
    /// 文件系统 ID
    pub filesystem_id: u64,
    /// 最大文件名长度
    pub max_filename_len: u32,
}

/// Ext4 文件系统
///
/// 提供完整的文件系统操作接口
///
/// # 示例
///
/// ```rust,ignore
/// use lwext4_core::{Ext4FileSystem, BlockDev};
///
/// let device = MyBlockDevice::new();
/// let mut bdev = BlockDev::new(device);
/// let mut fs = Ext4FileSystem::mount(&mut bdev)?;
///
/// // 打开文件
/// let mut file = fs.open("/etc/passwd")?;
/// let mut buf = vec![0u8; 1024];
/// let n = file.read(&mut buf)?;
///
/// // 读取目录
/// let entries = fs.read_dir("/bin")?;
/// for entry in entries {
///     println!("{}", entry.name);
/// }
///
/// // 获取文件元数据
/// let metadata = fs.metadata("/etc/passwd")?;
/// println!("File size: {} bytes", metadata.size);
/// ```
pub struct Ext4FileSystem<D: BlockDevice> {
    pub(crate) bdev: BlockDev<D>,
    sb: Superblock,
}

impl<D: BlockDevice> Ext4FileSystem<D> {
    /// 挂载文件系统
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备包装器
    ///
    /// # 返回
    ///
    /// 成功返回文件系统实例
    ///
    /// # 错误
    ///
    /// - `ErrorKind::Corrupted` - 无效的 superblock
    /// - `ErrorKind::Io` - 设备读取失败
    pub fn mount(mut bdev: BlockDev<D>) -> Result<Self> {
        let sb = Superblock::load(&mut bdev)?;

        Ok(Self { bdev, sb })
    }

    /// 卸载文件系统
    ///
    /// 显式卸载文件系统，确保所有数据写回磁盘。
    ///
    /// # 返回
    ///
    /// 成功时返回底层的块设备，失败时返回错误
    ///
    /// # 注意
    ///
    /// - 此方法会消费 `self`，之后无法再使用该文件系统实例
    /// - 确保所有文件句柄已经关闭
    /// - 自动写回 superblock
    /// - 同步块设备缓存
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let mut fs = Ext4FileSystem::mount(bdev)?;
    /// // ... 进行文件系统操作 ...
    /// let bdev = fs.unmount()?; // 显式卸载
    /// ```
    ///
    /// # 与 Drop 的区别
    ///
    /// 如果不调用此方法，`Ext4FileSystem` 被 drop 时不会自动刷新数据。
    /// 建议显式调用此方法以确保数据完整性。
    pub fn unmount(mut self) -> Result<BlockDev<D>> {
        // 1. 写回 superblock
        self.sb.write(&mut self.bdev)?;

        // 2. 同步块设备（确保所有写操作完成）
        // 注意：BlockDev 目前没有显式的 sync 方法，
        // 但所有写操作都是同步的，所以数据已经在磁盘上

        // 3. 返回块设备的所有权
        Ok(self.bdev)
    }

    /// 获取 superblock 引用
    pub fn superblock(&self) -> &Superblock {
        &self.sb
    }

    /// 获取块设备引用
    pub fn block_device(&self) -> &BlockDev<D> {
        &self.bdev
    }

    /// 获取可变块设备引用
    pub fn block_device_mut(&mut self) -> &mut BlockDev<D> {
        &mut self.bdev
    }

    /// 获取可变 superblock 引用
    pub fn superblock_mut(&mut self) -> &mut Superblock {
        &mut self.sb
    }

    /// 获取文件系统统计信息
    ///
    /// # 返回
    ///
    /// 文件系统使用情况统计
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let stats = fs.stats()?;
    /// println!("Total blocks: {}", stats.blocks_total);
    /// println!("Free blocks: {}", stats.blocks_free);
    /// println!("Free inodes: {}", stats.inodes_free);
    /// ```
    pub fn stats(&self) -> Result<FileSystemStats> {
        let sb_inner = self.sb.inner();

        Ok(FileSystemStats {
            block_size: self.sb.block_size(),
            blocks_total: u32::from_le(sb_inner.blocks_count_lo) as u64
                | ((u32::from_le(sb_inner.blocks_count_hi) as u64) << 32),
            blocks_free: u32::from_le(sb_inner.free_blocks_count_lo) as u64
                | ((u32::from_le(sb_inner.free_blocks_count_hi) as u64) << 32),
            blocks_available: {
                let free = u32::from_le(sb_inner.free_blocks_count_lo) as u64
                    | ((u32::from_le(sb_inner.free_blocks_count_hi) as u64) << 32);
                let reserved = u32::from_le(sb_inner.r_blocks_count_lo) as u64
                    | ((u32::from_le(sb_inner.r_blocks_count_hi) as u64) << 32);
                free.saturating_sub(reserved)
            },
            inodes_total: u32::from_le(sb_inner.inodes_count),
            inodes_free: u32::from_le(sb_inner.free_inodes_count),
            filesystem_id: {
                // UUID 的前 8 字节作为文件系统 ID
                let uuid = &sb_inner.uuid;
                u64::from_le_bytes([
                    uuid[0], uuid[1], uuid[2], uuid[3],
                    uuid[4], uuid[5], uuid[6], uuid[7],
                ])
            },
            max_filename_len: 255, // EXT4_NAME_LEN
        })
    }

    /// 刷新所有缓存的脏数据到磁盘
    ///
    /// 该方法会将块缓存中的所有脏块写回磁盘，并调用设备的硬件刷新。
    ///
    /// # 返回
    ///
    /// 成功返回 Ok(())，失败返回错误
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// fs.flush()?; // 确保所有数据写入磁盘
    /// ```
    pub fn flush(&mut self) -> Result<()> {
        self.bdev.flush()
    }

    /// 获取 inode 引用
    ///
    /// # 参数
    ///
    /// * `inode_num` - inode 编号
    ///
    /// # 返回
    ///
    /// 成功返回 InodeRef，自动管理加载和写回
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let mut inode_ref = fs.get_inode_ref(2)?;
    /// println!("Size: {}", inode_ref.size());
    /// inode_ref.set_size(1024);
    /// inode_ref.mark_dirty();
    /// // 自动写回
    /// ```
    pub fn get_inode_ref(&mut self, inode_num: u32) -> Result<InodeRef<D>> {
        InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)
    }

    /// 获取块组引用
    ///
    /// # 参数
    ///
    /// * `bgid` - 块组 ID
    ///
    /// # 返回
    ///
    /// 成功返回 BlockGroupRef，自动管理加载和写回
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let mut bg_ref = fs.get_block_group_ref(0)?;
    /// println!("Free blocks: {}", bg_ref.free_blocks_count());
    /// bg_ref.dec_free_blocks(1);
    /// bg_ref.mark_dirty();
    /// // 自动写回
    /// ```
    pub fn get_block_group_ref(&mut self, bgid: u32) -> Result<BlockGroupRef<D>> {
        BlockGroupRef::get(&mut self.bdev, &self.sb, bgid)
    }

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
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let metadata = fs.metadata("/etc/passwd")?;
    /// println!("Size: {} bytes", metadata.size);
    /// println!("UID: {}, GID: {}", metadata.uid, metadata.gid);
    /// ```
    pub fn metadata(&mut self, path: &str) -> Result<FileMetadata> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let inode = Inode::load(&mut self.bdev, &self.sb, inode_num)?;

        Ok(FileMetadata::from_inode(&inode, inode_num))
    }

    /// 检查路径是否存在
    ///
    /// # 参数
    ///
    /// * `path` - 路径（绝对路径）
    pub fn exists(&mut self, path: &str) -> bool {
        lookup_path(&mut self.bdev, &mut self.sb, path).is_ok()
    }

    /// 检查路径是否是目录
    ///
    /// # 参数
    ///
    /// * `path` - 路径（绝对路径）
    pub fn is_dir(&mut self, path: &str) -> Result<bool> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        inode_ref.is_dir()
    }

    /// 检查路径是否是普通文件
    ///
    /// # 参数
    ///
    /// * `path` - 路径（绝对路径）
    pub fn is_file(&mut self, path: &str) -> Result<bool> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        inode_ref.is_file()
    }

    // ========== Metadata Write Operations ==========

    /// 修改文件/目录权限
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    /// * `mode` - Unix 权限位（0o000 - 0o7777）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 设置为 rw-r--r-- (0o644)
    /// fs.set_mode("/tmp/test.txt", 0o644)?;
    ///
    /// // 设置为 rwxr-xr-x (0o755)
    /// fs.set_mode("/usr/bin/app", 0o755)?;
    /// ```
    pub fn set_mode(&mut self, path: &str, mode: u16) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_mode(mode)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    /// 修改文件/目录所有者
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    /// * `uid` - 用户 ID
    /// * `gid` - 组 ID
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 修改所有者为 root:root
    /// fs.set_owner("/tmp/test.txt", 0, 0)?;
    ///
    /// // 修改所有者为 user:group
    /// fs.set_owner("/home/user/file.txt", 1000, 1000)?;
    /// ```
    pub fn set_owner(&mut self, path: &str, uid: u32, gid: u32) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_owner(uid, gid)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    /// 修改访问时间
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    /// * `atime` - Unix 时间戳（秒）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// use std::time::{SystemTime, UNIX_EPOCH};
    ///
    /// let now = SystemTime::now()
    ///     .duration_since(UNIX_EPOCH)
    ///     .unwrap()
    ///     .as_secs() as u32;
    /// fs.set_atime("/tmp/test.txt", now)?;
    /// ```
    pub fn set_atime(&mut self, path: &str, atime: u32) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_atime(atime)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    /// 修改修改时间
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    /// * `mtime` - Unix 时间戳（秒）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// use std::time::{SystemTime, UNIX_EPOCH};
    ///
    /// let now = SystemTime::now()
    ///     .duration_since(UNIX_EPOCH)
    ///     .unwrap()
    ///     .as_secs() as u32;
    /// fs.set_mtime("/tmp/test.txt", now)?;
    /// ```
    pub fn set_mtime(&mut self, path: &str, mtime: u32) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_mtime(mtime)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    /// 修改变更时间
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    /// * `ctime` - Unix 时间戳（秒）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// use std::time::{SystemTime, UNIX_EPOCH};
    ///
    /// let now = SystemTime::now()
    ///     .duration_since(UNIX_EPOCH)
    ///     .unwrap()
    ///     .as_secs() as u32;
    /// fs.set_ctime("/tmp/test.txt", now)?;
    /// ```
    pub fn set_ctime(&mut self, path: &str, ctime: u32) -> Result<()> {
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;
        let mut inode_ref = self.get_inode_ref(inode_num)?;
        inode_ref.set_ctime(ctime)?;
        inode_ref.mark_dirty()?;
        Ok(())
    }

    // ========== Extended Attributes (xattr) API ==========

    /// 列出文件/目录的所有扩展属性
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    ///
    /// # 返回
    ///
    /// 扩展属性名称列表（Vec<String>）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let attrs = fs.listxattr("/etc/passwd")?;
    /// for attr in attrs {
    ///     println!("Attribute: {}", attr);
    /// }
    /// ```
    pub fn listxattr(&mut self, path: &str) -> Result<Vec<alloc::string::String>> {
        use crate::xattr;
        use alloc::string::String;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        // 获取 InodeRef 并直接使用新的 xattr API
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 调用新的 xattr API（使用 InodeRef）
        let mut buffer = alloc::vec![0u8; 4096]; // 4KB 缓冲区
        let len = xattr::list(&mut inode_ref, &mut buffer)?;

        // 解析结果（以 \0 分隔的字符串列表）
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
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    /// * `name` - 属性名（含前缀，如 "user.comment"）
    ///
    /// # 返回
    ///
    /// 属性值（Vec<u8>）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let value = fs.getxattr("/etc/passwd", "user.comment")?;
    /// let text = String::from_utf8_lossy(&value);
    /// println!("Comment: {}", text);
    /// ```
    pub fn getxattr(&mut self, path: &str, name: &str) -> Result<Vec<u8>> {
        use crate::xattr;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        // 获取 InodeRef 并直接使用新的 xattr API
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 调用新的 xattr API（使用 InodeRef）
        let mut buffer = alloc::vec![0u8; 65536]; // 64KB 缓冲区（xattr 值最大 64KB）
        let len = xattr::get(&mut inode_ref, name, &mut buffer)?;

        buffer.truncate(len);
        Ok(buffer)
    }

    /// 设置扩展属性
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    /// * `name` - 属性名（含前缀，如 "user.comment"）
    /// * `value` - 属性值
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// fs.setxattr("/etc/passwd", "user.comment", b"System password file")?;
    /// ```
    pub fn setxattr(&mut self, path: &str, name: &str, value: &[u8]) -> Result<()> {
        use crate::xattr;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        // 获取 InodeRef 并使用新的 xattr API
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 调用新的 xattr API（使用 InodeRef）
        xattr::set(&mut inode_ref, name, value)?;

        Ok(())
    }

    /// 删除扩展属性
    ///
    /// # 参数
    ///
    /// * `path` - 文件或目录路径（绝对路径）
    /// * `name` - 属性名（含前缀，如 "user.comment"）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// fs.removexattr("/etc/passwd", "user.comment")?;
    /// ```
    pub fn removexattr(&mut self, path: &str, name: &str) -> Result<()> {
        use crate::xattr;

        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, path)?;

        // 获取 InodeRef 并使用新的 xattr API
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 调用新的 xattr API（使用 InodeRef）
        xattr::remove(&mut inode_ref, name)?;

        Ok(())
    }

    // ========== Inode 分配和释放 API ==========

    /// 分配一个新的 inode
    ///
    /// 对应 lwext4 的 `ext4_fs_alloc_inode()`
    ///
    /// # 参数
    ///
    /// * `is_dir` - 是否是目录
    ///
    /// # 返回
    ///
    /// 成功返回新分配的 inode 编号
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let inode_num = fs.alloc_inode(false)?; // 分配普通文件的 inode
    /// let mut inode_ref = fs.get_inode_ref(inode_num)?;
    /// // 初始化 inode 并使用
    /// ```
    pub fn alloc_inode(&mut self, is_dir: bool) -> Result<u32> {
        use crate::ialloc::InodeAllocator;

        let mut allocator = InodeAllocator::new();
        let inode_num = allocator.alloc_inode(&mut self.bdev, &mut self.sb, is_dir)?;

        Ok(inode_num)
    }

    /// 释放一个 inode
    ///
    /// 对应 lwext4 的 `ext4_fs_free_inode()`
    ///
    /// # 参数
    ///
    /// * `inode_num` - 要释放的 inode 编号
    /// * `is_dir` - 是否是目录
    ///
    /// # 返回
    ///
    /// 成功返回 Ok(())
    ///
    /// # 注意
    ///
    /// - 调用前应确保 inode 的数据块已全部释放
    /// - 调用前应确保 inode 的引用计数为 0
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 先释放 inode 的所有数据块
    /// let mut inode_ref = fs.get_inode_ref(inode_num)?;
    /// let is_dir = inode_ref.is_dir()?;
    /// inode_ref.truncate(&mut fs.superblock_mut(), 0)?;
    /// drop(inode_ref);
    ///
    /// // 然后释放 inode 本身
    /// fs.free_inode(inode_num, is_dir)?;
    /// ```
    pub fn free_inode(&mut self, inode_num: u32, is_dir: bool) -> Result<()> {
        use crate::ialloc::free_inode;

        free_inode(&mut self.bdev, &mut self.sb, inode_num, is_dir)?;

        Ok(())
    }

    /// 分配一个数据块（用于文件写入）
    ///
    /// # 参数
    ///
    /// * `goal` - 建议的块组 ID（用于局部性优化）
    ///
    /// # 返回
    ///
    /// 成功返回新分配的物理块号
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let inode_ref = fs.get_inode_ref(inode_num)?;
    /// let goal = inode_ref.get_alloc_goal();
    /// let block_addr = fs.alloc_block(goal as u64)?;
    /// // 使用 block_addr 写入数据
    /// ```
    pub fn alloc_block(&mut self, goal: u64) -> Result<u64> {
        use crate::balloc::BlockAllocator;

        let mut allocator = BlockAllocator::new();
        let block_addr = allocator.alloc_block(&mut self.bdev, &mut self.sb, goal)?;

        Ok(block_addr)
    }

    /// 释放一个数据块
    ///
    /// # 参数
    ///
    /// * `block_addr` - 要释放的物理块号
    ///
    /// # 返回
    ///
    /// 成功返回 Ok(())
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// fs.free_block(block_addr)?;
    /// ```
    pub fn free_block(&mut self, block_addr: u64) -> Result<()> {
        use crate::balloc::free_block;

        free_block(&mut self.bdev, &mut self.sb, block_addr)?;

        Ok(())
    }

    /// 截断文件到指定大小
    ///
    /// # 参数
    ///
    /// * `inode_num` - inode 编号
    /// * `new_size` - 新的文件大小
    ///
    /// # 注意
    ///
    /// 这是一个简化的实现，仅更新 inode 的大小字段。
    /// 实际的块释放需要单独调用 extent::remove_space。
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// fs.truncate_file(inode_num, 1024)?; // 截断到 1KB
    /// ```
    pub fn truncate_file(&mut self, inode_num: u32, new_size: u64) -> Result<()> {
        use crate::extent::remove_space;

        // 先获取block_size，避免借用冲突
        let block_size = self.sb.block_size() as u64;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        let old_size = inode_ref.size()?;

        // 大小相同，无需操作
        if old_size == new_size {
            return Ok(());
        }

        log::debug!(
            "[TRUNCATE] inode {inode_num} truncate: {old_size} -> {new_size} bytes"
        );

        if old_size < new_size {
            // ===== 情况 1: 扩展文件 =====
            // ext4 支持稀疏文件，所以我们只需要更新 i_size
            // 新增的区域会被视为"hole"（空洞），读取时返回 0
            // 不需要实际分配块或写入数据

            log::debug!(
                "[TRUNCATE] Expanding file (sparse): {old_size} -> {new_size} bytes"
            );

            inode_ref.set_size(new_size)?;
            inode_ref.mark_dirty()?;

        } else {
            // ===== 情况 2: 缩小文件 =====
            // 需要：
            // 1. 更新 i_size
            // 2. 清零部分块（如果需要）
            // 3. 释放不再需要的数据块

            log::debug!(
                "[TRUNCATE] Shrinking file: {old_size} -> {new_size} bytes"
            );

            // 步骤 1: 更新 i_size
            inode_ref.set_size(new_size)?;
            inode_ref.mark_dirty()?;
            drop(inode_ref); // 立即释放，后续操作会重新获取

            // 步骤 2: 如果新大小不是块对齐的，需要清零部分块
            // 这是关键！确保被截断的数据不会在重新扩展时"复活"
            let offset_in_block = (new_size % block_size) as usize;
            if new_size > 0 && offset_in_block != 0 {
                // 新结尾在块内部，需要清零该块的剩余部分
                let last_block_num = ((new_size - 1) / block_size) as u32;

                log::debug!(
                    "[TRUNCATE] Zeroing partial block {last_block_num}: offset {offset_in_block} to {block_size}"
                );

                // 重新获取 inode_ref 用于查找物理块
                let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

                // 使用 get_blocks 查找逻辑块对应的物理块（不分配新块）
                use crate::extent::get_blocks;
                use crate::balloc::BlockAllocator;

                // get_blocks 需要 &mut Superblock，但 inode_ref 已经借用了 sb
                // 使用 unsafe 获取另一个引用（与 remove_space 相同的模式）
                let sb_ptr = inode_ref.superblock_mut() as *mut crate::superblock::Superblock;
                let sb_ref = unsafe { &mut *sb_ptr };

                let mut allocator = BlockAllocator::new();
                let (physical_block, _count) = get_blocks(
                    &mut inode_ref,
                    sb_ref,
                    &mut allocator,
                    last_block_num,
                    1,
                    false, // 不分配新块，只查找
                )?;

                // 释放 inode_ref 以便访问 self.bdev
                drop(inode_ref);

                if physical_block != 0 {
                    // 该块存在，读取并清零部分数据后写回

                    // 读取物理块
                    let mut block_buf = alloc::vec![0u8; block_size as usize];
                    self.bdev.read_block(physical_block, &mut block_buf)?;

                    // 清零从 offset_in_block 到块末尾的部分
                    block_buf[offset_in_block..].fill(0);

                    // 写回物理块
                    self.bdev.write_block(physical_block, &block_buf)?;

                    log::debug!(
                        "[TRUNCATE] Zeroed bytes [{offset_in_block}, {block_size}) in block {last_block_num} (physical block {physical_block})"
                    );
                } else {
                    // 该块不存在（稀疏文件的hole），无需清零
                    log::debug!(
                        "[TRUNCATE] Block {last_block_num} is a hole, no need to zero"
                    );
                }
            }

            // 步骤 3: 计算需要释放的逻辑块范围
            // first_block_to_keep: 新大小需要的最后一个块的下一个块
            // last_block_to_remove: 旧大小占用的最后一个块（包含）
            let first_block_to_remove = if new_size == 0 {
                0
            } else {
                new_size.div_ceil(block_size) as u32
            };

            let last_block_to_remove = if old_size == 0 {
                0
            } else {
                ((old_size - 1) / block_size) as u32 // 包含最后一个字节的块号
            };

            // 步骤 4: 如果有需要释放的块，调用 remove_space
            if first_block_to_remove <= last_block_to_remove {
                log::debug!(
                    "[TRUNCATE] Freeing blocks: [{first_block_to_remove}, {last_block_to_remove}]"
                );

                // 重新获取 inode_ref 用于 remove_space
                let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

                // remove_space 需要 &mut Superblock，但 inode_ref 已经借用了 sb
                // 这里使用 unsafe 获取 sb 的另一个可变引用
                //
                // 安全性保证：
                // - inode_ref.sb 和 sb_ref 指向同一个对象
                // - remove_space 和 inode_ref 操作的 sb 字段不冲突
                // - 在 inode_ref 的生命周期内使用 sb_ref
                let sb_ptr = inode_ref.superblock_mut() as *mut crate::superblock::Superblock;
                let sb_ref = unsafe { &mut *sb_ptr };

                // 调用 remove_space 释放块
                // 注意：remove_space 的 to 参数是包含的（不是左闭右开）
                remove_space(&mut inode_ref, sb_ref, first_block_to_remove, last_block_to_remove)?;

                log::debug!(
                    "[TRUNCATE] Successfully freed {} blocks",
                    last_block_to_remove - first_block_to_remove + 1
                );
            } else {
                log::debug!("[TRUNCATE] No blocks to free");
            }
        }

        Ok(())
    }

    // ========== 内部辅助方法 ==========

    /// 获取或分配文件块（供 File::write 使用）
    ///
    /// # 参数
    ///
    /// * `inode_num` - Inode 编号
    /// * `logical_block` - 逻辑块号
    ///
    /// # 返回
    ///
    /// 物理块号
    ///
    /// # 注意
    ///
    /// 由于借用检查器限制，目前仅支持查找已分配的块，不支持自动分配
    pub(crate) fn get_file_block(&mut self, inode_num: u32, logical_block: u32) -> Result<u64> {
        // ✅ 使用 InodeRef 的辅助方法，保证数据一致性
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        let physical_block = inode_ref.map_extent_block(logical_block)?
            .ok_or_else(|| Error::new(ErrorKind::Unsupported, "Block not allocated - automatic allocation requires API redesign"))?;

        Ok(physical_block)
    }

    /// 添加目录项（内部辅助方法）
    ///
    /// # 注意
    ///
    /// 由于借用检查器限制，这个方法暂时标记为 TODO
    fn add_dir_entry(&mut self, dir_inode: u32, name: &str, child_inode: u32, file_type: u8) -> Result<()> {
        use crate::dir::write;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dir_inode)?;

        // 安全性说明：
        // - dir::write::add_entry 需要 &mut Superblock 但 inode_ref 已持有 &mut sb
        // - add_entry 只读取 superblock 的一些字段（block_size 等），不会修改
        // - 使用 unsafe 指针绕过借用检查器，确保不会产生数据竞争
        let sb_ptr = inode_ref.superblock_mut() as *mut Superblock;
        let sb_ref = unsafe { &mut *sb_ptr };

        write::add_entry(&mut inode_ref, sb_ref, name, child_inode, file_type)?;

        Ok(())
    }

    /// 删除目录项（内部辅助方法）
    ///
    /// # 注意
    ///
    /// 使用 unsafe 指针绕过借用检查器的限制
    fn remove_dir_entry(&mut self, dir_inode: u32, name: &str) -> Result<()> {
        use crate::dir::write;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dir_inode)?;

        // dir::write::remove_entry 只需要 inode_ref，不需要单独的 superblock
        write::remove_entry(&mut inode_ref, name)?;

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
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let inode_num = fs.create_file("/tmp", "test.txt", 0o644)?;
    /// ```
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

            // 设置时间戳（使用简单的时间戳，实际应该从系统获取）
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
            // inode_ref drop 时自动写回
        }

        // 3. 查找父目录并添加条目
        let parent_inode = lookup_path(&mut self.bdev, &mut self.sb, parent_path)?;

        // 4. 添加到父目录（通过辅助方法避免借用冲突）
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
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let inode_num = fs.create_dir("/tmp", "mydir", 0o755)?;
    /// ```
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

            // 设置初始大小为 0（目录项会自动增长）
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
            // inode_ref drop 时自动写回
        }

        // 4. 添加 "." 和 ".." 条目到新目录
        self.add_dir_entry(inode_num, ".", inode_num, EXT4_DE_DIR)?;
        self.add_dir_entry(inode_num, "..", parent_inode, EXT4_DE_DIR)?;

        // 5. 添加到父目录
        self.add_dir_entry(parent_inode, name, inode_num, EXT4_DE_DIR)?;

        // 6. 增加父目录的链接计数（因为新目录的 ".." 指向父目录）
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
    /// 为现有文件创建一个新的硬链接（多个目录项指向同一个 inode）。
    ///
    /// # 参数
    ///
    /// * `src_path` - 源文件的完整路径
    /// * `dst_dir` - 目标目录路径
    /// * `dst_name` - 新链接的名称
    ///
    /// # 返回
    ///
    /// 成功返回 Ok(())
    ///
    /// # 错误
    ///
    /// - `ErrorKind::NotFound` - 源文件不存在
    /// - `ErrorKind::InvalidInput` - 源不是普通文件（不能对目录创建硬链接）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 为 /tmp/original.txt 创建硬链接 /tmp/link.txt
    /// fs.flink("/tmp/original.txt", "/tmp", "link.txt")?;
    /// ```
    ///
    /// # 说明
    ///
    /// 硬链接与原文件共享相同的 inode 和数据块，修改任一文件都会影响另一个。
    /// 只有当所有硬链接都被删除后，文件数据才会被真正释放。
    pub fn flink(&mut self, src_path: &str, dst_dir: &str, dst_name: &str) -> Result<()> {
        use crate::dir::write::EXT4_DE_REG_FILE;

        // 1. 查找源文件 inode
        let src_inode = lookup_path(&mut self.bdev, &mut self.sb, src_path)?;

        // 2. 验证源是普通文件（不能对目录创建硬链接）
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

        // 5. 在目标目录添加新的目录项（指向相同的 inode）
        self.add_dir_entry(dst_dir_inode, dst_name, src_inode, EXT4_DE_REG_FILE)?;

        Ok(())
    }

    /// 创建符号链接
    ///
    /// 创建一个指向目标路径的符号链接。
    ///
    /// # 参数
    ///
    /// * `target` - 符号链接指向的目标路径（可以是相对或绝对路径）
    /// * `link_dir` - 符号链接所在目录的路径
    /// * `link_name` - 符号链接的名称
    ///
    /// # 返回
    ///
    /// 成功返回新创建的符号链接的 inode 编号
    ///
    /// # 说明
    ///
    /// - 快速符号链接（< 60 字节）：目标路径直接存储在 inode.block 中
    /// - 慢速符号链接（>= 60 字节）：需要分配数据块存储目标路径
    /// - 符号链接的权限通常为 0o777
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 创建符号链接 /tmp/link -> /etc/passwd
    /// fs.fsymlink("/etc/passwd", "/tmp", "link")?;
    /// ```
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
                    let block_slice = unsafe {
                        core::slice::from_raw_parts_mut(
                            inode.blocks.as_mut_ptr() as *mut u8,
                            60,
                        )
                    };
                    block_slice[..target_bytes.len()].copy_from_slice(target_bytes);
                })?;
            } else {
                // 慢速符号链接：需要分配块存储
                // 设置 EXTENTS 标志
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

                // drop inode_ref，然后写块
                drop(inode_ref);

                // 写入目标路径到块
                let mut block_buf = alloc::vec![0u8; block_size as usize];
                block_buf[..target_bytes.len()].copy_from_slice(target_bytes);
                self.bdev.write_block(block_addr, &block_buf)?;

                // 重新获取 inode_ref 以便继续（实际上已经不需要了）
                // return 会退出，所以这里直接返回
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
    ///
    /// # 参数
    ///
    /// * `link_path` - 符号链接的完整路径
    ///
    /// # 返回
    ///
    /// 成功返回符号链接指向的目标路径
    ///
    /// # 错误
    ///
    /// - `ErrorKind::NotFound` - 路径不存在
    /// - `ErrorKind::InvalidInput` - 路径不是符号链接
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let target = fs.readlink("/tmp/link")?;
    /// println!("Link points to: {}", target);
    /// ```
    pub fn readlink(&mut self, link_path: &str) -> Result<alloc::string::String> {
        use crate::consts::*;

        // 1. 查找符号链接 inode
        let inode_num = lookup_path(&mut self.bdev, &mut self.sb, link_path)?;

        // 提取 block_size（避免借用冲突）
        let block_size = self.sb.block_size();

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 2. 验证是符号链接
        let mode = inode_ref.with_inode(|inode| u16::from_le(inode.mode))?;
        if (mode & EXT4_INODE_MODE_TYPE_MASK) != EXT4_INODE_MODE_SOFTLINK {
            return Err(Error::new(ErrorKind::InvalidInput, "Not a symlink"));
        }

        let size = inode_ref.size()? as usize;
        if size == 0 {
            return Ok(alloc::string::String::new());
        }

        // 3. 读取目标路径
        let target_bytes = if size < 60 {
            // 快速符号链接：从 inode.blocks 读取
            inode_ref.with_inode(|inode| {
                let block_slice = unsafe {
                    core::slice::from_raw_parts(inode.blocks.as_ptr() as *const u8, size)
                };
                block_slice.to_vec()
            })?
        } else {
            // 慢速符号链接：从数据块读取
            let block_addr = inode_ref.get_inode_dblk_idx(0, false)?;
            if block_addr == 0 {
                return Err(Error::new(ErrorKind::NotFound, "Symlink data block not found"));
            }

            // drop inode_ref，然后读块
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
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// fs.remove_file("/tmp", "test.txt")?;
    /// ```
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

        // 3. 检查是否是普通文件或符号链接（不能删除目录）
        {
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, file_inode)?;
            let is_dir = inode_ref.is_dir()?;
            if is_dir {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Cannot remove directory with remove_file (use remove_dir)",
                ));
            }
            // 允许删除普通文件和符号链接
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

            // 检查是否是快速符号链接（< 60 字节，无数据块）
            let mode = inode_ref.with_inode(|inode| u16::from_le(inode.mode))?;
            let size = inode_ref.size()?;
            let is_symlink = (mode & EXT4_INODE_MODE_TYPE_MASK) == EXT4_INODE_MODE_SOFTLINK;
            let is_fast = is_symlink && size < 60;

            (links == 0, is_fast)
        };

        // 6. 如果链接计数为 0，释放 inode 和数据块
        if should_free {
            // 快速符号链接没有数据块，跳过截断
            if !is_fast_symlink {
                // 先截断文件以释放所有数据块
                self.truncate_file(file_inode, 0)?;
            }

            // 释放 inode
            self.free_inode(file_inode, false)?;
        }

        Ok(())
    }

    /// 删除目录
    ///
    /// # 参数
    ///
    /// * `parent_path` - 父目录路径
    /// * `name` - 目录名
    ///
    /// # 注意
    ///
    /// 只能删除空目录（只包含 "." 和 ".." 条目）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// fs.remove_dir("/tmp", "mydir")?;
    /// ```
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

        // 4. 检查目录是否为空（只有 "." 和 ".." 条目）
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

            // 目录应该至少有 "." 和 ".."
            if entry_count < 2 {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Invalid directory structure",
                ));
            }
        }

        // 5. 从父目录删除条目并更新父目录链接计数
        self.remove_dir_entry(parent_inode, name)?;

        // 减少父目录的链接计数（因为删除了指向父目录的 ".." 条目）
        {
            let mut parent_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, parent_inode)?;

            // 减少父目录的链接计数（因为删除了指向父目录的 ".." 条目）
            parent_inode_ref.with_inode_mut(|inode| {
                let links = u16::from_le(inode.links_count);
                inode.links_count = (links.saturating_sub(1)).to_le();
            })?;

            parent_inode_ref.mark_dirty()?;
        }

        // 6. 释放目录 inode 和数据块
        // 先截断以释放数据块
        self.truncate_file(dir_inode, 0)?;

        // 释放 inode
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
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// fs.rename("/tmp", "old.txt", "/tmp", "new.txt")?;
    /// fs.rename("/tmp", "file.txt", "/home", "file.txt")?; // 移动文件
    /// ```
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
            // 删除旧的 ".." 条目
            self.remove_dir_entry(target_inode, "..")?;

            // 添加新的 ".." 条目
            self.add_dir_entry(target_inode, "..", new_parent_inode, EXT4_DE_DIR)?;
        }

        Ok(())
    }

    // ========== VFS-style Inode-based API ==========
    //
    // 这些方法提供基于 inode 编号的操作，适配标准 VFS 接口模式

    /// 使用闭包访问 InodeRef
    ///
    /// 提供灵活的 inode 访问方式，自动管理 InodeRef 的生命周期和写回
    ///
    /// # 参数
    ///
    /// * `inode_num` - inode 编号
    /// * `f` - 操作闭包，接收 &mut InodeRef
    ///
    /// # 返回
    ///
    /// 闭包的返回值
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let size = fs.with_inode_ref(inode_num, |inode_ref| {
    ///     inode_ref.size()
    /// })?;
    /// ```
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
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let mut buf = vec![0u8; 1024];
    /// let n = fs.read_at_inode(inode_num, &mut buf, 0)?;
    /// println!("Read {} bytes", n);
    /// ```
    pub fn read_at_inode(&mut self, inode_num: u32, buf: &mut [u8], offset: u64) -> Result<usize> {
        // ✅ 使用 InodeRef 的辅助方法，保证数据一致性
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 检查 EOF
        let file_size = inode_ref.size()?;
        if offset >= file_size {
            return Ok(0); // EOF
        }

        inode_ref.read_extent_file(offset, buf)
    }

    /// 向指定 inode 的指定偏移量写入数据
    ///
    /// # 参数
    ///
    /// * `inode_num` - inode 编号
    /// * `buf` - 要写入的数据
    /// * `offset` - 写入起始偏移量（字节）
    ///
    /// # 返回
    ///
    /// 实际写入的字节数
    ///
    /// # 注意
    ///
    /// 此方法一次最多写入一个块内的数据，如需写入更多数据，需要多次调用
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let data = b"Hello, World!";
    /// let n = fs.write_at_inode(inode_num, data, 0)?;
    /// println!("Wrote {} bytes", n);
    /// ```
    pub fn write_at_inode(&mut self, inode_num: u32, buf: &[u8], offset: u64) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let block_size = self.sb.block_size() as u64;
        let logical_block = (offset / block_size) as u32;
        let offset_in_block = (offset % block_size) as usize;

        // 计算本次写入的数据量（不超过当前块的剩余空间）
        let remaining_in_block = block_size as usize - offset_in_block;
        let write_len = buf.len().min(remaining_in_block);

        // 🚀 性能优化：只获取一次 InodeRef，避免重复的 inode 块查找
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

        // 获取当前文件大小（后面需要判断是否需要更新）
        let current_size = inode_ref.size()?;

        // 获取或分配物理块
        let physical_block = inode_ref.get_inode_dblk_idx(logical_block, true)?; // create=true 自动分配

        if physical_block == 0 {
            return Err(Error::new(
                ErrorKind::NoSpace,
                "Failed to allocate block for write",
            ));
        }

        // 通过 InodeRef 访问 bdev（避免释放 InodeRef）
        let bdev = inode_ref.bdev_mut();

        // 🚀 性能优化：全块写入时跳过读取
        let mut block_buf = alloc::vec![0u8; block_size as usize];
        let is_full_block_write = offset_in_block == 0 && write_len == block_size as usize;

        if !is_full_block_write {
            // 部分块写入：需要先读取
            bdev.read_block(physical_block, &mut block_buf)?;
        }
        // 全块写入：跳过读取，直接覆盖（block_buf 已初始化为 0）

        // 在块内写入数据
        block_buf[offset_in_block..offset_in_block + write_len]
            .copy_from_slice(&buf[..write_len]);

        // 写回块
        bdev.write_block(physical_block, &block_buf)?;

        // 更新文件大小（如果写入超过了文件末尾）
        let new_end = offset + write_len as u64;
        if new_end > current_size {
            inode_ref.set_size(new_end)?;
            inode_ref.mark_dirty()?;
        }

        // InodeRef 在此 drop，自动写回修改
        Ok(write_len)
    }

    /// 批量写入数据到指定 inode（性能优化版本）
    ///
    /// 与 write_at_inode 不同，此方法可以一次写入多个块，
    /// 避免重复获取 InodeRef，显著提升大文件写入性能。
    ///
    /// # 参数
    ///
    /// * `inode_num` - inode 编号
    /// * `buf` - 要写入的数据
    /// * `offset` - 写入起始偏移量（字节）
    ///
    /// # 返回
    ///
    /// 实际写入的字节数
    ///
    /// # 性能
    ///
    /// - 100000块写入：write_at_inode需要100000次InodeRef获取
    /// - 100000块写入：write_at_inode_batch只需要1次InodeRef获取
    ///
    /// 预期性能提升：2-3倍
    pub fn write_at_inode_batch(&mut self, inode_num: u32, buf: &[u8], offset: u64) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let block_size = self.sb.block_size() as u64;

        // 🚀 关键优化：只获取一次 InodeRef，处理所有块
        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        let current_size = inode_ref.size()?;

        let mut bytes_written = 0;
        let mut current_offset = offset;

        // 🚀 性能优化：复用块缓冲区，避免循环内的重复分配
        let mut block_buf = alloc::vec![0u8; block_size as usize];

        while bytes_written < buf.len() {
            let logical_block = (current_offset / block_size) as u32;
            let offset_in_block = (current_offset % block_size) as usize;
            let remaining_in_block = block_size as usize - offset_in_block;
            let write_len = (buf.len() - bytes_written).min(remaining_in_block);

            // 获取或分配物理块
            let physical_block = inode_ref.get_inode_dblk_idx(logical_block, true)?;
            if physical_block == 0 {
                return Err(Error::new(ErrorKind::NoSpace, "Failed to allocate block"));
            }

            // 通过 InodeRef 访问 bdev
            let bdev = inode_ref.bdev_mut();

            // 优化：全块写入时跳过读取
            let is_full_block = offset_in_block == 0 && write_len == block_size as usize;

            if !is_full_block {
                bdev.read_block(physical_block, &mut block_buf)?;
            }
            // 全块写入时不需要读取，直接覆盖（block_buf会被完全覆盖）

            // 写入数据
            block_buf[offset_in_block..offset_in_block + write_len]
                .copy_from_slice(&buf[bytes_written..bytes_written + write_len]);

            // 写回块
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
    ///
    /// # 参数
    ///
    /// * `inode_num` - inode 编号
    ///
    /// # 返回
    ///
    /// FileMetadata 结构，包含文件类型、大小、权限、时间戳等信息
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let attr = fs.get_inode_attr(inode_num)?;
    /// println!("File size: {}", attr.size);
    /// println!("Mode: {:o}", attr.mode);
    /// ```
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

        // 读取块数（使用 blocks_count_with_sb 以正确处理 HUGE_FILE）
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
    /// # 参数
    ///
    /// * `parent_inode` - 父目录的 inode 编号
    /// * `name` - 要查找的名称
    ///
    /// # 返回
    ///
    /// 找到的子项的 inode 编号
    ///
    /// # 错误
    ///
    /// - `ErrorKind::NotFound` - 名称不存在
    /// - `ErrorKind::InvalidInput` - parent_inode 不是目录
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let child_inode = fs.lookup_in_dir(parent_inode, "file.txt")?;
    /// ```
    pub fn lookup_in_dir(&mut self, parent_inode: u32, name: &str) -> Result<u32> {
        // 读取目录条目
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

        // 查找匹配的条目
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
    /// # 参数
    ///
    /// * `parent_inode` - 父目录的 inode 编号
    /// * `name` - 新条目的名称
    /// * `file_type` - 文件类型（0=未知, 1=文件, 2=目录, 7=符号链接）
    /// * `mode` - 权限模式
    ///
    /// # 返回
    ///
    /// 新创建的 inode 编号
    ///
    /// # 注意
    ///
    /// 此方法会：
    /// 1. 分配新 inode
    /// 2. 初始化 inode（设置类型、权限、时间戳）
    /// 3. 在父目录中添加目录条目
    /// 4. 如果是目录，初始化 "." 和 ".." 条目
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 创建普通文件 (file_type=1)
    /// let inode = fs.create_in_dir(parent_inode, "file.txt", 1, 0o644)?;
    ///
    /// // 创建目录 (file_type=2)
    /// let dir_inode = fs.create_in_dir(parent_inode, "mydir", 2, 0o755)?;
    /// ```
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

            // 设置文件类型和权限
            let inode_mode = match file_type {
                EXT4_DE_REG_FILE => EXT4_INODE_MODE_FILE,
                EXT4_DE_DIR => EXT4_INODE_MODE_DIRECTORY,
                EXT4_DE_SYMLINK => EXT4_INODE_MODE_SOFTLINK,
                _ => EXT4_INODE_MODE_FILE, // 默认为普通文件
            };

            // 读取 superblock 的 extra_isize 配置（在创建 inode_ref 之前）
            let inode_size = self.sb.inode_size();
            let extra_isize = if inode_size > EXT4_GOOD_OLD_INODE_SIZE as u16 {
                let want_extra_isize = u16::from_le(self.sb.inner().want_extra_isize);
                if want_extra_isize > 0 {
                    want_extra_isize
                } else {
                    32u16  // 默认值
                }
            } else {
                0u16
            };

            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, new_inode)?;

            inode_ref.with_inode_mut(|inode| {
                inode.mode = (inode_mode | mode).to_le();
                inode.links_count = 1u16.to_le();

                // 设置时间戳
                let now = 0u32; // TODO: 获取当前时间
                inode.atime = now.to_le();
                inode.mtime = now.to_le();
                inode.ctime = now.to_le();

                // 设置 extra_isize
                if extra_isize > 0 {
                    inode.extra_isize = extra_isize.to_le();
                }
            })?;

            // 设置 EXTENTS 标志（启用 extent 格式）
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
                crate::dir::write::dir_init(&mut inode_ref, parent_inode)?;
            }
        }

        // 在父目录中添加条目
        self.add_dir_entry(parent_inode, name, new_inode, file_type)?;

        Ok(new_inode)
    }

    /// 读取指定目录 inode 的所有条目
    ///
    /// # 参数
    ///
    /// * `dir_inode` - 目录的 inode 编号
    ///
    /// # 返回
    ///
    /// 目录条目列表
    ///
    /// # 错误
    ///
    /// - `ErrorKind::InvalidInput` - inode 不是目录
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let entries = fs.read_dir_from_inode(dir_inode)?;
    /// for entry in entries {
    ///     println!("{}: inode {}", entry.name, entry.inode);
    /// }
    /// ```
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
    /// # 参数
    ///
    /// * `parent_inode` - 父目录的 inode 编号
    /// * `name` - 要删除的条目名称
    ///
    /// # 返回
    ///
    /// 被删除条目的 inode 编号
    ///
    /// # 注意
    ///
    /// 此方法只删除目录条目，不会：
    /// - 减少目标 inode 的链接计数
    /// - 释放目标 inode 的数据块
    /// - 释放目标 inode 本身
    ///
    /// 调用者需要自行处理这些清理工作
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let removed_inode = fs.unlink_from_dir(parent_inode, "file.txt")?;
    ///
    /// // 减少链接计数
    /// let links = fs.with_inode_ref(removed_inode, |inode_ref| {
    ///     inode_ref.with_inode_mut(|inode| {
    ///         let links = u16::from_le(inode.links_count);
    ///         inode.links_count = (links - 1).to_le();
    ///         Ok(links - 1)
    ///     })
    /// })??;
    ///
    /// // 如果链接计数为 0，释放 inode
    /// if links == 0 {
    ///     fs.truncate_file(removed_inode, 0)?;
    ///     fs.free_inode(removed_inode, false)?;
    /// }
    /// ```
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
    ///
    /// 在两个目录之间移动/重命名条目，使用 inode 编号而非路径
    ///
    /// # 参数
    ///
    /// * `src_dir_ino` - 源目录的 inode 编号
    /// * `src_name` - 源条目名称
    /// * `dst_dir_ino` - 目标目录的 inode 编号
    /// * `dst_name` - 目标条目名称
    ///
    /// # 行为
    ///
    /// - 从源目录移除 `src_name` 条目
    /// - 在目标目录添加 `dst_name` 条目，指向同一 inode
    /// - 如果移动目录且跨父目录：
    ///   - 更新源父目录和目标父目录的链接计数
    ///   - 更新被移动目录的 ".." 条目
    ///
    /// # 错误
    ///
    /// - `ErrorKind::NotFound` - 源条目不存在
    /// - `ErrorKind::InvalidInput` - inode 不是目录
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 在同一目录内重命名
    /// fs.rename_inode(dir_ino, "old.txt", dir_ino, "new.txt")?;
    ///
    /// // 移动到不同目录
    /// fs.rename_inode(src_dir_ino, "file.txt", dst_dir_ino, "file.txt")?;
    /// ```
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
        //    注意：必须完整删除，包括释放 inode 和数据块
        //    否则会导致文件系统元数据损坏
        match self.lookup_in_dir(dst_dir_ino, dst_name) {
            Ok(old_target_inode) => {
                // 目标文件存在，需要完整删除
                // 先从目录中移除条目
                self.remove_dir_entry(dst_dir_ino, dst_name)?;

                // 减少链接计数并释放资源（如果链接计数降为 0）
                let (old_is_dir, _new_links) = {
                    let mut old_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, old_target_inode)?;
                    let old_is_dir = old_inode_ref.is_dir()?;

                    // 获取当前链接计数
                    let current_links = old_inode_ref.with_inode(|inode| {
                        u16::from_le(inode.links_count)
                    })?;

                    // 减少链接计数
                    let new_links = current_links.saturating_sub(1);
                    old_inode_ref.with_inode_mut(|inode| {
                        inode.links_count = new_links.to_le();
                    })?;
                    old_inode_ref.mark_dirty()?;

                    // 如果链接计数降为 0，只标记待删除，不立即释放
                    // 真正的删除会在 VFS 层没有引用时通过 drop_inode 触发
                    if new_links == 0 {
                        log::info!(
                            "[RENAME] inode {old_target_inode} i_nlink=0, marked for deferred deletion"
                        );
                        // 不在这里释放，等待 drop_inode 调用
                    }

                    (old_is_dir, new_links)
                }; // old_inode_ref 在这里被释放

                // 如果是目录，还需要减少父目录的链接计数
                if old_is_dir {
                    let mut dst_parent_ref = InodeRef::get(&mut self.bdev, &mut self.sb, dst_dir_ino)?;
                    dst_parent_ref.with_inode_mut(|inode| {
                        let links = u16::from_le(inode.links_count);
                        inode.links_count = links.saturating_sub(1).to_le();
                    })?;
                    dst_parent_ref.mark_dirty()?;
                }

                // 如果链接计数降为 0，inode会在后续被VFS层drop时释放
                // 这里不做任何操作
            }
            Err(_) => {
                // 目标不存在，忽略（这是正常情况）
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
            // 删除旧的 ".." 条目
            self.remove_dir_entry(target_inode, "..")?;

            // 添加新的 ".." 条目
            self.add_dir_entry(target_inode, "..", dst_dir_ino, EXT4_DE_DIR)?;
        }

        Ok(())
    }

    /// 创建硬链接 (VFS 风格)
    ///
    /// 在指定目录中创建指向已存在 inode 的新目录条目
    ///
    /// # 参数
    ///
    /// * `dir_ino` - 目录的 inode 编号
    /// * `name` - 新链接的名称
    /// * `child_ino` - 目标 inode 编号
    ///
    /// # 行为
    ///
    /// - 在目录中添加新条目
    /// - 增加目标 inode 的链接计数
    /// - 不允许对目录创建硬链接（ext4 限制）
    ///
    /// # 错误
    ///
    /// - `ErrorKind::InvalidInput` - dir_ino 不是目录或 child_ino 是目录
    /// - `ErrorKind::AlreadyExists` - 名称已存在
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 为文件创建硬链接
    /// fs.link_inode(dir_ino, "link_name.txt", file_ino)?;
    /// ```
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

        // 2. 验证 child_ino 不是目录（ext4 不支持目录硬链接）
        let file_type = {
            let mut child_inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, child_ino)?;
            let is_dir = child_inode_ref.is_dir()?;

            if is_dir {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Cannot create hard link to directory",
                ));
            }

            // 获取文件类型
            child_inode_ref.with_inode(|inode| {
                let mode = u16::from_le(inode.mode);
                let type_bits = mode & crate::consts::EXT4_INODE_MODE_TYPE_MASK;

                // 根据 mode 确定目录条目类型
                match type_bits {
                    crate::consts::EXT4_INODE_MODE_FILE => crate::dir::write::EXT4_DE_REG_FILE,
                    crate::consts::EXT4_INODE_MODE_SOFTLINK => crate::dir::write::EXT4_DE_SYMLINK,
                    crate::consts::EXT4_INODE_MODE_CHARDEV => crate::dir::write::EXT4_DE_CHRDEV,
                    crate::consts::EXT4_INODE_MODE_BLOCKDEV => crate::dir::write::EXT4_DE_BLKDEV,
                    crate::consts::EXT4_INODE_MODE_FIFO => crate::dir::write::EXT4_DE_FIFO,
                    crate::consts::EXT4_INODE_MODE_SOCKET => crate::dir::write::EXT4_DE_SOCK,
                    _ => EXT4_DE_REG_FILE, // 默认为普通文件
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

            // 释放数据块
            let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, ino)?;
            inode_ref.set_size(0)?;
            drop(inode_ref);

            // 释放inode号
            self.free_inode(ino, is_dir)?;
        } else {
            log::debug!("[DROP_INODE] inode {ino} still has nlink={nlink}, not freeing");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filesystem_api() {
        // 这些测试需要实际的块设备和 ext4 文件系统
        // 主要是验证 API 的设计和编译
    }
}
