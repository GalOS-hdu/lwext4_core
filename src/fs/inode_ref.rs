//! Inode 引用结构
//!
//! 对应 lwext4 的 `ext4_inode_ref`，提供 RAII 风格的 inode 操作

use crate::{
    block::{Block, BlockDev, BlockDevice},
    consts::*,
    error::{Error, ErrorKind, Result},
    extent::ExtentTree,
    superblock::Superblock,
    types::ext4_inode,
};

/// Inode 引用
///
/// 类似 lwext4 的 `ext4_inode_ref`，自动管理 inode 的加载和写回
///
/// # 设计说明
///
/// 与 lwext4 C 版本一致，InodeRef 持有一个 Block 句柄，
/// 直接操作 cache 中的 inode 数据，而不是持有数据副本。
/// 这保证了：
/// 1. **一致性**: 所有对同一 inode 的访问都操作同一份 cache 数据
/// 2. **性能**: 避免不必要的数据复制
/// 3. **正确语义**: 修改直接作用于 cache，自动标记为脏
///
/// # 生命周期
///
/// - 创建时获取包含 inode 的 block 句柄
/// - 通过 block 句柄访问和修改 inode 数据
/// - Drop 时自动释放 block 句柄
///
/// # 示例
///
/// ```rust,ignore
/// let mut inode_ref = InodeRef::get(&mut bdev, &sb, inode_num)?;
/// inode_ref.set_size(1024)?;
/// inode_ref.mark_dirty()?;
/// // Drop 时自动写回 inode
/// ```
pub struct InodeRef<'a, D: BlockDevice> {
    /// 块设备引用
    bdev: &'a mut BlockDev<D>,
    /// Superblock 引用（可变，以支持块分配等写操作）
    sb: &'a mut Superblock,
    /// Inode 编号
    inode_num: u32,
    /// Inode 所在的块地址
    inode_block_addr: u64,
    /// Inode 在块内的偏移（字节）
    offset_in_block: usize,
    /// 是否已标记为脏
    dirty: bool,
    /// 块映射缓存：(extent_logical_start, extent_len, physical_start)
    /// 🚀 性能优化：缓存整个extent的范围信息，而不是单个块
    /// 这样对于顺序访问，多个相邻块可以共享同一个缓存entry
    block_map_cache: Option<(u32, u32, u64)>,
}

impl<'a, D: BlockDevice> InodeRef<'a, D> {
    /// 获取 inode 引用（自动加载）
    ///
    /// # 参数
    ///
    /// * `bdev` - 块设备引用
    /// * `sb` - superblock 引用
    /// * `inode_num` - inode 编号
    ///
    /// # 返回
    ///
    /// 成功返回 InodeRef
    ///
    /// # 实现说明
    ///
    /// 对应 lwext4 的 `ext4_fs_get_inode_ref()`
    pub fn get(
        bdev: &'a mut BlockDev<D>,
        sb: &'a mut Superblock,
        inode_num: u32,
    ) -> Result<Self> {
        if inode_num == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid inode number (0)",
            ));
        }

        // 计算 inode 所在的块组和索引
        let inodes_per_group = sb.inodes_per_group();
        let block_group = (inode_num - 1) / inodes_per_group;
        let index_in_group = (inode_num - 1) % inodes_per_group;

        // 读取块组描述符以获取 inode 表位置
        // 注意：这里我们需要临时读取块组描述符，不需要持有 BlockGroupRef
        let inode_table_block = {
            use crate::block_group::BlockGroup;
            let bg = BlockGroup::load(bdev, sb, block_group)?;
            bg.get_inode_table_first_block(sb)
        };

        // 计算 inode 在 inode 表中的位置
        let block_size = sb.block_size() as u64;
        let inode_size = sb.inode_size() as u64;
        let inodes_per_block = block_size / inode_size;

        // 计算 inode 所在的块号和块内偏移
        let block_index = index_in_group as u64 / inodes_per_block;
        let offset_in_block = ((index_in_group as u64 % inodes_per_block) * inode_size) as usize;
        let inode_block_addr = inode_table_block + block_index;

        Ok(Self {
            bdev,
            sb,
            inode_num,
            inode_block_addr,
            offset_in_block,
            dirty: false,
            block_map_cache: None,
        })
    }

    /// 获取 inode 编号
    pub fn inode_num(&self) -> u32 {
        self.inode_num
    }

    /// 获取可变 Superblock 引用
    ///
    /// 注意：此方法仅供内部 API 使用，用于解决某些遗留 API 的借用冲突
    pub(crate) fn superblock_mut(&mut self) -> &mut Superblock {
        self.sb
    }

    /// 访问 inode 数据（只读）
    ///
    /// 通过闭包访问 inode 数据，避免生命周期问题
    pub fn with_inode<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&ext4_inode) -> R,
    {
        let mut block = Block::get(self.bdev, self.inode_block_addr)?;
        block.with_data(|data| {
            let inode = unsafe {
                &*(data.as_ptr().add(self.offset_in_block) as *const ext4_inode)
            };
            f(inode)
        })
    }

    /// 访问 inode 数据（可写）
    ///
    /// 通过闭包修改 inode 数据，自动标记 block 为脏
    pub fn with_inode_mut<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut ext4_inode) -> R,
    {
        let mut block = Block::get(self.bdev, self.inode_block_addr)?;
        let result = block.with_data_mut(|data| {
            let inode = unsafe {
                &mut *(data.as_mut_ptr().add(self.offset_in_block) as *mut ext4_inode)
            };
            f(inode)
        })?;
        self.dirty = true;
        Ok(result)
    }

    /// 访问 inode 原始字节数据（只读）
    ///
    /// 提供对完整 inode 区域的字节切片访问，包括 ext4_inode 结构体和额外空间。
    /// 这对于访问 xattr 等存储在 inode 额外空间的数据很有用。
    ///
    /// # 参数
    ///
    /// * `f` - 闭包，接收 inode 字节切片（长度为 inode_size）
    ///
    /// # 示例
    ///
    /// ```ignore
    /// inode_ref.with_inode_raw_data(|inode_data| {
    ///     // 访问 xattr 数据（在 inode 结构体之后）
    ///     let xattr_offset = EXT4_GOOD_OLD_INODE_SIZE + extra_isize;
    ///     let xattr_data = &inode_data[xattr_offset..];
    ///     // ...
    /// })?;
    /// ```
    pub fn with_inode_raw_data<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let inode_size = self.sb.inode_size() as usize;
        let mut block = Block::get(self.bdev, self.inode_block_addr)?;
        block.with_data(|data| {
            let start = self.offset_in_block;
            let end = start + inode_size;
            let inode_data = &data[start..end];
            f(inode_data)
        })
    }

    /// 访问 inode 原始字节数据（可写）
    ///
    /// 提供对完整 inode 区域的可变字节切片访问。
    /// 修改会自动标记 block 为脏。
    ///
    /// # 参数
    ///
    /// * `f` - 闭包，接收可变 inode 字节切片（长度为 inode_size）
    ///
    /// # 示例
    ///
    /// ```ignore
    /// inode_ref.with_inode_raw_data_mut(|inode_data| {
    ///     // 修改 xattr 数据
    ///     let xattr_offset = EXT4_GOOD_OLD_INODE_SIZE + extra_isize;
    ///     inode_data[xattr_offset..].copy_from_slice(&new_data);
    /// })?;
    /// ```
    pub fn with_inode_raw_data_mut<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let inode_size = self.sb.inode_size() as usize;
        let mut block = Block::get(self.bdev, self.inode_block_addr)?;
        let result = block.with_data_mut(|data| {
            let start = self.offset_in_block;
            let end = start + inode_size;
            let inode_data = &mut data[start..end];
            f(inode_data)
        })?;
        self.dirty = true;
        Ok(result)
    }

    /// 获取 Superblock 引用（只读）
    ///
    /// 注意：xattr 等模块需要访问 superblock 来获取配置信息
    pub fn superblock(&self) -> &Superblock {
        self.sb
    }

    /// 获取块设备可变引用
    ///
    /// 注意：此方法仅供内部模块使用（如 xattr 访问 xattr block）
    pub(crate) fn bdev_mut(&mut self) -> &mut BlockDev<D> {
        self.bdev
    }

    /// 获取块设备和 superblock 的可变引用
    ///
    /// 用于避免双重借用问题，当需要同时使用 bdev 和 sb 时使用此方法
    pub(crate) fn bdev_and_sb_mut(&mut self) -> (&mut BlockDev<D>, &mut Superblock) {
        (self.bdev, self.sb)
    }

    /// 标记为脏（需要写回）
    ///
    /// 注意：修改 inode 时会自动标记为脏，通常不需要手动调用
    pub fn mark_dirty(&mut self) -> Result<()> {
        if !self.dirty {
            // 标记 block 为脏 - 获取块并立即标记为脏
            let mut block = Block::get(self.bdev, self.inode_block_addr)?;
            block.with_data_mut(|_| {})?;
            self.dirty = true;
        }
        Ok(())
    }

    /// 检查是否为脏
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// 手动写回
    ///
    /// 通常不需要手动调用，Drop 时 Block 会自动写回脏数据
    pub fn flush(&mut self) -> Result<()> {
        // Block 的 Drop 会自动处理写回
        // 这里只需要清除 dirty 标志
        if self.dirty {
            self.dirty = false;
        }
        Ok(())
    }

    /// 强制写回 inode 到磁盘
    ///
    /// 🔧 关键修复：确保 inode 的修改被立即写入磁盘
    /// 用于关键操作后强制持久化，例如 extent 树增长后
    pub fn force_writeback(&mut self) -> Result<()> {
        if !self.dirty {
            // 没有修改，无需写回
            return Ok(());
        }

        // 显式读取并写回 inode block
        let mut block = crate::block::Block::get(self.bdev, self.inode_block_addr)?;

        // 通过 with_data_mut 触发 dirty 标记，确保 block drop 时写回
        block.with_data_mut(|_data| {
            // 数据已经在之前的 with_inode_mut 中修改过
            // 这里只是确保 block 被标记为 dirty
        })?;

        // 显式 drop block，触发写回
        drop(block);

        // 额外调用 flush 确保写入磁盘
        self.bdev.flush()?;

        self.dirty = false;

        log::debug!(
            "[InodeRef] force_writeback: ino={}, block_addr=0x{:x}",
            self.inode_num, self.inode_block_addr
        );

        Ok(())
    }

    // ===== 便捷方法 =====

    /// 获取文件大小
    pub fn size(&mut self) -> Result<u64> {
        self.with_inode(|inode| inode.file_size())
    }

    /// 设置文件大小
    pub fn set_size(&mut self, size: u64) -> Result<()> {
        self.with_inode_mut(|inode| {
            // 直接修改 inode 字段
            inode.size_lo = ((size << 32) >> 32).to_le() as u32;
            inode.size_hi = (size >> 32).to_le() as u32;
        })
    }

    /// 获取 blocks 计数（512 字节单位）
    pub fn blocks_count(&mut self) -> Result<u64> {
        // 先提取需要的 superblock 信息
        let has_huge_file = self.sb.has_ro_compat_feature(EXT4_FEATURE_RO_COMPAT_HUGE_FILE);
        let block_size = self.sb.block_size();

        self.with_inode(|inode| {
            // 读取 32 位低位
            let mut cnt = u32::from_le(inode.blocks_count_lo) as u64;

            // 检查是否启用了 HUGE_FILE 特性
            if has_huge_file {
                // 扩展到 48 位
                cnt |= (u16::from_le(inode.blocks_high) as u64) << 32;

                // 检查 inode 是否使用了 HUGE_FILE 标志
                let flags = u32::from_le(inode.flags);
                if flags & EXT4_INODE_FLAG_HUGE_FILE != 0 {
                    // 进行比例换算：从文件系统块单位转换为 512 字节单位
                    let block_bits = inode_block_bits_count(block_size);
                    return cnt << (block_bits - 9);
                }
            }

            cnt
        })
    }

    /// 设置 blocks 计数（512 字节单位）
    pub fn set_blocks_count(&mut self, count: u64) -> Result<()> {
        // 先提取需要的 superblock 信息
        let block_size = self.sb.block_size();

        self.with_inode_mut(|inode| {
            // 32 位最大值
            let max_32bit: u64 = 0xFFFFFFFF;

            if count <= max_32bit {
                // 可以用 32 位表示
                inode.blocks_count_lo = (count as u32).to_le();
                inode.blocks_high = 0;
                let flags = u32::from_le(inode.flags);
                inode.flags = (flags & !EXT4_INODE_FLAG_HUGE_FILE).to_le();
                return;
            }

            // 48 位最大值
            let max_48bit: u64 = 0xFFFFFFFFFFFF;

            if count <= max_48bit {
                // 可以用 48 位表示（不需要比例换算）
                inode.blocks_count_lo = (count as u32).to_le();
                inode.blocks_high = ((count >> 32) as u16).to_le();
                let flags = u32::from_le(inode.flags);
                inode.flags = (flags & !EXT4_INODE_FLAG_HUGE_FILE).to_le();
            } else {
                // 需要使用 HUGE_FILE 标志和比例换算
                let block_bits = inode_block_bits_count(block_size);

                let flags = u32::from_le(inode.flags);
                inode.flags = (flags | EXT4_INODE_FLAG_HUGE_FILE).to_le();

                // 从 512 字节单位转换为文件系统块单位
                let scaled_count = count >> (block_bits - 9);
                inode.blocks_count_lo = (scaled_count as u32).to_le();
                inode.blocks_high = ((scaled_count >> 32) as u16).to_le();
            }
        })
    }

    /// 增加 blocks 计数
    ///
    /// # 参数
    ///
    /// * `blocks` - 要增加的块数（文件系统块大小）
    pub fn add_blocks(&mut self, blocks: u32) -> Result<()> {
        let block_size = self.sb.block_size();
        let blocks_512 = blocks as u64 * (block_size as u64 / 512);
        let current = self.blocks_count()?;
        self.set_blocks_count(current + blocks_512)
    }

    /// 减少 blocks 计数
    ///
    /// # 参数
    ///
    /// * `blocks` - 要减少的块数（文件系统块大小）
    pub fn sub_blocks(&mut self, blocks: u32) -> Result<()> {
        let block_size = self.sb.block_size();
        let blocks_512 = blocks as u64 * (block_size as u64 / 512);
        let current = self.blocks_count()?;
        if current >= blocks_512 {
            self.set_blocks_count(current - blocks_512)
        } else {
            self.set_blocks_count(0)
        }
    }

    /// 设置文件权限（Unix 权限位）
    ///
    /// # 参数
    ///
    /// * `mode` - 权限位（0o000 - 0o7777）
    ///
    /// # 注意
    ///
    /// 只修改权限位（低 12 位），不修改文件类型位
    pub fn set_mode(&mut self, mode: u16) -> Result<()> {
        self.with_inode_mut(|inode| {
            let current_mode = u16::from_le(inode.mode);
            // 保留文件类型位（高 4 位），只修改权限位（低 12 位）
            let new_mode = (current_mode & 0xF000) | (mode & 0x0FFF);
            inode.mode = new_mode.to_le();
        })
    }

    /// 设置文件所有者
    ///
    /// # 参数
    ///
    /// * `uid` - 用户 ID
    /// * `gid` - 组 ID
    pub fn set_owner(&mut self, uid: u32, gid: u32) -> Result<()> {
        self.with_inode_mut(|inode| {
            // uid 存储在 uid 和 uid_high 字段
            inode.uid = (uid as u16).to_le();
            inode.uid_high = ((uid >> 16) as u16).to_le();

            // gid 存储在 gid 和 gid_high 字段
            inode.gid = (gid as u16).to_le();
            inode.gid_high = ((gid >> 16) as u16).to_le();
        })
    }

    /// 设置访问时间
    ///
    /// # 参数
    ///
    /// * `atime` - Unix 时间戳（秒）
    pub fn set_atime(&mut self, atime: u32) -> Result<()> {
        self.with_inode_mut(|inode| {
            inode.atime = atime.to_le();
        })
    }

    /// 设置修改时间
    ///
    /// # 参数
    ///
    /// * `mtime` - Unix 时间戳（秒）
    pub fn set_mtime(&mut self, mtime: u32) -> Result<()> {
        self.with_inode_mut(|inode| {
            inode.mtime = mtime.to_le();
        })
    }

    /// 设置变更时间
    ///
    /// # 参数
    ///
    /// * `ctime` - Unix 时间戳（秒）
    pub fn set_ctime(&mut self, ctime: u32) -> Result<()> {
        self.with_inode_mut(|inode| {
            inode.ctime = ctime.to_le();
        })
    }

    /// 检查是否是目录
    pub fn is_dir(&mut self) -> Result<bool> {
        self.with_inode(|inode| inode.is_dir())
    }

    /// 检查是否是普通文件
    pub fn is_file(&mut self) -> Result<bool> {
        self.with_inode(|inode| inode.is_file())
    }

    /// 检查是否使用 extents
    pub fn has_extents(&mut self) -> Result<bool> {
        self.with_inode(|inode| {
            let flags = u32::from_le(inode.flags);
            (flags & EXT4_INODE_FLAG_EXTENTS) != 0
        })
    }

    /// 获取 inode 数据的拷贝（用于需要长期持有的场景）
    ///
    /// 注意：返回的是数据副本，修改不会反映到磁盘
    pub fn get_inode_copy(&mut self) -> Result<ext4_inode> {
        self.with_inode(|inode| *inode)
    }

    /// 获取 inode 的 generation（用于校验和等）
    pub fn generation(&mut self) -> Result<u32> {
        self.with_inode(|inode| u32::from_le(inode.generation))
    }

    /// 获取 inode 编号（便捷方法）
    pub fn index(&self) -> u32 {
        self.inode_num
    }

    /// 获取 superblock 引用
    pub fn sb(&self) -> &Superblock {
        self.sb
    }

    /// 获取 BlockDev 的可变引用
    ///
    /// 用于需要访问块设备的操作（如读取目录块）
    pub fn bdev(&mut self) -> &mut BlockDev<D> {
        self.bdev
    }

    /// 获取 inode 所在的块地址
    pub fn inode_block_addr(&self) -> u64 {
        self.inode_block_addr
    }

    /// 获取 inode 在块内的偏移
    pub fn offset_in_block(&self) -> usize {
        self.offset_in_block
    }

    /// 将逻辑块号映射到物理块号
    ///
    /// 对应 lwext4 的 `ext4_fs_get_inode_dblk_idx()`
    ///
    /// # 参数
    ///
    /// * `logical_block` - 逻辑块号（文件内的块索引）
    /// * `create` - 是否在不存在时创建（暂不支持）
    ///
    /// # 返回
    ///
    /// 物理块号
    pub fn get_inode_dblk_idx(
        &mut self,
        logical_block: u32,
        create: bool,
    ) -> Result<u64> {
        use crate::{balloc::BlockAllocator, extent::get_blocks};

        // 检查是否使用 extents
        let uses_extents = self.has_extents()?;

        if !uses_extents {
            // 使用传统的 indirect blocks 映射
            if create {
                // Indirect blocks 的写入/分配暂不支持
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "Indirect block allocation not yet implemented",
                ));
            }

            // 使用 IndirectBlockMapper 进行只读映射
            use crate::indirect::IndirectBlockMapper;

            let mapper = IndirectBlockMapper::new(self.sb.block_size());
            let inode_wrapper = self.get_inode()?;

            match mapper.map_block(self.bdev, &inode_wrapper, logical_block as u64)? {
                Some(physical_block) => Ok(physical_block),
                None => Err(Error::new(
                    ErrorKind::NotFound,
                    "Logical block is a sparse hole in file",
                )),
            }
        } else {
            // 使用 extent 树映射

            // 🚀 性能优化：写入模式下也先检查缓存
            // 缓存存储整个extent的范围，对于顺序访问有极高的命中率
            if let Some((extent_start, extent_len, physical_start)) = self.block_map_cache {
                if logical_block >= extent_start && logical_block < extent_start + extent_len {
                    let offset = logical_block - extent_start;
                    let physical_block = physical_start + offset as u64;
                    return Ok(physical_block);
                }
            }

            if !create {
                // 只读模式：使用 ExtentTree 查找
                // 注意：这里使用快照是安全的，因为：
                // 1. self (InodeRef) 持有对 inode 块的独占访问
                // 2. 获取快照后立即使用，中间无其他操作
                // 3. InodeRef 不会被释放
                let inode_copy = self.get_inode_copy()?;
                let mut extent_tree = ExtentTree::new(self.bdev, self.sb.block_size());

                match extent_tree.map_block_internal(&inode_copy, logical_block)? {
                    Some(physical_block) => {
                        // 更新缓存（暂时缓存单个块，长度=1）
                        // TODO: 优化为缓存完整的extent范围
                        self.block_map_cache = Some((logical_block, 1, physical_block));
                        Ok(physical_block)
                    }
                    None => Err(Error::new(
                        ErrorKind::NotFound,
                        "Logical block not found in extent tree",
                    )),
                }
            } else {
                // 写入模式：使用 get_blocks 进行分配或查找
                // 安全性说明：
                // - get_blocks 需要 &mut Superblock 但 self 已持有 &mut sb
                // - 使用 unsafe 指针绕过借用检查器
                // - get_blocks 会修改 superblock 的空闲块计数，但不会与 InodeRef 冲突
                let sb_ptr = self.superblock_mut() as *mut Superblock;
                let sb_ref = unsafe { &mut *sb_ptr };

                let mut allocator = BlockAllocator::new();

                // 完全禁用推测性分配：只分配实际需要的块
                //
                // 背景：磁盘空间有限（rootfs 镜像可能只有 100-200MB）
                // 即使保守的预分配策略也会导致空间耗尽
                //
                // 策略：只分配 1 个块
                // - 优点：最大化空间利用率
                // - 缺点：可能创建更多 extent，但 insert_extent_with_auto_split 会自动处理
                //
                // 注意：insert_extent_with_auto_split() 会自动：
                // - grow_tree_depth 当根节点满时
                // - 插入到深度 1 的叶节点
                // 所以即使每个块一个 extent 也能正常工作
                let speculative_blocks = 1;

                let (physical_block, allocated_count) =
                    get_blocks(self, sb_ref, &mut allocator, logical_block, speculative_blocks, true)?;

                if physical_block == 0 {
                    Err(Error::new(
                        ErrorKind::NoSpace,
                        "Failed to allocate block",
                    ))
                } else {
                    // 🚀 更新缓存：缓存分配/查找到的块范围
                    // allocated_count表示从logical_block开始的连续块数
                    self.block_map_cache = Some((logical_block, allocated_count, physical_block));
                    Ok(physical_block)
                }
            }
        }
    }

    // ========================================================================
    // 块分配集成说明
    // ========================================================================
    //
    // InodeRef 的块分配功能通过 `balloc::fs_integration` 模块提供。
    //
    // 使用示例：
    // ```rust,ignore
    // use lwext4_core::balloc::fs_integration;
    //
    // // 分配块
    // let baddr = fs_integration::alloc_block_with_inode(
    //     &mut allocator, bdev, &mut sb, &mut inode_ref, goal
    // )?;
    //
    // // 释放块
    // fs_integration::free_block_with_inode(
    //     bdev, &mut sb, &mut inode_ref, baddr
    // )?;
    // ```
    //
    // 这些函数会自动更新 inode 的 blocks 计数和 superblock 的空闲块计数。

    // ========================================================================
    // xattr 支持方法
    // ========================================================================

    /// 获取 Inode 的只读引用（用于 xattr 等操作）
    ///
    /// 注意：返回的 Inode 不能修改，只能查询
    pub fn get_inode(&mut self) -> Result<crate::inode::Inode> {
        let inode_copy = self.get_inode_copy()?;
        Ok(crate::inode::Inode::from_raw(inode_copy, self.inode_num))
    }

    /// 获取完整的 inode 块数据（用于 xattr）
    ///
    /// 返回包含 inode 的完整块数据
    pub fn get_inode_data(&mut self) -> Result<alloc::vec::Vec<u8>> {
        // 直接从块设备读取 inode 所在的块
        let mut buf = alloc::vec![0u8; self.sb.block_size() as usize];
        self.bdev.read_block(self.inode_block_addr, &mut buf)?;
        Ok(buf)
    }

    /// 获取可修改的 inode 块数据（用于 xattr 写操作）
    ///
    /// 返回包含 inode 的完整块数据（可修改）
    ///
    /// 注意：调用者需要确保修改后调用 write_inode_data() 写回
    pub fn get_inode_data_mut(&mut self) -> Result<alloc::vec::Vec<u8>> {
        // 和 get_inode_data 相同，返回数据副本
        // 调用者负责写回
        self.get_inode_data()
    }

    /// 写回 inode 块数据
    ///
    /// 将修改后的 inode 块数据写回磁盘
    ///
    /// # 参数
    ///
    /// * `data` - 修改后的 inode 块数据
    ///
    /// # 注意
    ///
    /// 这个方法用于 xattr 等需要修改整个 inode 块的操作
    pub fn write_inode_data(&mut self, data: &[u8]) -> Result<()> {
        // 写回整个块
        self.bdev.write_block(self.inode_block_addr, data)?;
        // 标记为 dirty（虽然已经写回，但保持一致性）
        self.dirty = true;
        Ok(())
    }

    /// 获取 xattr block 地址
    ///
    /// 对应 C 的 ext4_inode_get_file_acl()
    ///
    /// # 返回
    ///
    /// xattr block 的块地址，如果没有则返回 0
    pub fn get_xattr_block_addr(&mut self) -> Result<u64> {
        let has_64bit = self.sb.has_incompat_feature(EXT4_FEATURE_INCOMPAT_64BIT);
        self.with_inode(|inode| {
            let mut acl = u32::from_le(inode.file_acl_lo) as u64;
            if has_64bit {
                acl |= (u16::from_le(inode.file_acl_high) as u64) << 32;
            }
            acl
        })
    }

    /// 设置 xattr block 地址
    ///
    /// 对应 C 的 ext4_inode_set_file_acl()
    ///
    /// # 参数
    ///
    /// * `addr` - xattr block 的块地址（0 表示删除）
    pub fn set_xattr_block_addr(&mut self, addr: u64) -> Result<()> {
        let has_64bit = self.sb.has_incompat_feature(EXT4_FEATURE_INCOMPAT_64BIT);
        self.with_inode_mut(|inode| {
            inode.file_acl_lo = (addr as u32).to_le();
            if has_64bit {
                inode.file_acl_high = ((addr >> 32) as u16).to_le();
            }
        })
    }

    /// 读取 xattr block（如果存在）
    ///
    /// 检查 inode.file_acl 字段，如果非零则读取对应的块
    pub fn read_xattr_block(&mut self) -> Result<Option<alloc::vec::Vec<u8>>> {
        let file_acl = self.get_xattr_block_addr()?;

        if file_acl == 0 {
            return Ok(None);
        }

        // 读取 xattr block
        let mut buf = alloc::vec![0u8; self.sb.block_size() as usize];
        self.bdev.read_block(file_acl, &mut buf)?;
        Ok(Some(buf))
    }

    /// 读取可修改的 xattr block（如果存在）
    ///
    /// 注意：调用者需要确保修改后写回
    pub fn read_xattr_block_mut(&mut self) -> Result<Option<alloc::vec::Vec<u8>>> {
        // 和 read_xattr_block 相同
        self.read_xattr_block()
    }

    // ========================================================================
    // 文件大小和块操作（写操作）
    // ========================================================================

    // 注意：truncate 方法已移到 Ext4FileSystem 层实现
    // 请使用 fs.truncate_file(inode_num, new_size)

    /// 获取 inode 当前文件末尾的逻辑块号
    ///
    /// 用于计算下一个要追加的块位置
    ///
    /// # 返回
    ///
    /// 文件末尾的逻辑块号（下一个块的位置）
    pub fn get_next_logical_block(&mut self) -> Result<u32> {
        let file_size = self.size()?;
        let block_size = self.sb.block_size();

        // 计算当前文件占用的块数（向上取整）
        let blocks = file_size.div_ceil(block_size as u64) as u32;

        Ok(blocks)
    }

    /// 计算块分配的目标位置（hint）
    ///
    /// 对应 lwext4 的 `ext4_fs_inode_to_goal_block()`
    ///
    /// # 返回
    ///
    /// 建议的物理块组 ID
    pub fn get_alloc_goal(&self) -> u32 {
        self.inode_num / self.sb.inodes_per_group()
    }

    /// 读取文件内容（支持 extent 和 indirect blocks，保证数据一致性）
    ///
    /// # 参数
    ///
    /// * `offset` - 文件内偏移（字节）
    /// * `buf` - 输出缓冲区
    ///
    /// # 返回
    ///
    /// 实际读取的字节数
    ///
    /// # 数据一致性
    ///
    /// 此方法会根据 inode 的标志自动选择 extent 或 indirect blocks 映射
    pub fn read_extent_file(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        // 特殊处理符号链接：VFS 层通过 read_at() 调用 read_extent_file() 来读取符号链接内容
        let is_symlink = self.with_inode(|inode| inode.is_symlink())?;
        if is_symlink {
            let file_size = self.size()?;

            // 快速符号链接：目标路径存储在 inode.blocks 中（< 60 字节）
            if file_size < 60 {
                if offset >= file_size {
                    return Ok(0); // EOF
                }

                let to_read = buf.len().min((file_size - offset) as usize);

                return self.with_inode(|inode| {
                    // 从 inode.blocks 读取字节数据
                    let symlink_data = unsafe {
                        core::slice::from_raw_parts(
                            inode.blocks.as_ptr() as *const u8,
                            file_size as usize,
                        )
                    };

                    buf[..to_read].copy_from_slice(
                        &symlink_data[offset as usize..offset as usize + to_read]
                    );

                    to_read
                })
                .map_err(|_| Error::new(ErrorKind::Io, "Failed to read fast symlink"));
            }
            // 慢速符号链接：目标路径存储在数据块中（≥ 60 字节）
            // 继续正常的文件读取流程
        }

        // 检查文件大小
        let file_size = self.size()?;
        if offset >= file_size {
            return Ok(0); // EOF
        }

        // 计算实际可读取的字节数
        let to_read = buf.len().min((file_size - offset) as usize);
        if to_read == 0 {
            return Ok(0);
        }

        let block_size = self.sb.block_size() as u64;

        // 检查是否使用 extents
        let uses_extents = self.has_extents()?;

        if uses_extents {
            // 使用 extent 树读取
            use crate::extent::ExtentTree;

            let bdev_ptr = self.bdev as *mut _;
            let bdev_ref = unsafe { &mut *bdev_ptr };
            let mut extent_tree = ExtentTree::new(bdev_ref, block_size as u32);

            self.with_inode(|inode| {
                extent_tree.read_file_internal(inode, offset, &mut buf[..to_read])
            })?
        } else {
            // 使用 indirect blocks 读取
            #[cfg(feature = "std")]
            eprintln!("[inode_ref] Reading with indirect blocks: offset={}, to_read={}", offset, to_read);

            let mut bytes_read = 0;
            let mut current_offset = offset;

            // 🚀 性能优化：复用块缓冲区
            let mut block_buf = alloc::vec![0u8; block_size as usize];

            while bytes_read < to_read {
                let logical_block = (current_offset / block_size) as u32;
                let offset_in_block = (current_offset % block_size) as usize;
                let remaining = to_read - bytes_read;
                let to_read_in_block = remaining.min(block_size as usize - offset_in_block);

                #[cfg(feature = "std")]
                eprintln!("[inode_ref] Logical block={}, offset_in_block={}, to_read_in_block={}",
                         logical_block, offset_in_block, to_read_in_block);

                // 使用 get_inode_dblk_idx 获取物理块号（已支持 indirect blocks）
                match self.get_inode_dblk_idx(logical_block, false) {
                    Ok(physical_block) => {
                        #[cfg(feature = "std")]
                        eprintln!("[inode_ref] Physical block={}", physical_block);

                        // 读取块数据（复用 block_buf）
                        let result = self.bdev.read_block(physical_block, &mut block_buf);

                        #[cfg(feature = "std")]
                        eprintln!("[inode_ref] Read result: {:?}", result);

                        result?;

                        // 复制到输出缓冲区
                        buf[bytes_read..bytes_read + to_read_in_block]
                            .copy_from_slice(&block_buf[offset_in_block..offset_in_block + to_read_in_block]);

                        bytes_read += to_read_in_block;
                        current_offset += to_read_in_block as u64;
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        #[cfg(feature = "std")]
                        eprintln!("[inode_ref] Block is a hole");

                        // 空洞，填充零
                        buf[bytes_read..bytes_read + to_read_in_block].fill(0);
                        bytes_read += to_read_in_block;
                        current_offset += to_read_in_block as u64;
                    }
                    Err(e) => {
                        #[cfg(feature = "std")]
                        eprintln!("[inode_ref] Error getting block: {:?}", e);
                        return Err(e);
                    }
                }
            }

            Ok(bytes_read)
        }
    }

    /// 映射逻辑块号到物理块号（使用 extent，保证数据一致性）
    ///
    /// # 参数
    ///
    /// * `logical_block` - 逻辑块号
    ///
    /// # 返回
    ///
    /// 物理块号（如果存在）
    ///
    /// # 数据一致性
    ///
    /// 此方法在 `with_inode` 闭包内使用 extent tree，保证读取最新数据
    pub fn map_extent_block(&mut self, logical_block: u32) -> Result<Option<u64>> {
        use crate::extent::ExtentTree;

        // 安全性说明：同 read_extent_file
        let bdev_ptr = self.bdev as *mut _;
        let block_size = self.sb.block_size();

        let bdev_ref = unsafe { &mut *bdev_ptr };
        let mut extent_tree = ExtentTree::new(bdev_ref, block_size);

        self.with_inode(|inode| {
            extent_tree.map_block_internal(inode, logical_block)
        })?
    }
}

impl<'a, D: BlockDevice> Drop for InodeRef<'a, D> {
    fn drop(&mut self) {
        // Block 的 Drop 会自动处理写回
        // 这里不需要额外操作
    }
}

/// 计算块大小的位数
///
/// 对应 lwext4 的 `ext4_inode_block_bits_count()`
///
/// # 参数
///
/// * `block_size` - 块大小（字节）
///
/// # 返回
///
/// 块大小的位数（用于地址计算）
fn inode_block_bits_count(block_size: u32) -> u32 {
    let mut bits = 8;
    let mut size = block_size;

    while size > 256 {
        bits += 1;
        size >>= 1;
    }

    bits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inode_ref_api() {
        // 这些测试需要实际的块设备和 ext4 文件系统
        // 主要是验证 API 的设计和编译
    }
}
