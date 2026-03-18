//! 块设备核心类型

use crate::error::{Error, ErrorKind, Result};

/// 块设备接口
///
/// 实现此 trait 以提供底层块设备访问。
///
/// # 示例
///
/// ```rust,ignore
/// use lwext4_core::{BlockDevice, Result};
///
/// struct MyDevice {
///     // ...
/// }
///
/// impl BlockDevice for MyDevice {
///     fn block_size(&self) -> u32 {
///         4096
///     }
///
///     fn sector_size(&self) -> u32 {
///         512
///     }
///
///     fn total_blocks(&self) -> u64 {
///         1000000
///     }
///
///     fn read_blocks(&mut self, lba: u64, count: u32, buf: &mut [u8]) -> Result<usize> {
///         // 实现块读取
///         Ok(count as usize * self.sector_size() as usize)
///     }
///
///     fn write_blocks(&mut self, lba: u64, count: u32, buf: &[u8]) -> Result<usize> {
///         // 实现块写入
///         Ok(count as usize * self.sector_size() as usize)
///     }
/// }
/// ```
pub trait BlockDevice {
    /// 逻辑块大小（通常 4096）
    fn block_size(&self) -> u32;

    /// 物理扇区大小（通常 512）
    fn sector_size(&self) -> u32;

    /// 总块数
    fn total_blocks(&self) -> u64;

    /// 读取扇区
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址（以扇区为单位）
    /// * `count` - 要读取的扇区数
    /// * `buf` - 目标缓冲区（大小至少为 count * sector_size）
    ///
    /// # 返回
    ///
    /// 成功返回实际读取的字节数
    fn read_blocks(&mut self, lba: u64, count: u32, buf: &mut [u8]) -> Result<usize>;

    /// 写入扇区
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址（以扇区为单位）
    /// * `count` - 要写入的扇区数
    /// * `buf` - 源缓冲区（大小至少为 count * sector_size）
    ///
    /// # 返回
    ///
    /// 成功返回实际写入的字节数
    fn write_blocks(&mut self, lba: u64, count: u32, buf: &[u8]) -> Result<usize>;

    /// 刷新缓存
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    /// 是否只读
    fn is_read_only(&self) -> bool {
        false
    }

    /// 打开设备
    ///
    /// 在开始使用设备前调用，用于初始化设备资源。
    /// 默认实现什么都不做，设备可以根据需要覆盖此方法。
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// impl BlockDevice for MyDevice {
    ///     fn open(&mut self) -> Result<()> {
    ///         // 打开文件、初始化硬件等
    ///         self.file = File::open(&self.path)?;
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn open(&mut self) -> Result<()> {
        Ok(())
    }

    /// 关闭设备
    ///
    /// 在停止使用设备后调用，用于清理设备资源。
    /// 默认实现什么都不做，设备可以根据需要覆盖此方法。
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// impl BlockDevice for MyDevice {
    ///     fn close(&mut self) -> Result<()> {
    ///         // 刷新并关闭文件、释放资源等
    ///         self.flush()?;
    ///         self.file.close()?;
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// 块设备包装器
///
/// 为 ext4 文件系统提供块级访问，包含统计信息。
///
/// # 并发使用
///
/// BlockDev 本身不包含内部锁，在单线程环境中可以直接使用。
/// 对于多线程环境，用户应该使用 `DeviceLock` trait 包装 BlockDev：
///
/// ```rust,ignore
/// use std::sync::{Arc, Mutex};
///
/// // 单线程
/// let mut block_dev = BlockDev::new(device)?;
///
/// // 多线程
/// let block_dev = Arc::new(Mutex::new(BlockDev::new(device)?));
/// ```
///
/// 对应 lwext4 的 `ext4_block_dev_lock/unlock` API
pub struct BlockDev<D: BlockDevice> {
    /// 底层设备
    device: D,
    /// 分区偏移（字节）
    partition_offset: u64,
    /// 分区大小（字节）
    partition_size: u64,
    /// 逻辑读取次数（包括缓存命中）
    read_count: u64,
    /// 逻辑写入次数（包括缓存写入）
    write_count: u64,
    /// 物理读取次数（实际设备操作）
    physical_read_count: u64,
    /// 物理写入次数（实际设备操作）
    physical_write_count: u64,
    /// 引用计数（用于跟踪设备使用）
    ref_count: u32,
    /// 块缓存（可选）
    pub(super) bcache: Option<crate::cache::BlockCache>,
}

impl<D: BlockDevice> BlockDev<D> {
    /// 创建新的块设备包装器（无缓存）
    pub fn new(device: D) -> Result<Self> {
        let block_size = device.block_size();
        let sector_size = device.sector_size();

        // 验证块大小是扇区大小的整数倍
        if block_size % sector_size != 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Block size must be a multiple of sector size",
            ));
        }

        let total_blocks = device.total_blocks();
        let partition_size = total_blocks * block_size as u64;

        Ok(Self {
            device,
            partition_offset: 0,
            partition_size,
            read_count: 0,
            write_count: 0,
            physical_read_count: 0,
            physical_write_count: 0,
            ref_count: 0,
            bcache: None,
        })
    }

    /// 创建带缓存的块设备包装器
    ///
    /// # 参数
    ///
    /// * `device` - 底层块设备
    /// * `cache_blocks` - 缓存块数量
    pub fn new_with_cache(device: D, cache_blocks: usize) -> Result<Self> {
        let mut bd = Self::new(device)?;
        let block_size = bd.block_size() as usize;
        bd.bcache = Some(crate::cache::BlockCache::new(cache_blocks, block_size));
        Ok(bd)
    }

    /// 创建使用默认缓存大小的块设备包装器
    ///
    /// 使用 `DEFAULT_CACHE_SIZE` (8 块) 作为缓存大小
    pub fn with_default_cache(device: D) -> Result<Self> {
        Self::new_with_cache(device, crate::cache::DEFAULT_CACHE_SIZE)
    }

    /// 创建指定分区的块设备包装器（无缓存）
    ///
    /// # 参数
    ///
    /// * `device` - 底层块设备
    /// * `offset` - 分区起始偏移（字节）
    /// * `size` - 分区大小（字节）
    pub fn new_partition(device: D, offset: u64, size: u64) -> Result<Self> {
        let mut bd = Self::new(device)?;
        bd.set_partition(offset, size);
        Ok(bd)
    }

    /// 创建指定分区且带缓存的块设备包装器
    ///
    /// # 参数
    ///
    /// * `device` - 底层块设备
    /// * `offset` - 分区起始偏移（字节）
    /// * `size` - 分区大小（字节）
    /// * `cache_blocks` - 缓存块数量
    pub fn new_partition_with_cache(
        device: D,
        offset: u64,
        size: u64,
        cache_blocks: usize,
    ) -> Result<Self> {
        let mut bd = Self::new_with_cache(device, cache_blocks)?;
        bd.set_partition(offset, size);
        Ok(bd)
    }

    /// 获取底层设备的引用
    pub fn device(&self) -> &D {
        &self.device
    }

    /// 获取底层设备的可变引用
    pub fn device_mut(&mut self) -> &mut D {
        &mut self.device
    }

    /// 获取逻辑块大小
    pub fn block_size(&self) -> u32 {
        self.device.block_size()
    }

    /// 获取物理扇区大小
    pub fn sector_size(&self) -> u32 {
        self.device.sector_size()
    }

    /// 获取总块数
    pub fn total_blocks(&self) -> u64 {
        self.device.total_blocks()
    }

    /// 获取逻辑读取次数（包括缓存命中）
    pub fn read_count(&self) -> u64 {
        self.read_count
    }

    /// 获取逻辑写入次数（包括缓存写入）
    pub fn write_count(&self) -> u64 {
        self.write_count
    }

    /// 获取物理读取次数（实际设备操作）
    ///
    /// 对应 lwext4 的 `bread_ctr`
    pub fn physical_read_count(&self) -> u64 {
        self.physical_read_count
    }

    /// 获取物理写入次数（实际设备操作）
    ///
    /// 对应 lwext4 的 `bwrite_ctr`
    pub fn physical_write_count(&self) -> u64 {
        self.physical_write_count
    }

    /// 获取缓存命中率
    ///
    /// 返回 0.0 到 1.0 之间的值，表示缓存命中的百分比
    pub fn cache_hit_rate(&self) -> f64 {
        if self.read_count == 0 {
            return 0.0;
        }
        let hits = self.read_count.saturating_sub(self.physical_read_count);
        hits as f64 / self.read_count as f64
    }

    /// 设置分区偏移和大小
    ///
    /// # 参数
    ///
    /// * `offset` - 分区起始偏移（字节）
    /// * `size` - 分区大小（字节）
    pub fn set_partition(&mut self, offset: u64, size: u64) {
        self.partition_offset = offset;
        self.partition_size = size;
    }

    /// 获取分区偏移
    pub fn partition_offset(&self) -> u64 {
        self.partition_offset
    }

    /// 获取分区大小
    pub fn partition_size(&self) -> u64 {
        self.partition_size
    }

    // 内部辅助方法

    /// 将逻辑块地址转换为物理扇区地址
    pub(super) fn logical_to_physical(&self, lba: u64) -> u64 {
        let block_size = self.device.block_size() as u64;
        let sector_size = self.device.sector_size() as u64;
        (lba * block_size + self.partition_offset) / sector_size
    }

    /// 每个逻辑块包含的物理扇区数
    pub(super) fn sectors_per_block(&self) -> u32 {
        self.device.block_size() / self.device.sector_size()
    }

    /// 增加逻辑读计数（每次 read_block 调用时递增）
    pub(super) fn inc_read_count(&mut self) {
        self.read_count += 1;
    }

    /// 增加逻辑写计数（每次 write_block 调用时递增）
    pub(super) fn inc_write_count(&mut self) {
        self.write_count += 1;
    }

    /// 增加物理读计数（实际调用 device.read_blocks 时递增）
    pub(super) fn inc_physical_read_count(&mut self) {
        self.physical_read_count += 1;
    }

    /// 增加物理写计数（实际调用 device.write_blocks 时递增）
    pub(super) fn inc_physical_write_count(&mut self) {
        self.physical_write_count += 1;
    }

    /// 刷新指定逻辑块地址的缓存
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    ///
    /// # 返回
    ///
    /// 成功返回 Ok(())
    ///
    /// # 错误
    ///
    /// 如果块不在缓存中或写入失败，返回错误
    ///
    /// # 性能优化
    ///
    /// 使用栈分配的临时缓冲区（对于小块）或预分配缓冲区
    pub fn flush_lba(&mut self, lba: u64) -> Result<()> {
        // 先获取必要参数，避免借用冲突
        let sector_size = self.device.sector_size();
        let partition_offset = self.partition_offset;
        let block_size = self.block_size();

        // 🚀 性能优化：预分配缓冲区
        let mut flush_buf = alloc::vec![0u8; block_size as usize];

        let has_data = if let Some(cache) = &mut self.bcache {
            // 使用新架构：Cache提供数据，BlockDev负责I/O
            // 复制数据到预分配缓冲区
            if let Some(data) = cache.get_block_data(lba) {
                flush_buf[..data.len()].copy_from_slice(data);
                true
            } else {
                false
            }
        } else {
            return Ok(());
        };

        if !has_data {
            return Ok(());
        }

        // 计算物理地址并写入
        let pba = (lba * block_size as u64 + partition_offset) / sector_size as u64;
        let count = (block_size as usize).div_ceil(sector_size as usize);
        self.inc_physical_write_count();
        self.device_mut().write_blocks(pba, count as u32, &flush_buf)?;

        // 重新借用cache并标记为clean
        if let Some(cache) = &mut self.bcache {
            cache.mark_clean(lba)?;
        }

        log::debug!("[BlockDev] Flushed single block LBA={lba:#x}");
        Ok(())
    }

    /// 部分flush：刷新指定数量的脏块（从LRU端开始）
    ///
    /// 这是主动flush机制的核心方法，用于防止cache被脏块填满。
    /// 当cache接近满或脏块比例过高时调用。
    ///
    /// # 参数
    ///
    /// * `count` - 要flush的块数量，如果脏块数量不足则flush所有脏块
    ///
    /// # 返回
    ///
    /// 返回实际flush的块数量
    ///
    /// # 性能优化
    ///
    /// 预分配缓冲区复用，避免每个脏块都分配新的 Vec
    pub fn flush_some_dirty_blocks(&mut self, count: usize) -> Result<usize> {
        // 先获取必要参数，避免借用冲突
        let sector_size = self.device.sector_size();
        let partition_offset = self.partition_offset;
        let block_size = self.block_size();

        let to_flush = if let Some(cache) = &mut self.bcache {
            let dirty_blocks = cache.get_dirty_blocks();
            dirty_blocks.into_iter().take(count).collect::<alloc::vec::Vec<_>>()
        } else {
            return Ok(0);
        };

        let actual_count = to_flush.len();
        if actual_count > 0 {
            log::debug!("[BlockDev] Flushing {actual_count} dirty blocks (LRU)");

            // 🚀 性能优化：预分配缓冲区复用
            let mut flush_buf = alloc::vec![0u8; block_size as usize];

            for lba in to_flush {
                // 每次循环重新借用cache，复制数据到预分配的缓冲区
                let has_data = if let Some(cache) = &self.bcache {
                    if let Some(data) = cache.get_block_data(lba) {
                        flush_buf[..data.len()].copy_from_slice(data);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                if !has_data {
                    continue;
                }

                // 进行I/O（此时没有cache借用）
                let pba = (lba * block_size as u64 + partition_offset) / sector_size as u64;
                let sector_count = (block_size as usize).div_ceil(sector_size as usize);
                self.inc_physical_write_count();
                self.device_mut().write_blocks(pba, sector_count as u32, &flush_buf)?;

                // 标记clean
                if let Some(cache) = &mut self.bcache {
                    cache.mark_clean(lba)?;
                }
            }

            log::debug!("[BlockDev] Flushed {actual_count} blocks successfully");
        }

        Ok(actual_count)
    }

    // ===== 缓存管理接口 =====

    /// 获取缓存统计信息
    ///
    /// # 返回
    ///
    /// 如果启用了缓存，返回 Some(CacheStats)，否则返回 None
    pub fn cache_stats(&self) -> Option<crate::cache::CacheStats> {
        self.bcache.as_ref().map(|cache| cache.stats())
    }

    /// 检查是否启用了缓存
    pub fn has_cache(&self) -> bool {
        self.bcache.is_some()
    }

    /// 使块缓存失效（从缓存中移除）
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    ///
    /// # 返回
    ///
    /// 成功返回 Ok(())
    pub fn invalidate_cache_block(&mut self, lba: u64) -> Result<()> {
        if let Some(cache) = &mut self.bcache {
            cache.invalidate_buffer(lba)?;
        }
        Ok(())
    }

    /// 使一组连续块的缓存失效
    ///
    /// # 参数
    ///
    /// * `from` - 起始逻辑块地址
    /// * `count` - 块数量
    ///
    /// # 返回
    ///
    /// 成功返回失效的块数量
    pub fn invalidate_cache_range(&mut self, from: u64, count: u32) -> Result<usize> {
        if let Some(cache) = &mut self.bcache {
            return cache.invalidate_range(from, count);
        }
        Ok(0)
    }

    // ===== 写回模式控制 =====

    /// 启用缓存写回模式
    ///
    /// 对应 lwext4 的 `ext4_block_cache_write_back(bdev, 1)`
    ///
    /// 启用后，修改的块会保留在缓存中，直到显式刷新或驱逐。
    /// 可以嵌套调用以实现引用计数式的写回控制。
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// // 开始批量写操作
    /// block_dev.enable_write_back();
    ///
    /// // 执行多次写操作（延迟写入磁盘）
    /// block_dev.write_block(0, &data1)?;
    /// block_dev.write_block(1, &data2)?;
    ///
    /// // 结束批量操作，刷新所有脏块
    /// block_dev.disable_write_back()?;
    /// ```
    pub fn enable_write_back(&mut self) {
        if let Some(cache) = &mut self.bcache {
            cache.enable_write_back();
        }
    }

    /// 禁用缓存写回模式
    ///
    /// 对应 lwext4 的 `ext4_block_cache_write_back(bdev, 0)`
    ///
    /// 如果引用计数降为 0，会自动刷新所有脏块到设备。
    ///
    /// # 返回
    ///
    /// 成功返回刷新的块数量，如果仍处于写回模式则返回 0
    pub fn disable_write_back(&mut self) -> Result<usize> {
        if let Some(cache) = &mut self.bcache {
            let sector_size = self.device.sector_size();
            let partition_offset = self.partition_offset;
            return cache.disable_write_back(&mut self.device, sector_size, partition_offset);
        }
        Ok(0)
    }

    /// 检查是否启用写回模式
    pub fn is_write_back_enabled(&self) -> bool {
        self.bcache
            .as_ref()
            .map(|cache| cache.is_write_back_enabled())
            .unwrap_or(false)
    }

    /// 获取写回模式引用计数
    pub fn write_back_counter(&self) -> u32 {
        self.bcache
            .as_ref()
            .map(|cache| cache.write_back_counter())
            .unwrap_or(0)
    }

    /// 打开底层设备
    ///
    /// 调用底层设备的 `open()` 方法进行初始化。
    /// 对应 lwext4 的 `ext4_block_init`
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let mut block_dev = BlockDev::new(device)?;
    /// block_dev.open()?; // 初始化设备资源
    /// // ... 使用设备 ...
    /// block_dev.close()?; // 清理设备资源
    /// ```
    pub fn open(&mut self) -> Result<()> {
        self.device.open()
    }

    /// 关闭底层设备
    ///
    /// 先刷新所有缓存，然后调用底层设备的 `close()` 方法。
    /// 对应 lwext4 的 `ext4_block_fini`
    ///
    /// # 返回
    ///
    /// 如果刷新或关闭失败则返回错误
    pub fn close(&mut self) -> Result<()> {
        // 先刷新所有数据
        self.flush()?;
        // 然后关闭设备
        self.device.close()
    }

    /// 增加引用计数
    ///
    /// 对应 lwext4 的内部引用计数管理。
    /// 当有新的组件开始使用此设备时调用。
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// block_dev.get(); // 增加引用计数
    /// // ... 使用设备 ...
    /// block_dev.put(); // 减少引用计数
    /// ```
    pub fn get(&mut self) {
        self.ref_count = self.ref_count.saturating_add(1);
    }

    /// 减少引用计数
    ///
    /// 当组件停止使用此设备时调用。
    /// 使用饱和减法，计数不会低于 0。
    pub fn put(&mut self) {
        self.ref_count = self.ref_count.saturating_sub(1);
    }

    /// 获取当前引用计数
    ///
    /// 返回值表示当前有多少组件正在使用此设备。
    pub fn ref_count(&self) -> u32 {
        self.ref_count
    }

    /// 检查设备是否正在被引用
    ///
    /// 如果引用计数大于 0，返回 true
    pub fn is_referenced(&self) -> bool {
        self.ref_count > 0
    }
}

/// Drop实现：在BlockDev销毁时自动flush所有脏块
///
/// **重要**：这是防止数据丢失的关键机制。
/// 当BlockDev超出作用域（文件系统unmount、程序退出等），
/// 自动flush所有缓存中的脏块到磁盘。
///
/// 这个实现使用重构后的架构：BlockDev::flush()协调I/O操作
impl<D: BlockDevice> Drop for BlockDev<D> {
    fn drop(&mut self) {
        if let Some(cache) = &self.bcache {
            let dirty_count = cache.dirty_count();
            if dirty_count > 0 {
                log::warn!(
                    "[BlockDev] Dropping with {dirty_count} dirty blocks, flushing..."
                );

                // 使用重构后的flush方法
                match self.flush() {
                    Ok(()) => {
                        log::info!("[BlockDev] Drop: successfully flushed all dirty blocks");
                    }
                    Err(e) => {
                        log::error!("[BlockDev] Drop: failed to flush cache: {e:?}");
                    }
                }
            }
        }
    }
}
