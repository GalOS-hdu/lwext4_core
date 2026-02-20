//! 块 I/O 操作实现

use super::{BlockDev, BlockDevice};
use crate::error::{Error, ErrorKind, Result};
use alloc::vec;

impl<D: BlockDevice> BlockDev<D> {
    /// 读取单个逻辑块
    ///
    /// 从指定逻辑块地址读取一个完整的块到缓冲区。
    /// 如果启用了缓存，优先从缓存读取；缓存未命中则从设备读取并填充缓存。
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    /// * `buf` - 目标缓冲区（大小至少为 block_size）
    ///
    /// # 返回
    ///
    /// 成功返回读取的字节数
    pub fn read_block(&mut self, lba: u64, buf: &mut [u8]) -> Result<usize> {
        let block_size = self.device().block_size();

        if buf.len() < block_size as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "buffer too small for block",
            ));
        }

        self.inc_read_count();

        // 如果启用了缓存，尝试从缓存读取
        let cache_miss = if let Some(cache) = &self.bcache {
            // 尝试从缓存读取（只读检查）
            match cache.read_block(lba) {
                Ok(data) => {
                    // 缓存命中
                    buf[..data.len()].copy_from_slice(data);
                    return Ok(data.len());
                }
                Err(_) => true, // 缓存未命中
            }
        } else {
            false // 无缓存
        };

        if cache_miss {
            // 缓存未命中 - 从设备读取到用户缓冲区
            let pba = self.logical_to_physical(lba);
            let count = self.sectors_per_block();
            self.device_mut().read_blocks(pba, count, buf)?;

            // 将数据填充到缓存
            if let Some(cache) = &mut self.bcache {
                // 使用主动flush机制
                let result = cache.alloc(lba);
                let (cache_buf, _is_new) = match result {
                    Ok(result) => result,
                    Err(e) if e.kind() == crate::error::ErrorKind::NoSpace => {
                        // 先获取capacity，然后释放借用
                        let flush_count = cache.capacity() / 4;
                        // prepare for contest replace warn with info
                        log::info!("[read_block] Cache full, flushing {flush_count} blocks");
                        self.flush_some_dirty_blocks(flush_count)?;
                        // 重新借用并重试
                        self.bcache.as_mut().unwrap().alloc(lba)?
                    }
                    Err(e) => return Err(e),
                };
                cache_buf.data.copy_from_slice(&buf[..block_size as usize]);
                cache_buf.mark_uptodate();
                // ✅ lru crate 自动管理生命周期，无需手动 free
            }

            return Ok(block_size as usize);
        }

        // 无缓存 - 直接从设备读取
        let pba = self.logical_to_physical(lba);
        let count = self.sectors_per_block();
        self.device_mut().read_blocks(pba, count, buf)
    }

    /// 写入单个逻辑块
    ///
    /// 将缓冲区数据写入指定逻辑块地址。
    /// 如果启用了缓存，写入缓存并标记为脏；否则直接写入设备。
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    /// * `buf` - 源数据缓冲区（大小至少为 block_size）
    ///
    /// # 返回
    ///
    /// 成功返回写入的字节数
    pub fn write_block(&mut self, lba: u64, buf: &[u8]) -> Result<usize> {
        let block_size = self.device().block_size();

        if buf.len() < block_size as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "buffer too small for block",
            ));
        }

        self.inc_write_count();

        // 如果启用了缓存，写入缓存
        if let Some(cache) = &mut self.bcache {
            // 先尝试从缓存获取块（可能已存在）
            match cache.write_block(lba, buf) {
                Ok(n) => {
                    // 块已在缓存中，写入成功
                    return Ok(n);
                }
                Err(_) => {
                    // 块不在缓存中 - 分配新块并写入
                    // 使用主动flush机制
                    // flush的工作现在在device中完成，cache只需要负责缓存管理
                    let result = cache.alloc(lba);
                    let (cache_buf, _is_new) = match result {
                        Ok(result) => result,
                        Err(e) if e.kind() == crate::error::ErrorKind::NoSpace => {
                            let flush_count = cache.capacity() / 4;
                            // prepare for contest replace warn with info
                            log::info!("[write_block] Cache full, flushing {flush_count} blocks");
                            self.flush_some_dirty_blocks(flush_count)?;
                            // 重新获取cache并分配
                            let cache = self.bcache.as_mut().unwrap();
                            cache.alloc(lba)?
                        }
                        Err(e) => return Err(e),
                    };
                    cache_buf.data[..buf.len()].copy_from_slice(buf);
                    cache_buf.mark_uptodate();
                    cache_buf.mark_dirty();

                    // 将块加入脏列表（需要重新借用cache，因为可能经过了drop）
                    if let Some(cache) = &mut self.bcache {
                        cache.mark_dirty(lba)?;
                    }

                    // ✅ lru crate 自动管理生命周期，无需手动 free

                    return Ok(buf.len());
                }
            }
        }

        // 无缓存 - 直接写入设备
        let pba = self.logical_to_physical(lba);
        let count = self.sectors_per_block();
        self.device_mut().write_blocks(pba, count, buf)
    }

    /// 读取字节
    ///
    /// 从任意字节偏移读取，自动处理跨块情况。
    ///
    /// # 参数
    ///
    /// * `offset` - 字节偏移量
    /// * `buf` - 目标缓冲区
    ///
    /// # 返回
    ///
    /// 成功返回读取的字节数
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let mut buf = vec![0u8; 100];
    /// block_dev.read_bytes(1024, &mut buf)?;
    /// ```
    pub fn read_bytes(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let len = buf.len();
        let block_size = self.device().block_size() as u64;

        // 计算起始块和块内偏移
        let start_block = offset / block_size;
        let block_offset = (offset % block_size) as usize;

        // 计算需要读取的块数
        let total_size = block_offset + len;
        let block_count = (total_size as u64).div_ceil(block_size) as usize;

        // 分配临时缓冲区
        let mut temp = vec![0u8; block_count * block_size as usize];

        // 读取所有相关块
        for i in 0..block_count {
            let lba = start_block + i as u64;
            let block_buf = &mut temp[i * block_size as usize..(i + 1) * block_size as usize];
            self.read_block(lba, block_buf)?;
        }

        // 复制所需字节
        buf.copy_from_slice(&temp[block_offset..block_offset + len]);

        Ok(len)
    }

    /// 写入字节
    ///
    /// 向任意字节偏移写入，自动处理跨块情况。
    ///
    /// # 参数
    ///
    /// * `offset` - 字节偏移量
    /// * `buf` - 源数据缓冲区
    ///
    /// # 返回
    ///
    /// 成功返回写入的字节数
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let data = b"Hello, ext4!";
    /// block_dev.write_bytes(1024, data)?;
    /// ```
    pub fn write_bytes(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
        let len = buf.len();
        let block_size = self.device().block_size() as u64;

        let start_block = offset / block_size;
        let block_offset = (offset % block_size) as usize;

        let total_size = block_offset + len;
        let block_count = (total_size as u64).div_ceil(block_size) as usize;

        let mut temp = vec![0u8; block_count * block_size as usize];

        // 如果不是块对齐，需要先读取现有数据
        if block_offset != 0 || len % block_size as usize != 0 {
            for i in 0..block_count {
                let lba = start_block + i as u64;
                let block_buf =
                    &mut temp[i * block_size as usize..(i + 1) * block_size as usize];
                // 忽略读取错误（可能是新块）
                let _ = self.read_block(lba, block_buf);
            }
        }

        // 写入数据到临时缓冲区
        temp[block_offset..block_offset + len].copy_from_slice(buf);

        // 写回所有块
        for i in 0..block_count {
            let lba = start_block + i as u64;
            let block_buf = &temp[i * block_size as usize..(i + 1) * block_size as usize];
            self.write_block(lba, block_buf)?;
        }

        Ok(len)
    }

    /// 刷新所有缓存
    ///
    /// 如果启用了缓存，先刷新所有脏块到设备，然后调用设备的 flush。
    /// 这是两层刷新：缓存层和硬件层。
    /// 刷新所有缓存的脏块到磁盘
    ///
    /// 这是架构重构后的新实现：
    /// - BlockCache提供脏块列表和数据
    /// - BlockDev负责实际的I/O操作
    /// - 职责清晰，无借用冲突
    ///
    /// # 性能优化
    ///
    /// - 预分配缓冲区复用，避免每个脏块都分配新的 Vec
    /// - 检测连续脏块，批量写入
    pub fn flush(&mut self) -> Result<()> {
        // 第一层：刷新缓存中的脏块
        // 先获取必要的参数（避免借用冲突）
        let sector_size = self.device().sector_size();
        let partition_offset = self.partition_offset();
        let block_size = self.block_size();

        let dirty_blocks = if let Some(cache) = &mut self.bcache {
            cache.get_dirty_blocks()
        } else {
            alloc::vec::Vec::new()
        };

        let dirty_count = dirty_blocks.len();
        if dirty_count > 0 {
            log::debug!("[BlockDev] Flushing {dirty_count} dirty blocks");

            // 🚀 性能优化：预分配缓冲区复用（避免每次循环分配新 Vec）
            let mut flush_buf = vec![0u8; block_size as usize];

            // 逐个flush脏块
            for lba in dirty_blocks {
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

                // 进行I/O操作（此时没有cache借用）
                let pba = (lba * block_size as u64 + partition_offset) / sector_size as u64;
                let count = (block_size as usize).div_ceil(sector_size as usize);
                self.device_mut().write_blocks(pba, count as u32, &flush_buf)?;

                // 标记为clean
                if let Some(cache) = &mut self.bcache {
                    cache.mark_clean(lba)?;
                }
            }

            log::debug!("[BlockDev] Flushed {dirty_count} blocks successfully");
        }

        // 第二层：调用设备的硬件刷新（如 fsync）
        self.device_mut().flush()
    }
}
