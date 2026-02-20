//! 块句柄 - RAII 风格的块访问
//!
//! 对应 lwext4 的 `ext4_block` API

use crate::error::{Error, ErrorKind, Result};
use crate::block::{BlockDevice, BlockDev};

/// 块句柄
///
/// 对应 lwext4 的 `struct ext4_block`
///
/// 提供 RAII 风格的块访问：
/// - 获取时自动从缓存分配或从磁盘读取
/// - 在持有期间，由于持有 `&mut BlockDev`，保证缓存块不会被其他操作访问
/// - 修改时自动标记为脏
/// - 丢弃时释放对 BlockDev 的可变引用，lru crate 自动管理块生命周期
/// - 一般用block获得某个缓存块的引用， 使用闭包操作缓存块中的数据
/// - 由于block需要持有device的mut引用， 同一时刻应该只有一个block存在
/// # 设计说明
///
/// Block 直接操作缓存中的块，而不是持有数据副本。这保证了：
/// 1. **同步性**: 所有修改都直接作用于缓存，不会出现多个副本不一致
/// 2. **性能**: 避免 4KB 数据的复制开销
/// 3. **正确语义**: `get_noread` 在缓存中预留槽位，不读取磁盘
///
/// 对于无缓存的情况，Block 会退化为持有本地数据副本。
///
/// # 示例
///
/// ```rust,ignore
/// // 读取块
/// let block = Block::get(&mut block_dev, 0)?;
/// block.with_data(|data| {
///     println!("First byte: {:02x}", data[0]);
/// })?;
///
/// // 修改块
/// let mut block = Block::get(&mut block_dev, 1)?;
/// block.with_data_mut(|data| {
///     data[0] = 0x42;
/// })?;
/// // block 超出作用域时自动释放引用，脏块最终会写回
///
/// // 新块（不从磁盘读取）
/// let mut block = Block::get_noread(&mut block_dev, 10)?;
/// block.with_data_mut(|data| {
///     data.fill(0xFF);
/// })?;
/// ```
pub struct Block<'a, D: BlockDevice> {
    /// 块设备引用
    block_dev: &'a mut BlockDev<D>,
    /// 逻辑块地址
    lba: u64,
    /// 是否持有缓存块引用（需要在 drop 时释放）
    held: bool,
    /// 本地数据副本（仅在无缓存时使用）
    local_data: Option<alloc::vec::Vec<u8>>,
    /// 本地脏标志（仅在无缓存时使用）
    local_dirty: bool,
}

impl<'a, D: BlockDevice> Block<'a, D> {
    /// 获取块（读取数据）
    ///
    /// 对应 lwext4 的 `ext4_block_get()`
    ///
    /// # 缓存路径
    ///
    /// 1. 调用 `cache.alloc(lba)` 在缓存中分配块
    ///    - 如果块已存在：返回现有块的可变引用
    ///    - 如果块不存在：分配新槽位（可能驱逐 LRU 块）
    /// 2. 如果是新分配的块，从磁盘读取数据到缓存块
    /// 3. Block 持有 `&mut BlockDev`，保证块在使用期间不被其他操作访问
    /// 4. Drop 时释放可变引用，lru crate 自动管理块生命周期
    ///
    /// # 无缓存路径
    ///
    /// 如果未启用缓存，从磁盘读取到本地 Vec<u8>
    ///
    /// # 参数
    ///
    /// * `block_dev` - 块设备
    /// * `lba` - 逻辑块地址
    pub fn get(block_dev: &'a mut BlockDev<D>, lba: u64) -> Result<Self> {
        let block_size = block_dev.block_size() as usize;

        // 先获取需要的值，避免借用冲突
        let pba = block_dev.logical_to_physical(lba);
        let count = block_dev.sectors_per_block();

        if let Some(cache) = &mut block_dev.bcache {
            // 有缓存：在缓存中分配块
            // 使用主动flush机制：如果alloc失败（NoSpace），先flush一些脏块再重试
            let (_cache_buf, is_new) = match cache.alloc(lba) {
                Ok(result) => result,
                Err(e) if e.kind() == crate::error::ErrorKind::NoSpace => {
                    // Cache满且都是脏块 - 主动flush后重试
                    let flush_count = cache.capacity() / 4;
                    // prepare for contest replace warn with info
                    log::info!("[Block::get] Cache full with dirty blocks, flushing {flush_count} blocks");

                    // Flush 25%的cache容量
                    block_dev.flush_some_dirty_blocks(flush_count)?;

                    // 重试alloc
                    block_dev.bcache.as_mut().unwrap().alloc(lba)?
                }
                Err(e) => return Err(e),
            };

            if is_new {
                // 新分配的块，需要从磁盘读取
                // ⚠️ 解决借用冲突：先读取到临时缓冲区，然后重新获取 cache 引用填充数据
                // 第一次 alloc 的引用在调用 device_mut() 前必须结束，否则会有借用冲突

                // 先读取数据到临时缓冲区
                block_dev.inc_physical_read_count();
                let mut temp_buf = alloc::vec![0u8; block_size];
                block_dev.device_mut().read_blocks(pba, count, &mut temp_buf)?;

                // 重新获取缓存块引用并填充数据
                let (cache_buf, _) = block_dev.bcache.as_mut().unwrap().alloc(lba)?;
                cache_buf.data.copy_from_slice(&temp_buf);
                cache_buf.mark_uptodate();
            }

            // Block 持有 &mut BlockDev，保证缓存块不被其他操作访问

            Ok(Self {
                block_dev,
                lba,
                held: true,
                local_data: None,
                local_dirty: false,
            })
        } else {
            // 无缓存：读取到本地副本
            let mut data = alloc::vec![0u8; block_size];
            block_dev.read_block(lba, &mut data)?;

            Ok(Self {
                block_dev,
                lba,
                held: false,
                local_data: Some(data),
                local_dirty: false,
            })
        }
    }

    /// 获取块（不读取数据）
    ///
    /// 对应 lwext4 的 `ext4_block_get_noread()`
    ///
    /// # 语义
    ///
    /// "在缓存池中给我预留一个位置（Slot），标记属于 LBA=X。
    /// 因为我马上要覆盖整个块，所以不需要浪费时间把磁盘上的旧数据读进来。"
    ///
    /// # 缓存路径
    ///
    /// 1. 调用 `cache.alloc(lba)` 在缓存中分配块
    /// 2. 如果是新块，**不从磁盘读取**
    /// 3. 标记为 `uptodate`（因为调用者会立即覆盖）
    /// 4. Block 持有 `&mut BlockDev`，drop 时释放，lru crate 自动管理
    ///
    /// # 无缓存路径
    ///
    /// 分配一个全零的本地 Vec<u8>
    ///
    /// # 参数
    ///
    /// * `block_dev` - 块设备
    /// * `lba` - 逻辑块地址
    pub fn get_noread(block_dev: &'a mut BlockDev<D>, lba: u64) -> Result<Self> {
        let block_size = block_dev.block_size() as usize;

        if let Some(cache) = &mut block_dev.bcache {
            // 有缓存：在缓存中分配块，但不读取磁盘
            // 使用主动flush机制
            let (cache_buf, _is_new) = match cache.alloc(lba) {
                Ok(result) => result,
                Err(e) if e.kind() == crate::error::ErrorKind::NoSpace => {
                    let flush_count = cache.capacity() / 4;
                    // prepare for contest replace warn with info
                    log::info!("[Block::get_noread] Cache full, flushing {flush_count} blocks");
                    block_dev.flush_some_dirty_blocks(flush_count)?;
                    block_dev.bcache.as_mut().unwrap().alloc(lba)?
                }
                Err(e) => return Err(e),
            };

            // 不管是新块还是已存在，都标记为 uptodate
            // 因为调用者会立即覆盖整个块
            cache_buf.mark_uptodate();

            Ok(Self {
                block_dev,
                lba,
                held: true,
                local_data: None,
                local_dirty: false,
            })
        } else {
            // 无缓存：分配全零的本地副本
            let data = alloc::vec![0u8; block_size];

            Ok(Self {
                block_dev,
                lba,
                held: false,
                local_data: Some(data),
                local_dirty: false,
            })
        }
    }

    /// 获取逻辑块地址
    pub fn lba(&self) -> u64 {
        self.lba
    }

    /// 访问块数据（只读）
    ///
    /// 通过闭包访问块数据，避免生命周期问题。
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// block.with_data(|data| {
    ///     println!("First byte: {:02x}", data[0]);
    /// })?;
    /// ```
    pub fn with_data<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        if let Some(cache) = &mut self.block_dev.bcache {
            // 有缓存：临时获取缓存块引用
            // 使用主动flush机制
            let (cache_buf, _) = match cache.alloc(self.lba) {
                Ok(result) => result,
                Err(e) if e.kind() == crate::error::ErrorKind::NoSpace => {
                    let flush_count = cache.capacity() / 4;
                    // prepare for contest replace warn with info
                    log::info!("[Block::with_data] Cache full, flushing {flush_count} blocks");
                    self.block_dev.flush_some_dirty_blocks(flush_count)?;
                    self.block_dev.bcache.as_mut().unwrap().alloc(self.lba)?
                }
                Err(e) => return Err(e),
            };
            let result = f(&cache_buf.data);
            // ✅ lru crate 自动管理生命周期，无需手动 free
            Ok(result)
        } else if let Some(data) = &self.local_data {
            // 无缓存：使用本地副本
            Ok(f(data))
        } else {
            Err(Error::new(ErrorKind::InvalidInput, "Block not initialized"))
        }
    }

    /// 访问块数据（可写）
    ///
    /// 通过闭包修改块数据，自动标记为脏。
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// block.with_data_mut(|data| {
    ///     data[0] = 0x42;
    ///     data[1] = 0x43;
    /// })?;
    /// ```
    pub fn with_data_mut<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        if let Some(cache) = &mut self.block_dev.bcache {
            // 有缓存：临时获取缓存块可变引用
            // 使用主动flush机制
            let (cache_buf, _) = match cache.alloc(self.lba) {
                Ok(result) => result,
                Err(e) if e.kind() == crate::error::ErrorKind::NoSpace => {
                    let flush_count = cache.capacity() / 4;
                    // prepare for contest replace warn with info
                    log::info!("[Block::with_data_mut] Cache full, flushing {flush_count} blocks");
                    self.block_dev.flush_some_dirty_blocks(flush_count)?;
                    self.block_dev.bcache.as_mut().unwrap().alloc(self.lba)?
                }
                Err(e) => return Err(e),
            };
            let result = f(&mut cache_buf.data);
            // 标记为脏
            cache_buf.mark_dirty();
            // 将块加入脏列表（需要重新借用cache，因为可能经过了drop）
            if let Some(cache) = &mut self.block_dev.bcache {
                cache.mark_dirty(self.lba)?;
            }
            // ✅ lru crate 自动管理生命周期，无需手动 free
            // 脏块会在 dirty_set 中跟踪，flush 时会写回磁盘
            Ok(result)
        } else if let Some(data) = &mut self.local_data {
            // 无缓存：修改本地副本并标记为脏
            let result = f(data);
            self.local_dirty = true;
            Ok(result)
        } else {
            Err(Error::new(ErrorKind::InvalidInput, "Block not initialized"))
        }
    }

    /// 手动释放块（消费 self）
    ///
    /// 对应 lwext4 的 `ext4_block_set()`
    ///
    /// 通常不需要手动调用，Drop trait 会自动处理。
    pub fn release(mut self) -> Result<()> {
        self.do_release()
    }

    /// 实际的释放逻辑
    fn do_release(&mut self) -> Result<()> {
        if self.held {
            // 有缓存：lru crate 自动管理生命周期
            // Block 不再需要显式释放引用计数
            // 当 Block drop 时，对 BlockDev 的可变引用被释放，
            // 缓存块可以被后续操作访问或驱逐
            self.held = false;
        } else if self.local_dirty {
            // 无缓存且有修改：写回磁盘
            if let Some(data) = &self.local_data {
                self.block_dev.write_block(self.lba, data)?;
                self.local_dirty = false;
            }
        }
        Ok(())
    }
}

/// 实现 Drop trait，自动释放块
impl<'a, D: BlockDevice> Drop for Block<'a, D> {
    fn drop(&mut self) {
        // 忽略错误（drop 不能返回 Result）
        let _ = self.do_release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockDevice;
    use crate::error::Result;

    struct MockDevice {
        block_size: u32,
        sector_size: u32,
        total_blocks: u64,
        storage: alloc::vec::Vec<u8>,
    }

    impl MockDevice {
        fn new(total_blocks: u64) -> Self {
            let block_size = 4096;
            let sector_size = 512;
            let storage = alloc::vec![0u8; (total_blocks * block_size as u64) as usize];
            Self {
                block_size,
                sector_size,
                total_blocks,
                storage,
            }
        }
    }

    impl BlockDevice for MockDevice {
        fn block_size(&self) -> u32 {
            self.block_size
        }

        fn sector_size(&self) -> u32 {
            self.sector_size
        }

        fn total_blocks(&self) -> u64 {
            self.total_blocks
        }

        fn read_blocks(&mut self, lba: u64, count: u32, buf: &mut [u8]) -> Result<usize> {
            let start = (lba * self.sector_size as u64) as usize;
            let len = (count * self.sector_size) as usize;
            buf[..len].copy_from_slice(&self.storage[start..start + len]);
            Ok(len)
        }

        fn write_blocks(&mut self, lba: u64, count: u32, buf: &[u8]) -> Result<usize> {
            let start = (lba * self.sector_size as u64) as usize;
            let len = (count * self.sector_size) as usize;
            self.storage[start..start + len].copy_from_slice(&buf[..len]);
            Ok(len)
        }
    }

    #[test]
    fn test_block_get_with_cache() {
        let device = MockDevice::new(100);
        let mut block_dev = BlockDev::new_with_cache(device, 8).unwrap();

        // 获取块
        let mut block = Block::get(&mut block_dev, 0).unwrap();
        assert_eq!(block.lba(), 0);
        assert!(block.held);

        // 读取数据
        let result = block.with_data(|data| {
            assert_eq!(data.len(), 4096);
            data[0]
        }).unwrap();
        assert_eq!(result, 0);

        // 显式释放
        block.release().unwrap();
    }

    #[test]
    fn test_block_get_without_cache() {
        let device = MockDevice::new(100);
        let mut block_dev = BlockDev::new(device).unwrap();

        // 获取块（无缓存）
        let mut block = Block::get(&mut block_dev, 0).unwrap();
        assert_eq!(block.lba(), 0);
        assert!(!block.held);
        assert!(block.local_data.is_some());

        // 读取数据
        block.with_data(|data| {
            assert_eq!(data.len(), 4096);
        }).unwrap();
    }

    #[test]
    fn test_block_modify_with_cache() {
        let device = MockDevice::new(100);
        let mut block_dev = BlockDev::new_with_cache(device, 8).unwrap();

        // 修改块
        {
            let mut block = Block::get(&mut block_dev, 0).unwrap();
            block.with_data_mut(|data| {
                data[0] = 0x42;
                data[1] = 0x43;
            }).unwrap();
        } // 自动释放

        // 验证修改（应该还在缓存中）
        {
            let mut block = Block::get(&mut block_dev, 0).unwrap();
            block.with_data(|data| {
                assert_eq!(data[0], 0x42);
                assert_eq!(data[1], 0x43);
            }).unwrap();
        }
    }

    #[test]
    fn test_block_modify_without_cache() {
        let device = MockDevice::new(100);
        let mut block_dev = BlockDev::new(device).unwrap();

        // 修改块
        {
            let mut block = Block::get(&mut block_dev, 0).unwrap();
            block.with_data_mut(|data| {
                data[0] = 0xAA;
            }).unwrap();
        } // 自动写回

        // 验证修改
        {
            let mut block = Block::get(&mut block_dev, 0).unwrap();
            block.with_data(|data| {
                assert_eq!(data[0], 0xAA);
            }).unwrap();
        }
    }

    #[test]
    fn test_block_get_noread_with_cache() {
        let device = MockDevice::new(100);
        let mut block_dev = BlockDev::new_with_cache(device, 8).unwrap();

        // 获取新块（不读取磁盘）
        let mut block = Block::get_noread(&mut block_dev, 10).unwrap();
        assert_eq!(block.lba(), 10);
        assert!(block.held);

        // 覆盖整个块
        block.with_data_mut(|data| {
            data.fill(0xFF);
        }).unwrap();

        // 释放并重新读取
        block.release().unwrap();

        let mut block = Block::get(&mut block_dev, 10).unwrap();
        block.with_data(|data| {
            assert_eq!(data[0], 0xFF);
            assert_eq!(data[4095], 0xFF);
        }).unwrap();
    }

    #[test]
    fn test_block_sequential_access() {
        let device = MockDevice::new(100);
        let mut block_dev = BlockDev::new_with_cache(device, 8).unwrap();

        // 第一次访问
        {
            let mut block = Block::get(&mut block_dev, 0).unwrap();
            block.with_data_mut(|data| {
                data[0] = 0x99;
            }).unwrap();
        } // 释放，Block drop，&mut BlockDev 被释放

        // 第二次访问同一个块（应该还在缓存中）
        {
            let mut block = Block::get(&mut block_dev, 0).unwrap();
            block.with_data(|data| {
                assert_eq!(data[0], 0x99); // 验证数据还在缓存中
            }).unwrap();
        }

        // 注意：Rust 的借用检查器不允许同时持有多个 Block（因为都是 &mut BlockDev）
        // 这是正确的设计，确保同一时间只有一个代码路径在修改块设备
    }

    #[test]
    fn test_block_auto_drop() {
        let device = MockDevice::new(100);
        let mut block_dev = BlockDev::new_with_cache(device, 8).unwrap();

        {
            let _block = Block::get(&mut block_dev, 0).unwrap();
            // block 在这里自动 drop，释放 &mut BlockDev
        }

        // 验证可以再次获取
        let _block = Block::get(&mut block_dev, 0).unwrap();
    }
}
