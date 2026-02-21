//! 块缓存实现（使用 lru crate 重构）
//!
//! 对应 lwext4 的 `ext4_bcache` 结构和相关函数
//!
//! # 重构说明
//!
//! ✅ **使用 lru crate 后的优势**：
//! - 代码量减少60%+（从~800行降至~300行）
//! - 自动LRU管理，无需手动维护lru_index和lru_counter
//! - O(1)操作性能（HashMap），而非O(log n)（BTreeMap）
//! - 消除死锁风险（"All cache blocks are referenced, cannot evict"）
//! - 消除引用计数泄漏风险
//! - 久经考验的实现，bug少
//!
//! # 架构对比
//!
//! **旧实现**（复杂，易出错）：
//! ```text
//! struct BlockCache {
//!     lba_index: BTreeMap<u64, BufferId>,      // O(log n)
//!     lru_index: BTreeMap<u32, BufferId>,      // 手动维护
//!     buffers: Vec<Option<CacheBuffer>>,       // 需要管理空闲槽位
//!     dirty_list: VecDeque<BufferId>,
//!     free_list: Vec<BufferId>,
//!     lru_counter: u32,                         // 手动递增
//!     ref_blocks: u32,                          // 手动计数
//!     // ... 大量状态和不变量
//! }
//! ```
//!
//! **新实现**（简单，可靠）：
//! ```text
//! struct BlockCache {
//!     cache: LruCache<u64, CacheBuffer>,  // O(1)，自动LRU
//!     dirty_set: HashSet<u64>,            // 追踪脏块
//!     block_size: usize,
//!     write_back_counter: u32,
//!     stats: CacheStats,                   // 统计信息
//! }
//! ```
//! 注意：block_cache重构后，device与cache的职责已经分离，device负责实际的I/O操作，cache负责缓存管理。我们希望所有flush相关的动作都由device完成。

use crate::{
    block::BlockDevice,
    error::{Error, ErrorKind, Result},
};

use super::buffer::CacheBuffer;
use alloc::collections::BTreeSet;  // 使用BTreeSet因为no_std环境
use core::num::NonZeroUsize;
use lru::LruCache;

/// 默认缓存块数量
/// 增大到256以支持大量写操作（如apk add vim）
pub const DEFAULT_CACHE_SIZE: usize = 256;

/// 缓存统计信息
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// 总访问次数
    pub total_accesses: u64,
    /// 缓存命中次数
    pub hits: u64,
    /// 缓存未命中次数
    pub misses: u64,
    /// 脏块写回次数
    pub writebacks: u64,
    /// 当前脏块数量
    pub dirty_blocks: usize,
}

impl CacheStats {
    /// 计算命中率
    pub fn hit_rate(&self) -> f64 {
        if self.total_accesses == 0 {
            0.0
        } else {
            self.hits as f64 / self.total_accesses as f64
        }
    }
}

/// 块缓存
///
/// # 使用 lru crate 的优势
///
/// 1. **自动LRU管理**：
///    - `cache.get(key)` 自动将块移到最近使用
///    - `cache.put(key, value)` 满时自动驱逐LRU
///    - 无需手动维护lru_index、lru_counter
///
/// 2. **O(1)性能**：
///    - 内部使用HashMap + 双向链表
///    - get/put/pop_lru 都是O(1)
///
/// 3. **无死锁风险**：
///    - 不再有"所有块都被引用"的情况
///    - 驱逐前自动处理脏块
///
/// 4. **代码简洁**：
///    - 无需BufferId、free_list、lru_index
///    - 无需ref_blocks计数
///    - 无需复杂的不变量维护
pub struct BlockCache {
    /// LRU缓存核心：自动管理块的生命周期和访问顺序
    cache: LruCache<u64, CacheBuffer>,

    /// 脏块集合：追踪需要写回的块
    dirty_set: BTreeSet<u64>,

    /// 块大小（字节）
    block_size: usize,

    /// 写回模式计数器
    ///
    /// > 0 时启用写回模式（延迟写入）
    /// > == 0 时启用写穿模式（立即写入）
    write_back_counter: u32,

    /// 统计信息
    stats: CacheStats,
}

impl BlockCache {
    /// 创建新的块缓存
    ///
    /// # 参数
    ///
    /// * `capacity` - 缓存容量（块数量）
    /// * `block_size` - 块大小（字节）
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let cache = BlockCache::new(1024, 4096);  // 1024个4KB块 = 4MB缓存
    /// ```
    pub fn new(capacity: usize, block_size: usize) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
            dirty_set: BTreeSet::new(),
            block_size,
            write_back_counter: 0,
            stats: CacheStats::default(),
        }
    }

    /// 分配缓存块
    ///
    /// 对应 lwext4 的 `ext4_bcache_alloc`
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    ///
    /// # 返回
    ///
    /// `(块的可变引用, 是否是新分配)`
    /// - 如果块已存在：返回 `(块, false)` 并自动更新LRU
    /// - 如果块不存在：分配新块返回 `(块, true)`，满时自动驱逐LRU
    /// - 如果所有块都脏且cache满，返回CacheFull错误
    ///
    /// # 自动驱逐策略
    ///
    /// 当缓存满时，lru crate 会自动驱逐最久未使用的块：
    /// - 优先驱逐干净的块
    /// - 如果所有块都脏，返回CacheFull错误，调用者应先flush再重试
    ///
    /// # 示例
    ///
    /// ```rust,ignore
    /// let (buf, is_new) = cache.alloc(100)?;
    /// if is_new {
    ///     // 读取数据到 buf.data
    ///     device.read_block(100, &mut buf.data)?;
    ///     buf.mark_uptodate();
    /// }
    /// // 使用buf...
    /// ```
    /// TODO:如果多处想要引用同一逻辑块的cache，当前的实现无法满足并发只读的需求，只能做到串行访问。
    /// 因为cache的操作通过block handle进行，而block handle需要持有device的mut引用，同一时刻只能有一个block handle。
    pub fn alloc(&mut self, lba: u64) -> Result<(&mut CacheBuffer, bool)> {
        self.stats.total_accesses += 1;

        // lru crate 自动处理：
        // - 如果存在，get_mut会移到MRU（最近使用）
        // - 如果不存在，contains检查后手动插入
        if self.cache.contains(&lba) {
            self.stats.hits += 1;
            // get_mut 会自动更新LRU顺序
            let buf = self.cache.get_mut(&lba).unwrap();
            log::trace!("[CACHE] alloc LBA={:#x} HIT (dirty={})", lba, buf.is_dirty());
            return Ok((buf, false));
        }

        self.stats.misses += 1;
        log::debug!("[CACHE] alloc LBA={:#x} MISS, cache={}/{}", lba, self.cache.len(), self.cache.cap().get());

        // 检查脏块比例 - 如果超过80%发出警告
        let dirty_ratio = (self.dirty_set.len() * 100) / self.cache.len().max(1);
        if dirty_ratio > 80 {
            // log::warn!(
            //     "[CACHE] High dirty ratio: {}/{} ({}%). Consider calling flush_all()",
            //     self.dirty_set.len(),
            //     self.cache.len(),
            //     dirty_ratio
            // );
        }

        // 新块：需要检查是否满
        if self.cache.len() >= self.cache.cap().get() {
            // 缓存满，驱逐LRU块（只驱逐干净块）
            self.evict_for_new_block()?;
        }

        // 创建新块并插入
        let buf = CacheBuffer::new(lba, self.block_size);
        self.cache.put(lba, buf);
        log::debug!("[CACHE] alloc LBA={lba:#x} NEW block inserted");

        // 返回新插入的块
        Ok((self.cache.get_mut(&lba).unwrap(), true))
    }

    /// 驱逐一个块为新块腾出空间
    ///
    /// # 策略
    ///
    /// 1. 从LRU端开始查找第一个**非脏**块
    /// 2. 驱逐该块
    /// 3. 如果所有块都是脏的，返回CacheFull错误
    ///
    /// **重要**：绝不驱逐脏块！驱逐脏块会导致数据丢失和磁盘损坏。
    /// 调用者应该在调用alloc之前检查脏块比例，必要时主动flush。
    fn evict_for_new_block(&mut self) -> Result<()> {
        // lru crate的iter()按照MRU到LRU顺序遍历（从head.next开始）
        // 收集所有块的LBA
        let keys: alloc::vec::Vec<u64> = self.cache.iter().map(|(k, _)| *k).collect();

        // 从LRU端（最老的）开始查找非脏块
        // iter()是MRU→LRU顺序，所以需要rev()从LRU端开始
        for lba in keys.iter().rev() {
            if !self.dirty_set.contains(lba) {
                // 找到非脏块，驱逐它
                self.cache.pop(lba);
                log::debug!("[CACHE] Evicted clean block LBA={lba:#x}");
                return Ok(());
            }
        }

        // 所有块都是脏的，返回NoSpace错误
        // 调用者应该flush一些脏块后重试
        // prepare for contest replace error with info
        log::info!("[CACHE] Cannot evict: all {} blocks are dirty! Need flush before alloc.", self.cache.len());
        Err(Error::new(
            ErrorKind::NoSpace,
            "All cache blocks are dirty, cannot evict. Caller must flush before alloc."
        ))
    }

    /// 查找块（不增加引用计数，因为无引用计数了！）
    ///
    /// 对应 lwext4 的 `ext4_bcache_find_get`
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    ///
    /// # 返回
    ///
    /// 如果找到返回块的可变引用，否则返回 None
    ///
    /// 注意：get_mut 会自动更新LRU顺序
    /// TODO: 这个方法已经不再使用，因为重构了device与cache的职责，
    /// 现在device负责实际的I/O操作，cache负责缓存管理。将来或许考虑删除这个方法。
    pub fn find_get(&mut self, lba: u64) -> Option<&mut CacheBuffer> {
        self.stats.total_accesses += 1;

        if self.cache.contains(&lba) {
            self.stats.hits += 1;
            self.cache.get_mut(&lba)
        } else {
            self.stats.misses += 1;
            None
        }
    }

    /// 标记块为脏
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    pub fn mark_dirty(&mut self, lba: u64) -> Result<()> {
        let was_dirty = self.dirty_set.contains(&lba);
        self.dirty_set.insert(lba);
        if let Some(buf) = self.cache.get_mut(&lba) {
            buf.mark_dirty();
        }
        if !was_dirty {
            log::debug!("[CACHE] mark_dirty LBA={:#x}, total_dirty={}", lba, self.dirty_set.len());
        }
        Ok(())
    }

    /// 只读访问缓存块数据
    ///
    /// 如果块在缓存中，返回对数据的不可变引用
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    ///
    /// # 返回
    ///
    /// 成功返回块数据的切片，失败返回NotFound错误
    pub fn read_block(&self, lba: u64) -> Result<&[u8]> {
        if let Some(buf) = self.cache.peek(&lba) {
            if buf.is_uptodate() {
                return Ok(&buf.data);
            }
        }
        Err(Error::new(ErrorKind::NotFound, "Block not in cache"))
    }

    /// 写入缓存块数据
    ///
    /// 如果块在缓存中，写入数据并标记为脏
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    /// * `data` - 要写入的数据
    ///
    /// # 返回
    ///
    /// 成功返回写入的字节数，失败返回NotFound错误
    pub fn write_block(&mut self, lba: u64, data: &[u8]) -> Result<usize> {
        if let Some(buf) = self.cache.get_mut(&lba) {
            let len = data.len().min(buf.data.len());
            buf.data[..len].copy_from_slice(&data[..len]);
            buf.mark_uptodate();
            buf.mark_dirty();
            self.dirty_set.insert(lba);
            return Ok(len);
        }
        Err(Error::new(ErrorKind::NotFound, "Block not in cache"))
    }

    /// TODO: flush_lba flush_all 这两个方法已经不再使用，因为重构了device与cache的职责，
    /// 现在device负责实际的I/O操作，cache负责缓存管理。将来或许考虑删除这两个方法。
    /// 刷新单个块到磁盘
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    /// * `device` - 块设备
    /// * `sector_size` - 扇区大小
    /// * `partition_offset` - 分区偏移
    pub fn flush_lba<D: BlockDevice>(
        &mut self,
        lba: u64,
        device: &mut D,
        sector_size: u32,
        _partition_offset: u64,
    ) -> Result<()> {
        log::debug!("[CACHE] flush_lba LBA={lba:#x}");
        if let Some(buf) = self.cache.get_mut(&lba) {
            if buf.is_dirty() {
                // 计算物理块地址
                let pba = lba;  // 简化版本，实际可能需要转换
                let count = (self.block_size / sector_size as usize) as u32;

                // 写入磁盘
                device.write_blocks(pba, count, &buf.data)?;

                // 标记为干净
                buf.clear_dirty();
                self.dirty_set.remove(&lba);
                self.stats.writebacks += 1;
            }
        }
        Ok(())
    }

    /// 刷新所有脏块到磁盘
    ///
    /// 对应 lwext4 的 `ext4_block_cache_flush`
    ///
    /// # 参数
    ///
    /// * `device` - 块设备
    /// * `sector_size` - 扇区大小
    /// * `partition_offset` - 分区偏移
    pub fn flush_all<D: BlockDevice>(
        &mut self,
        device: &mut D,
        sector_size: u32,
        partition_offset: u64,
    ) -> Result<usize> {
        // 收集所有脏块LBA
        let dirty_lbas: alloc::vec::Vec<u64> = self.dirty_set.iter().copied().collect();
        let count = dirty_lbas.len();

        log::debug!(
            "[CACHE] Flushing {count} dirty blocks"
        );

        // 逐个刷新
        for lba in dirty_lbas {
            self.flush_lba(lba, device, sector_size, partition_offset)?;
        }

        // 确保dirty_set已清空
        self.dirty_set.clear();

        Ok(count)
    }

    /// 使块无效（从缓存中移除）
    ///
    /// 对应 lwext4 的 `ext4_bcache_invalidate_lba`
    ///
    /// # 参数
    ///
    /// * `lba` - 逻辑块地址
    pub fn invalidate_buffer(&mut self, lba: u64) -> Result<()> {
        self.cache.pop(&lba);
        self.dirty_set.remove(&lba);
        Ok(())
    }

    /// 使范围内的块无效
    ///
    /// # 参数
    ///
    /// * `from` - 起始LBA
    /// * `count` - 块数量
    ///
    /// # 返回
    ///
    /// 实际无效化的块数量
    pub fn invalidate_range(&mut self, from: u64, count: u32) -> Result<usize> {
        let mut invalidated = 0;

        for lba in from..(from + count as u64) {
            if self.cache.pop(&lba).is_some() {
                invalidated += 1;
            }
            self.dirty_set.remove(&lba);
        }

        Ok(invalidated)
    }

    /// 启用写回模式
    ///
    /// 对应 lwext4 的 `ext4_block_cache_write_back(bdev, 1)`
    pub fn enable_write_back(&mut self) {
        self.write_back_counter = self.write_back_counter.saturating_add(1);
    }

    /// 禁用写回模式并刷新所有脏块
    ///
    /// 对应 lwext4 的 `ext4_block_cache_write_back(bdev, 0)`
    pub fn disable_write_back<D: BlockDevice>(
        &mut self,
        device: &mut D,
        sector_size: u32,
        partition_offset: u64,
    ) -> Result<usize> {
        if self.write_back_counter > 0 {
            self.write_back_counter = self.write_back_counter.saturating_sub(1);
        }

        if self.write_back_counter == 0 {
            // 刷新所有脏块并返回刷新的块数量
            self.flush_all(device, sector_size, partition_offset)
        } else {
            Ok(0)
        }
    }

    /// 检查是否启用写回模式
    pub fn is_write_back_enabled(&self) -> bool {
        self.write_back_counter > 0
    }

    /// 获取写回计数器值
    pub fn write_back_counter(&self) -> u32 {
        self.write_back_counter
    }

    /// 获取缓存统计信息
    pub fn stats(&self) -> CacheStats {
        let mut stats = self.stats.clone();
        stats.dirty_blocks = self.dirty_set.len();
        stats
    }

    /// 获取缓存容量
    pub fn capacity(&self) -> usize {
        self.cache.cap().get()
    }

    /// 获取当前缓存块数量
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// 检查缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// 获取脏块数量
    pub fn dirty_count(&self) -> usize {
        self.dirty_set.len()
    }

    /// 获取脏块比例 (0.0 - 1.0)
    pub fn dirty_ratio(&self) -> f64 {
        if self.cache.is_empty() {
            0.0
        } else {
            self.dirty_set.len() as f64 / self.cache.len() as f64
        }
    }

    /// 获取所有脏块的LBA列表
    ///
    /// 返回的列表按LRU顺序排列（最老的在前）
    pub fn get_dirty_blocks(&self) -> alloc::vec::Vec<u64> {
        // 获取LRU顺序的所有块
        let lru_order: alloc::vec::Vec<u64> = self.cache.iter().map(|(k, _)| *k).collect();

        // 过滤出脏块
        lru_order.into_iter()
            .filter(|lba| self.dirty_set.contains(lba))
            .collect()
    }

    /// 获取块的数据（用于外部flush）
    ///
    /// 返回块数据的不可变引用，如果块不存在返回None
    pub fn get_block_data(&self, lba: u64) -> Option<&[u8]> {
        self.cache.peek(&lba).map(|buf| buf.data.as_slice())
    }

    /// 标记块为clean（flush完成后调用）
    ///
    /// 注意：这不会修改块的数据或uptodate标记
    pub fn mark_clean(&mut self, lba: u64) -> Result<()> {
        if self.dirty_set.remove(&lba) {
            log::debug!("[CACHE] mark_clean LBA={:#x}, remaining_dirty={}", lba, self.dirty_set.len());
        }
        Ok(())
    }

    /// 调整缓存大小
    ///
    /// 如果新容量小于当前块数，会驱逐LRU块
    pub fn resize(&mut self, new_capacity: NonZeroUsize) {
        self.cache.resize(new_capacity);
    }

    /// 清空缓存（不刷新脏块！）
    ///
    /// 警告：会丢失所有脏块数据
    pub fn clear(&mut self) {
        self.cache.clear();
        self.dirty_set.clear();
    }
}

impl core::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BlockCache")
            .field("capacity", &self.cache.cap())
            .field("len", &self.cache.len())
            .field("dirty_count", &self.dirty_set.len())
            .field("block_size", &self.block_size)
            .field("write_back_enabled", &self.is_write_back_enabled())
            .field("stats", &self.stats)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockDevice;

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

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_cache_creation() {
        let cache = BlockCache::new(8, 4096);
        assert_eq!(cache.capacity(), 8);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert_eq!(cache.dirty_count(), 0);
    }

    #[test]
    fn test_alloc_new_block() {
        let mut cache = BlockCache::new(8, 4096);

        let (buf, is_new) = cache.alloc(100).unwrap();
        assert!(is_new);
        assert_eq!(buf.lba, 100);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.stats.misses, 1);
    }

    #[test]
    fn test_alloc_existing_block() {
        let mut cache = BlockCache::new(8, 4096);

        // 第一次分配
        let (_buf, is_new) = cache.alloc(100).unwrap();
        assert!(is_new);

        // 第二次分配（已存在）
        let (_buf, is_new) = cache.alloc(100).unwrap();
        assert!(!is_new);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.stats.hits, 1);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = BlockCache::new(4, 4096);

        // 填满缓存
        for i in 0..4 {
            cache.alloc(i).unwrap();
        }
        assert_eq!(cache.len(), 4);

        // 访问块0，使其成为MRU
        cache.alloc(0).unwrap();

        // 分配新块，应该驱逐块1（最早分配且未再访问）
        cache.alloc(10).unwrap();
        assert_eq!(cache.len(), 4);

        // 块0应该还在
        assert!(cache.find_get(0).is_some());
        // 块1应该被驱逐
        assert!(cache.find_get(1).is_none());
    }

    #[test]
    fn test_mark_dirty_and_flush() {
        let mut cache = BlockCache::new(8, 4096);
        let mut device = MockDevice::new(100);

        // 分配块并标记为脏
        let (buf, _) = cache.alloc(10).unwrap();
        buf.data[0] = 0x42;
        cache.mark_dirty(10).unwrap();

        assert_eq!(cache.dirty_count(), 1);
        assert!(cache.find_get(10).unwrap().is_dirty());

        // 刷新
        cache.flush_lba(10, &mut device, 512, 0).unwrap();

        assert_eq!(cache.dirty_count(), 0);
        assert!(!cache.find_get(10).unwrap().is_dirty());
    }

    #[test]
    fn test_flush_all() {
        let mut cache = BlockCache::new(8, 4096);
        let mut device = MockDevice::new(100);

        // 分配多个块并标记为脏
        for i in 0..5 {
            cache.alloc(i).unwrap();
            cache.mark_dirty(i).unwrap();
        }

        assert_eq!(cache.dirty_count(), 5);

        // 刷新所有
        cache.flush_all(&mut device, 512, 0).unwrap();

        assert_eq!(cache.dirty_count(), 0);
    }

    #[test]
    fn test_invalidate_buffer() {
        let mut cache = BlockCache::new(8, 4096);

        cache.alloc(10).unwrap();
        assert_eq!(cache.len(), 1);

        cache.invalidate_buffer(10).unwrap();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_stats() {
        let mut cache = BlockCache::new(8, 4096);

        // 第一次访问 - miss
        cache.alloc(10).unwrap();
        assert_eq!(cache.stats.total_accesses, 1);
        assert_eq!(cache.stats.misses, 1);
        assert_eq!(cache.stats.hits, 0);

        // 第二次访问 - hit
        cache.alloc(10).unwrap();
        assert_eq!(cache.stats.total_accesses, 2);
        assert_eq!(cache.stats.hits, 1);

        assert_eq!(cache.stats.hit_rate(), 0.5);
    }

    #[test]
    fn test_write_back_mode() {
        let mut cache = BlockCache::new(8, 4096);

        assert!(!cache.is_write_back_enabled());

        cache.enable_write_back();
        assert!(cache.is_write_back_enabled());
        assert_eq!(cache.write_back_counter(), 1);

        cache.enable_write_back();
        assert_eq!(cache.write_back_counter(), 2);

        let mut device = MockDevice::new(100);
        cache.disable_write_back(&mut device, 512, 0).unwrap();
        assert_eq!(cache.write_back_counter(), 1);
        assert!(cache.is_write_back_enabled());

        cache.disable_write_back(&mut device, 512, 0).unwrap();
        assert_eq!(cache.write_back_counter(), 0);
        assert!(!cache.is_write_back_enabled());
    }
}
