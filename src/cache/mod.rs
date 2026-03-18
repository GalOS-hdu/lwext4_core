//! 块缓存模块
//!
//! 这个模块提供了完整的块缓存实现，对应 lwext4 的 bcache 功能。
//!
//! # 主要组件
//!
//! - [`CacheBuffer`] - 单个缓存块，包含数据和元数据
//! - [`BlockCache`] - 块缓存管理器，使用 lru crate 提供 LRU 驱逐
//! - [`CacheFlags`] - 缓存块状态标志
//! - [`CacheStats`] - 缓存统计信息
//!
//! # 设计原理
//!
//! 本模块使用成熟的 `lru` crate 替代手动实现 LRU 缓存。关键改进：
//!
//! 1. **数据结构**：使用 `lru::LruCache` 替代手动的 BTreeMap 双索引
//! 2. **内存安全**：无需手动管理引用计数，lru crate 自动管理生命周期
//! 3. **类型安全**：使用强类型和 `bitflags` 替代 C 的 int 标志
//! 4. **性能**：O(1) 操作替代 O(log n)，无手动 LRU 维护开销
//! 5. **可靠性**：使用经过充分测试的 lru crate，避免自实现的 bug
//!
//! # 与 lwext4 的对应关系
//!
//! | lwext4 C                          | lwext4-rust                       |
//! |-----------------------------------|-----------------------------------|
//! | `struct ext4_buf`                 | [`CacheBuffer`]                   |
//! | `struct ext4_bcache`              | [`BlockCache`]                    |
//! | `RB_HEAD(lba_root)`               | `LruCache<u64, CacheBuffer>`      |
//! | `RB_HEAD(lru_root)`               | *(lru crate 内部)*                 |
//! | `SLIST_HEAD(dirty_list)`          | `BTreeSet<u64>`                   |
//! | `ext4_bcache_alloc()`             | [`BlockCache::alloc()`]           |
//! | `ext4_bcache_free()`              | *(不再需要)*                       |
//! | `ext4_bcache_find_get()`          | *(已合并到 alloc)*                  |
//! | `ext4_buf_lowest_lru()`           | *(lru crate 内部)*                 |
//! | `ext4_bcache_invalidate_lba()`    | [`BlockCache::invalidate_buffer()`]|
//! | `ext4_block_cache_flush()`        | [`BlockCache::flush_all()`]       |
//!
//! # 功能完整性
//!
//! ✅ LBA 索引（O(1) 快速查找）
//! ✅ LRU 驱逐策略（lru crate 自动管理）
//! ❌ 引用计数（不再需要，Rust 借用检查器保证安全）
//! ✅ 脏块跟踪
//! ✅ 范围失效
//! ✅ 异步写入回调
//! ✅ 缓存统计信息
//!
//! # 使用示例
//!
//! ```rust,ignore
//! use lwext4_core::cache::{BlockCache, DEFAULT_CACHE_SIZE};
//!
//! // 创建缓存（8 个块，每块 4096 字节）
//! let mut cache = BlockCache::new(DEFAULT_CACHE_SIZE, 4096);
//!
//! // 分配块
//! let (buf, is_new) = cache.alloc(100)?;
//! if is_new {
//!     // 从设备读取数据
//!     block_device.read_block(100, &mut buf.data)?;
//!     buf.mark_uptodate();
//! }
//!
//! // 修改数据
//! buf.data[0] = 42;
//! buf.mark_dirty();
//! cache.mark_dirty(100)?;
//!
//! // ✅ 不再需要手动 free，lru crate 自动管理
//!
//! // 刷新所有脏块
//! cache.flush_all(&mut block_device, sector_size, partition_offset)?;
//!
//! // 查看统计信息
//! let stats = cache.stats();
//! println!("Cache: {}/{} used, {} dirty",
//!          stats.used, stats.capacity, stats.dirty_blocks);
//! ```
//!
//! # 性能特性
//!
//! - **查找**: O(1) - HashMap 查找（lru crate 内部）
//! - **插入**: O(1) - HashMap 插入 + LRU 链表更新
//! - **LRU 驱逐**: O(1) - 直接访问 LRU 链表尾部
//! - **刷新**: O(k) - k 为脏块数量
//!
//! 相比手动实现的 BTreeMap，lru crate 提供更快的 O(1) 操作和自动的 LRU 管理。
//!
//! # 内存分配要求
//!
//! 本模块依赖 `alloc` crate，需要用户提供全局分配器。
//! 参见 [`alloc::alloc::GlobalAlloc`] 和 `#[global_allocator]`。

mod buffer;
mod block_cache;

pub use buffer::{CacheBuffer, CacheFlags, EndWriteCallback};
pub use block_cache::{BlockCache, CacheStats, DEFAULT_CACHE_SIZE};
