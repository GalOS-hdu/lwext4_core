//! lwext4_core: Pure Rust ext4 filesystem implementation
//!
//! 这是一个纯 Rust 实现的 ext4 文件系统库，旨在提供：
//! - **零 unsafe 代码**（除必要的结构体定义）
//! - **Rust 惯用风格**的 API
//! - **完整的类型安全**
//! - **可选的 C API 兼容层**
//!
//! # 示例
//!
//! ```rust,ignore
//! use lwext4_core::{BlockDevice, block::BlockDev, Result};
//!
//! // 实现 BlockDevice trait
//! struct MyDevice {
//!     // ...
//! }
//!
//! impl BlockDevice for MyDevice {
//!     // 实现必要的方法
//!     // ...
//! }
//!
//! fn main() -> Result<()> {
//!     let device = MyDevice::new();
//!     let mut block_dev = BlockDev::new(device);
//!
//!     // 读取块
//!     let mut buf = vec![0u8; 4096];
//!     block_dev.read_block(0, &mut buf)?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # 模块结构
//!
//! - [`error`] - 错误类型定义
//! - [`block`] - 块设备抽象和 I/O 操作
//! - [`consts`] - 常量定义
//! - [`types`] - 数据结构定义
//! - [`superblock`] - Superblock 操作
//! - [`c_api`] - C API 兼容层（可选）

#![no_std]
#![deny(unsafe_op_in_unsafe_fn)]
#![allow(missing_docs)]
// Suppress clippy warnings that are not worth fixing
#![allow(clippy::too_many_arguments)]
#![allow(clippy::len_without_is_empty)]
#![allow(deprecated)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(private_interfaces)]

extern crate alloc;

// ===== 核心模块 =====

/// 错误处理
pub mod error;

/// 块设备抽象
pub mod block;

/// 常量定义
pub mod consts;

/// 数据结构定义
pub mod types;

/// Superblock 操作
pub mod superblock;

/// Inode 操作
pub mod inode;

/// 块组操作
pub mod block_group;

/// Extent 树操作
pub mod extent;

/// Indirect blocks 操作（传统 ext2/ext3 间接块寻址）
pub mod indirect;

/// 目录操作
pub mod dir;

/// 文件系统高级 API
pub mod fs;

/// 块缓存
pub mod cache;

/// 位图操作
pub mod bitmap;

/// Inode 分配
pub mod ialloc;

/// 块分配
pub mod balloc;

/// Transaction 系统
pub mod transaction;

/// Journal (JBD2) 系统
pub mod journal;

/// Extended Attributes (xattr)
pub mod xattr;

/// CRC32C 校验和计算
pub(crate) mod crc;

// ===== C API 兼容层（可选）=====

/// C API 兼容层
///
/// 提供与 lwext4 C 库兼容的函数接口。
#[cfg(feature = "c-api")]
pub mod c_api;

// ===== 公共导出 =====

// 错误处理
pub use error::{Error, ErrorKind, Result};

// 块设备
pub use block::{BlockDevice, BlockDev, Block};

// Superblock
pub use superblock::{Superblock, read_superblock};

// Inode
pub use inode::{Inode, read_inode};

// BlockGroup
pub use block_group::{BlockGroup, read_block_group_desc, write_block_group_desc};

// Extent
pub use extent::ExtentTree;

// Indirect blocks
pub use indirect::IndirectBlockMapper;

// Dir
pub use dir::{DirEntry, DirIterator, DirReader, PathLookup, read_dir, lookup_path, get_inode_ref_by_path};

// FileSystem
pub use fs::{
    Ext4FileSystem, File, FileMetadata, FileType,
    FileAttr, FsConfig, InodeType, StatFs, SystemHal,
    InodeRef, BlockGroupRef,
};

// Cache
pub use cache::{BlockCache, CacheBuffer, CacheFlags, CacheStats, DEFAULT_CACHE_SIZE};

// Transaction
pub use transaction::SimpleTransaction;

// Journal
pub use journal::{JbdFs, JbdJournal, JbdTrans, JbdBuf, JournalError};

// Xattr
pub use xattr::{list as xattr_list, get as xattr_get, set as xattr_set, remove as xattr_remove};

// C API（当启用时）
#[cfg(feature = "c-api")]
pub use c_api::block::{
    ext4_blocks_get_direct, ext4_blocks_set_direct, ext4_block_readbytes,
    ext4_block_writebytes, ext4_block_cache_flush,
};
