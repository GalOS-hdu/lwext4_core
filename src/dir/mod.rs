//! 目录操作模块
//!
//! 这个模块提供 ext4 目录的解析和路径查找功能。
//!
//! ## 模块结构
//!
//! - `checksum` - 目录校验和功能
//! - `iterator` - 目录迭代器（使用 Block handle）
//! - `path_lookup` - 路径查找（使用 InodeRef）
//! - `hash` - HTree 哈希算法（支持所有哈希版本）
//! - `htree` - HTree 索引功能（查找完成，写入部分完成）
//! - `write` - 目录写操作（支持添加/删除条目）
//! - `reader` - ArceOS 兼容的有状态目录读取器

pub mod checksum;
pub mod iterator;
pub mod reader;
pub mod path_lookup;
pub mod hash;
pub mod htree;
pub mod htree_split;
pub mod write;
pub mod write_init;

// 重新导出常用类型
pub use iterator::{DirEntry, DirIterator, read_dir};
pub use reader::DirReader;
pub use path_lookup::{PathLookup, lookup_path, get_inode_ref_by_path};
