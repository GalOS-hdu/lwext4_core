//! 文件系统高级 API
//!
//! 这个模块提供完整的 ext4 文件系统操作接口。

mod filesystem;
mod file;
mod metadata;
mod inode_ref;
mod block_group_ref;
mod types;
mod path_ops;
mod alloc_ops;
mod vfs_ops;

pub use filesystem::Ext4FileSystem;
pub use file::File;
pub use metadata::{FileMetadata, FileType};
pub use inode_ref::InodeRef;
pub use block_group_ref::BlockGroupRef;
pub use types::{FileAttr, FsConfig, InodeType, StatFs, SystemHal};
