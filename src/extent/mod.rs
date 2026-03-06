//! Extent 树操作模块
//!
//! 这个模块提供 ext4 extent 树的解析和块映射功能。
//!
//! Extent 是现代 ext4 文件系统中用于表示文件数据块位置的机制，
//! 相比传统的间接块方式更高效。
//!
//! ## 子模块
//!
//! - `tree` - Extent 树读取操作（✅ 完全实现）
//! - `write` - Extent 树写入操作（✅ 核心功能完整）
//! - `checksum` - Extent 块校验和（✅ 完整实现）
//! - `unwritten` - Unwritten extent 支持（✅ 完整实现）
//! - `unwritten_multilevel` - 多层树 unwritten 支持（✅ 完整实现）
//! - `verify` - Extent 树完整性验证（✅ 完整实现）
//! - `grow` - 树深度增长（✅ 完整实现）
//! - `split` - 节点分裂（✅ 完整实现）
//! - `merge` - Extent 合并（✅ 完整实现）
//! - `remove` - 空间移除（✅ 多层树支持）
//!
//! ## 主要功能
//!
//! ### 读取操作
//! - `find_extent()` - 查找逻辑块对应的 extent
//! - extent 树遍历和解析
//!
//! ### 写入操作
//! - `tree_init()` - 初始化 extent 树
//! - `get_blocks()` - 获取/分配物理块（支持自动分配和多层树）
//! - `remove_space()` - 删除/截断文件（释放物理块，支持多层树）
//! - `ExtentWriter` - 高级 extent 写入器（支持节点分裂）
//!
//! ### 校验和功能
//! - `compute_checksum()` - 计算 extent 块校验和
//! - `set_checksum()` - 设置 extent 块校验和
//! - `verify_checksum()` - 验证 extent 块校验和
//!
//! ### Unwritten Extent 功能
//! - `mark_initialized()` / `mark_unwritten()` - 标记 extent 状态
//! - `is_unwritten()` - 检查 extent 是否为 unwritten
//! - `split_extent_at()` - 分裂 extent
//! - `convert_to_initialized()` - 转换 unwritten extent
//!
//! ### 完整性验证功能
//! - `check_extent_block()` - 验证 extent 块结构
//! - `check_inode_extent()` - 验证 inode 中的 extent 树
//! - `quick_check_header()` - 快速检查 extent header
//!
//! ## 实现状态
//!
//! - ✅ 小文件支持（深度 0 的 extent 树）
//! - ✅ 大文件支持（多层树，自动分裂/增长）
//! - ✅ 文件创建、写入、截断、删除
//! - ✅ 块分配和回收
//! - ✅ CRC32C 校验和支持
//! - ✅ Unwritten extent 支持（预分配、状态转换）
//! - ✅ 完整性验证（结构检查、校验和验证）
//! - ✅ Extent 自动合并（减少碎片化）
mod checksum;
mod grow;
mod helpers;
mod insert;
mod lookup;
mod merge;
mod node_ops;
mod path;
mod remove;
mod split;
mod tree;
mod unwritten;
mod unwritten_multilevel;
mod verify;
mod write;

pub use checksum::*;
pub use grow::grow_tree_depth;
pub use helpers::*;
pub use merge::{try_merge_and_insert, MergeDirection};
pub use remove::remove_space_multilevel;
pub use split::split_extent_node;
pub use tree::*;
pub use unwritten::*;
pub use unwritten_multilevel::{
    convert_to_initialized_multilevel,
    split_extent_at_multilevel,
};
pub use verify::*;
pub use write::{
    get_blocks, remove_space, tree_init,
    ExtentWriter,
};
