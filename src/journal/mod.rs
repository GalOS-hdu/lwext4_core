//! ext4 Journal (JBD2) 实现
//!
//! 这个模块提供完整的ext4 journal功能，实现崩溃一致性和原子事务。
//!
//! # 架构概述
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │                   Application Layer                       │
//! │              (File/Directory Operations)                  │
//! └───────────────────────┬──────────────────────────────────┘
//!                         │
//!                         ▼
//! ┌──────────────────────────────────────────────────────────┐
//! │                 Transaction Layer                         │
//! │   JournalTransaction::begin() / commit() / abort()        │
//! └───────────────────────┬──────────────────────────────────┘
//!                         │
//!                         ▼
//! ┌──────────────────────────────────────────────────────────┐
//! │                   Journal Core                            │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
//! │  │  JbdJournal  │  │   JbdTrans   │  │    JbdBuf    │   │
//! │  │  (Manager)   │  │ (Transaction)│  │   (Buffer)   │   │
//! │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
//! │         │                  │                  │           │
//! │         └──────────────────┴──────────────────┘           │
//! │                            │                              │
//! │                            ▼                              │
//! │                    ┌──────────────┐                       │
//! │                    │    JbdFs     │                       │
//! │                    │(Journal FS)  │                       │
//! │                    └──────┬───────┘                       │
//! └───────────────────────────┼───────────────────────────────┘
//!                             │
//!                             ▼
//! ┌──────────────────────────────────────────────────────────┐
//! │                     Block Layer                           │
//! │   BlockCache / BlockDev / InodeRef                        │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! # 核心组件
//!
//! - [`types`] - JBD2磁盘格式定义
//! - [`JbdFs`] - Journal文件系统实例，管理journal inode
//! - [`JbdJournal`] - Journal管理器，维护所有活跃事务
//! - [`JbdTrans`] - 单个事务，跟踪修改的块
//! - [`JbdBuf`] - Journal缓冲区，描述事务中的块
//!
//! # 实现状态
//!
//! - ✅ JBD2 磁盘格式定义（types.rs）
//! - ⏭️ Journal 文件系统（jbd_fs.rs）
//! - ⏭️ Journal 管理器（jbd_journal.rs）
//! - ⏭️ 事务管理（jbd_trans.rs）
//! - ⏭️ 缓冲区管理（jbd_buf.rs）
//! - ⏭️ 崩溃恢复（recovery.rs）
//! - ⏭️ 事务提交（commit.rs）
//! - ⏭️ 检查点（checkpoint.rs）
//! - ⏭️ 校验和（checksum.rs）
//!
//! # 使用示例
//!
//! ```rust,ignore
//! use lwext4_core::journal::{JbdFs, JbdJournal, JournalTransaction};
//!
//! // 1. 初始化journal（mount时）
//! let mut jbd_fs = JbdFs::get(&mut fs)?;
//! jbd_fs.recover()?;  // 执行崩溃恢复
//! let mut journal = JbdJournal::start(&mut jbd_fs)?;
//!
//! // 2. 开始事务
//! let mut trans = journal.new_transaction()?;
//!
//! // 3. 执行修改
//! let mut block = trans.get_block(100)?;
//! block.with_data_mut(|data| {
//!     data[0] = 0x42;
//! })?;
//! trans.mark_block_dirty(100)?;
//!
//! // 4. 提交事务
//! journal.commit_transaction(trans)?;
//!
//! // 5. 停止journal（unmount时）
//! journal.stop()?;
//! jbd_fs.put()?;
//! ```
//!
//! # 对应lwext4
//!
//! 本模块是 lwext4 `ext4_journal.c` (3,710行) 的 Rust 重写。
//!
//! | lwext4                | lwext4-rust          |
//! |-----------------------|----------------------|
//! | `struct jbd_fs`       | [`JbdFs`]            |
//! | `struct jbd_journal`  | [`JbdJournal`]       |
//! | `struct jbd_trans`    | [`JbdTrans`]         |
//! | `struct jbd_buf`      | [`JbdBuf`]           |
//! | `jbd_recover()`       | [`JbdFs::recover()`] |
//! | `jbd_journal_start()` | [`JbdJournal::start()`] |
//! | `jbd_journal_commit_trans()` | [`JbdJournal::commit_transaction()`] |

pub mod types;

// Module declarations (to be implemented)
mod jbd_fs;
mod jbd_journal;
mod jbd_trans;
mod jbd_buf;
mod recovery;
mod commit;
mod checkpoint;
mod checksum;

// Re-exports
pub use types::*;
pub use jbd_fs::JbdFs;
pub use jbd_journal::JbdJournal;
pub use jbd_trans::JbdTrans;
pub use jbd_buf::JbdBuf;

/// Journal 初始化错误
#[derive(Debug)]
pub enum JournalError {
    /// Journal inode 不存在
    NoJournalInode,
    /// Journal 超级块无效
    InvalidSuperblock,
    /// Journal 功能不支持
    UnsupportedFeature(u32),
    /// 恢复失败
    RecoveryFailed,
    /// 空间不足
    NoSpace,
    /// IO 错误
    IoError,
}

impl core::fmt::Display for JournalError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            JournalError::NoJournalInode => write!(f, "Journal inode not found"),
            JournalError::InvalidSuperblock => write!(f, "Invalid journal superblock"),
            JournalError::UnsupportedFeature(feat) => {
                write!(f, "Unsupported journal feature: 0x{feat:08x}")
            }
            JournalError::RecoveryFailed => write!(f, "Journal recovery failed"),
            JournalError::NoSpace => write!(f, "Journal has no space"),
            JournalError::IoError => write!(f, "Journal I/O error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_module_compiles() {
        // Basic compilation test
        assert_eq!(JBD_MAGIC_NUMBER, 0xC03B3998);
    }
}
