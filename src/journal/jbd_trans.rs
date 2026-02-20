//! JBD Transaction 管理
//!
//! 对应 lwext4 的 `struct jbd_trans`

use super::JbdBuf;
use crate::error::Result;
use alloc::{collections::{BTreeMap, VecDeque}, vec::Vec};

/// JBD Revoke Record（撤销记录）
///
/// 对应 lwext4 的 `struct jbd_revoke_rec`
///
/// 存储在事务的撤销树中，用于标记该事务希望撤销/覆盖的块。
#[derive(Debug, Clone)]
pub struct JbdRevokeRec {
    /// Physical block number to revoke
    pub lba: u64,
}

impl JbdRevokeRec {
    /// Create a new revoke record
    pub fn new(lba: u64) -> Self {
        Self { lba }
    }
}

/// JBD Block Record（块记录）
///
/// 对应 lwext4 的 `struct jbd_block_rec`
///
/// 记录一个物理块（LBA）当前被哪个事务所持有或修改。
/// 确保一个块在同一时间只被一个事务修改。
#[derive(Debug)]
pub struct JbdBlockRec {
    /// Physical block address
    pub lba: u64,

    /// Owning transaction ID
    pub trans_id: u64,

    /// Dirty buffers for this block
    pub dirty_buf_queue: VecDeque<usize>, // Indices into JbdTrans::buf_queue
}

impl JbdBlockRec {
    /// Create a new block record
    pub fn new(lba: u64, trans_id: u64) -> Self {
        Self {
            lba,
            trans_id,
            dirty_buf_queue: VecDeque::new(),
        }
    }
}

/// JBD Transaction（事务）
///
/// 对应 lwext4 的 `struct jbd_trans`
///
/// 代表一组原子操作。这是 JBD 系统的核心控制单元。
///
/// # lwext4 对应关系
///
/// ```c
/// struct jbd_trans {
///     uint32_t trans_id;
///     uint32_t start_iblock;
///     int alloc_blocks;
///     int data_cnt;
///     uint32_t data_csum;
///     int written_cnt;
///     int error;
///     struct jbd_journal *journal;
///     TAILQ_HEAD(..., jbd_buf) buf_queue;
///     RB_HEAD(..., jbd_revoke_rec) revoke_root;
///     LIST_HEAD(..., jbd_block_rec) tbrec_list;
///     TAILQ_ENTRY(jbd_trans) trans_node;
/// };
/// ```
///
/// # Rust 实现
///
/// - `buf_queue`: 使用 `VecDeque<JbdBuf>` 替代 TAILQ
/// - `revoke_root`: 使用 `BTreeMap<u64, JbdRevokeRec>` 替代 RB_HEAD
/// - `tbrec_list`: 使用 `Vec<JbdBlockRec>` 替代 LIST
/// - `trans_node`: 不需要，由 Journal 的 VecDeque 管理
#[derive(Debug)]
pub struct JbdTrans {
    /// Transaction ID (unique)
    pub trans_id: u64,

    /// Starting journal block number
    pub start_iblock: u32,

    /// Number of allocated journal blocks
    pub alloc_blocks: i32,

    /// Number of data blocks in transaction
    pub data_cnt: i32,

    /// Data checksum (if enabled)
    pub data_csum: u32,

    /// Number of blocks already written to journal
    pub written_cnt: i32,

    /// Error code (if any)
    pub error: i32,

    /// Buffer queue (all jbd_buf in this transaction)
    pub buf_queue: VecDeque<JbdBuf>,

    /// Revoke tree (blocks to be revoked in this transaction)
    /// Key: LBA, Value: JbdRevokeRec
    pub revoke_root: BTreeMap<u64, JbdRevokeRec>,

    /// Block record list (blocks involved in this transaction)
    pub tbrec_list: Vec<JbdBlockRec>,
}

impl JbdTrans {
    /// Create a new transaction
    ///
    /// # Parameters
    ///
    /// * `trans_id` - Unique transaction ID
    /// * `start_iblock` - Starting journal block number
    pub fn new(trans_id: u64, start_iblock: u32) -> Self {
        Self {
            trans_id,
            start_iblock,
            alloc_blocks: 0,
            data_cnt: 0,
            data_csum: 0,
            written_cnt: 0,
            error: 0,
            buf_queue: VecDeque::new(),
            revoke_root: BTreeMap::new(),
            tbrec_list: Vec::new(),
        }
    }

    /// Add a buffer to this transaction
    ///
    /// # Parameters
    ///
    /// * `buf` - The journal buffer to add
    pub fn add_buffer(&mut self, buf: JbdBuf) {
        self.buf_queue.push_back(buf);
        self.data_cnt += 1;
    }

    /// Add a revoke record
    ///
    /// # Parameters
    ///
    /// * `lba` - Physical block address to revoke
    ///
    /// # Returns
    ///
    /// true if the record was newly inserted, false if it already existed
    pub fn add_revoke(&mut self, lba: u64) -> bool {
        use alloc::collections::btree_map::Entry;

        match self.revoke_root.entry(lba) {
            Entry::Vacant(e) => {
                e.insert(JbdRevokeRec::new(lba));
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Try to revoke a block (returns true if successfully added)
    ///
    /// Same as add_revoke but returns Result for consistency with lwext4 API
    pub fn try_revoke(&mut self, lba: u64) -> Result<bool> {
        Ok(self.add_revoke(lba))
    }

    /// Check if a block is revoked in this transaction
    pub fn is_revoked(&self, lba: u64) -> bool {
        self.revoke_root.contains_key(&lba)
    }

    /// Add a block record
    pub fn add_block_record(&mut self, rec: JbdBlockRec) {
        self.tbrec_list.push(rec);
    }

    /// Get number of buffers in transaction
    pub fn buffer_count(&self) -> usize {
        self.buf_queue.len()
    }

    /// Get number of revoke records
    pub fn revoke_count(&self) -> usize {
        self.revoke_root.len()
    }

    /// Mark transaction as having an error
    pub fn set_error(&mut self, error: i32) {
        self.error = error;
    }

    /// Check if transaction has an error
    pub fn has_error(&self) -> bool {
        self.error != 0
    }

    /// Get transaction error code
    pub fn get_error(&self) -> i32 {
        self.error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trans_creation() {
        let trans = JbdTrans::new(1, 100);
        assert_eq!(trans.trans_id, 1);
        assert_eq!(trans.start_iblock, 100);
        assert_eq!(trans.alloc_blocks, 0);
        assert_eq!(trans.data_cnt, 0);
        assert_eq!(trans.buffer_count(), 0);
    }

    #[test]
    fn test_add_buffer() {
        let mut trans = JbdTrans::new(1, 100);
        let buf = JbdBuf::new(200, 1000);

        trans.add_buffer(buf);
        assert_eq!(trans.buffer_count(), 1);
        assert_eq!(trans.data_cnt, 1);
    }

    #[test]
    fn test_add_revoke() {
        let mut trans = JbdTrans::new(1, 100);

        // First insert should succeed
        assert!(trans.add_revoke(500));
        assert_eq!(trans.revoke_count(), 1);

        // Second insert of same block should return false
        assert!(!trans.add_revoke(500));
        assert_eq!(trans.revoke_count(), 1);

        // Different block should succeed
        assert!(trans.add_revoke(600));
        assert_eq!(trans.revoke_count(), 2);
    }

    #[test]
    fn test_is_revoked() {
        let mut trans = JbdTrans::new(1, 100);

        assert!(!trans.is_revoked(500));
        trans.add_revoke(500);
        assert!(trans.is_revoked(500));
        assert!(!trans.is_revoked(600));
    }

    #[test]
    fn test_error_handling() {
        let mut trans = JbdTrans::new(1, 100);

        assert!(!trans.has_error());
        assert_eq!(trans.get_error(), 0);

        trans.set_error(-5);
        assert!(trans.has_error());
        assert_eq!(trans.get_error(), -5);
    }
}
