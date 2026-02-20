//! JBD Journal 管理器
//!
//! 对应 lwext4 的 `struct jbd_journal`

use super::{JbdTrans, jbd_trans::JbdBlockRec};
use alloc::collections::{BTreeMap, VecDeque};

/// JBD Journal（日志管理器）
///
/// 对应 lwext4 的 `struct jbd_journal`
///
/// 维护所有活跃事务和全局块状态。
///
/// # lwext4 对应关系
///
/// ```c
/// struct jbd_journal {
///     uint32_t first;                 // 日志区域的起始块号
///     uint32_t start;                 // 下一个事务将要开始写入的日志块号（动态）
///     uint32_t last;                  // 日志区域的结束块号
///     uint32_t trans_id;              // 下一个要使用的事务 ID
///     uint32_t alloc_trans_id;        // 已经分配 ID 但尚未开始的事务 ID
///     uint32_t block_size;            // 日志块大小
///     TAILQ_HEAD(..., jbd_trans) cp_queue; // 检查点队列
///     RB_HEAD(..., jbd_block_rec) block_rec_root; // 全局块记录红黑树
///     struct jbd_fs *jbd_fs;          // 指向父级文件系统实例
/// };
/// ```
///
/// # Rust 实现
///
/// - `cp_queue`: 使用 `VecDeque<JbdTrans>` 替代 TAILQ
/// - `block_rec_root`: 使用 `BTreeMap<u64, JbdBlockRec>` 替代 RB_HEAD（key是LBA）
/// - `jbd_fs`: 暂时不存储指针，由外部管理
#[derive(Debug)]
pub struct JbdJournal {
    /// First block of journal area
    pub first: u32,

    /// Next transaction will start writing from this block (dynamic)
    pub start: u32,

    /// Last block of journal area
    pub last: u32,

    /// Next transaction ID to use
    pub trans_id: u64,

    /// Allocated transaction ID (but not yet started)
    pub alloc_trans_id: u64,

    /// Journal block size
    pub block_size: u32,

    /// Checkpoint queue (transactions being checkpointed)
    pub cp_queue: VecDeque<JbdTrans>,

    /// Global block record index (all active block records)
    /// Key: LBA, Value: JbdBlockRec
    pub block_rec_root: BTreeMap<u64, JbdBlockRec>,
}

impl JbdJournal {
    /// Create a new journal manager
    ///
    /// # Parameters
    ///
    /// * `first` - First block of journal area
    /// * `last` - Last block of journal area
    /// * `block_size` - Journal block size
    pub fn new(first: u32, last: u32, block_size: u32) -> Self {
        Self {
            first,
            start: first, // Initially start from first block
            last,
            trans_id: 1, // Start from 1
            alloc_trans_id: 1,
            block_size,
            cp_queue: VecDeque::new(),
            block_rec_root: BTreeMap::new(),
        }
    }

    /// Allocate a new transaction ID
    ///
    /// # Returns
    ///
    /// New transaction ID
    pub fn alloc_trans_id(&mut self) -> u64 {
        let id = self.alloc_trans_id;
        self.alloc_trans_id += 1;
        id
    }

    /// Create a new transaction
    ///
    /// # Returns
    ///
    /// New transaction with allocated ID
    pub fn new_transaction(&mut self) -> JbdTrans {
        let trans_id = self.alloc_trans_id();
        let start_iblock = self.start;

        // Update start for next transaction
        // (actual space allocation happens during commit)

        JbdTrans::new(trans_id, start_iblock)
    }

    /// Add a block record to the global index
    ///
    /// # Parameters
    ///
    /// * `lba` - Physical block address
    /// * `trans_id` - Transaction ID that owns this block
    ///
    /// # Returns
    ///
    /// true if newly inserted, false if block already has a record
    pub fn add_block_record(&mut self, lba: u64, trans_id: u64) -> bool {
        use alloc::collections::btree_map::Entry;

        match self.block_rec_root.entry(lba) {
            Entry::Vacant(e) => {
                e.insert(JbdBlockRec::new(lba, trans_id));
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Remove a block record from the global index
    ///
    /// # Parameters
    ///
    /// * `lba` - Physical block address
    pub fn remove_block_record(&mut self, lba: u64) -> Option<JbdBlockRec> {
        self.block_rec_root.remove(&lba)
    }

    /// Get a block record from the global index
    ///
    /// # Parameters
    ///
    /// * `lba` - Physical block address
    pub fn get_block_record(&self, lba: u64) -> Option<&JbdBlockRec> {
        self.block_rec_root.get(&lba)
    }

    /// Get mutable reference to a block record
    pub fn get_block_record_mut(&mut self, lba: u64) -> Option<&mut JbdBlockRec> {
        self.block_rec_root.get_mut(&lba)
    }

    /// Add a transaction to the checkpoint queue
    ///
    /// # Parameters
    ///
    /// * `trans` - Transaction to checkpoint
    pub fn add_to_checkpoint(&mut self, trans: JbdTrans) {
        self.cp_queue.push_back(trans);
    }

    /// Get number of transactions in checkpoint queue
    pub fn checkpoint_queue_len(&self) -> usize {
        self.cp_queue.len()
    }

    /// Allocate journal blocks for a transaction
    ///
    /// # Parameters
    ///
    /// * `count` - Number of blocks to allocate
    ///
    /// # Returns
    ///
    /// Starting block number, or None if insufficient space
    pub fn allocate_blocks(&mut self, count: u32) -> Option<u32> {
        let total_blocks = self.last - self.first;
        let available = if self.start >= self.first {
            self.last - self.start
        } else {
            // Wrapped around
            total_blocks - (self.first - self.start)
        };

        if count > available {
            return None;
        }

        let allocated_start = self.start;
        self.start += count;

        // Handle wraparound
        if self.start >= self.last {
            self.start = self.first + (self.start - self.last);
        }

        Some(allocated_start)
    }

    /// Get total journal size in blocks
    pub fn total_blocks(&self) -> u32 {
        self.last - self.first
    }

    /// Check if journal has enough space for N blocks
    pub fn has_space(&self, count: u32) -> bool {
        let available = if self.start >= self.first {
            self.last - self.start
        } else {
            self.total_blocks() - (self.first - self.start)
        };
        count <= available
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_creation() {
        let journal = JbdJournal::new(100, 200, 4096);
        assert_eq!(journal.first, 100);
        assert_eq!(journal.start, 100);
        assert_eq!(journal.last, 200);
        assert_eq!(journal.trans_id, 1);
        assert_eq!(journal.block_size, 4096);
        assert_eq!(journal.total_blocks(), 100);
    }

    #[test]
    fn test_alloc_trans_id() {
        let mut journal = JbdJournal::new(100, 200, 4096);

        assert_eq!(journal.alloc_trans_id(), 1);
        assert_eq!(journal.alloc_trans_id(), 2);
        assert_eq!(journal.alloc_trans_id(), 3);
        assert_eq!(journal.alloc_trans_id, 4);
    }

    #[test]
    fn test_new_transaction() {
        let mut journal = JbdJournal::new(100, 200, 4096);

        let trans = journal.new_transaction();
        assert_eq!(trans.trans_id, 1);
        assert_eq!(trans.start_iblock, 100);
    }

    #[test]
    fn test_block_record_management() {
        let mut journal = JbdJournal::new(100, 200, 4096);

        // Add block record
        assert!(journal.add_block_record(500, 1));
        assert!(!journal.add_block_record(500, 2)); // Already exists

        // Get block record
        let rec = journal.get_block_record(500).unwrap();
        assert_eq!(rec.lba, 500);
        assert_eq!(rec.trans_id, 1);

        // Remove block record
        let removed = journal.remove_block_record(500).unwrap();
        assert_eq!(removed.lba, 500);
        assert!(journal.get_block_record(500).is_none());
    }

    #[test]
    fn test_checkpoint_queue() {
        let mut journal = JbdJournal::new(100, 200, 4096);

        assert_eq!(journal.checkpoint_queue_len(), 0);

        let trans = JbdTrans::new(1, 100);
        journal.add_to_checkpoint(trans);
        assert_eq!(journal.checkpoint_queue_len(), 1);
    }

    #[test]
    fn test_block_allocation() {
        let mut journal = JbdJournal::new(100, 200, 4096);

        // Allocate 10 blocks
        let start = journal.allocate_blocks(10).unwrap();
        assert_eq!(start, 100);
        assert_eq!(journal.start, 110);

        // Allocate 20 more
        let start2 = journal.allocate_blocks(20).unwrap();
        assert_eq!(start2, 110);
        assert_eq!(journal.start, 130);

        // Try to allocate too many blocks
        assert!(journal.allocate_blocks(1000).is_none());
    }

    #[test]
    fn test_has_space() {
        let journal = JbdJournal::new(100, 200, 4096);

        assert!(journal.has_space(50));
        assert!(journal.has_space(100));
        assert!(!journal.has_space(101));
        assert!(!journal.has_space(1000));
    }
}
