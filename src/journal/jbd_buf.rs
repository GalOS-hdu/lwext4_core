//! JBD Buffer 管理
//!
//! 对应 lwext4 的 `struct jbd_buf`


/// JBD Buffer（日志缓冲区）
///
/// 对应 lwext4 的 `struct jbd_buf`
///
/// 描述一个块在日志事务中的状态。
///
/// # 字段说明
///
/// - `jbd_lba`: 该块在日志设备上的逻辑块地址
/// - `fs_lba`: 该块在文件系统上的逻辑块地址
/// - `trans_id`: 所属事务的ID
/// - `dirty`: 是否脏
///
/// # lwext4 对应关系
///
/// ```c
/// struct jbd_buf {
///     uint32_t jbd_lba;
///     struct ext4_block block;
///     struct jbd_trans *trans;
///     struct jbd_block_rec *block_rec;
///     TAILQ_ENTRY(jbd_buf) buf_node;           // 链接到 trans->buf_queue
///     TAILQ_ENTRY(jbd_buf) dirty_buf_node;     // 链接到 block_rec->dirty_buf_queue
/// };
/// ```
///
/// # Rust 实现
///
/// - 使用 `trans_id` 替代指针
/// - `buf_node` / `dirty_buf_node`: 由 VecDeque 管理，不需要显式字段
pub struct JbdBuf {
    /// Journal logical block address
    pub(super) jbd_lba: u32,

    /// Filesystem logical block address
    pub(super) fs_lba: u64,

    /// Owning transaction ID
    pub(super) trans_id: Option<u64>,

    /// Block record ID in global index
    pub(super) block_rec_id: Option<u64>,

    /// Whether this buffer is dirty
    pub(super) dirty: bool,
}

impl JbdBuf {
    /// Create a new journal buffer
    pub(super) fn new(jbd_lba: u32, fs_lba: u64) -> Self {
        Self {
            jbd_lba,
            fs_lba,
            trans_id: None,
            block_rec_id: None,
            dirty: false,
        }
    }

    /// Get journal LBA
    pub fn jbd_lba(&self) -> u32 {
        self.jbd_lba
    }

    /// Get filesystem LBA
    pub fn fs_lba(&self) -> u64 {
        self.fs_lba
    }

    /// Mark buffer as dirty
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Check if buffer is dirty
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Set owning transaction
    pub(super) fn set_transaction(&mut self, trans_id: u64) {
        self.trans_id = Some(trans_id);
    }

    /// Set block record
    pub(super) fn set_block_record(&mut self, rec_id: u64) {
        self.block_rec_id = Some(rec_id);
    }
}

impl core::fmt::Debug for JbdBuf {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("JbdBuf")
            .field("jbd_lba", &self.jbd_lba)
            .field("fs_lba", &self.fs_lba)
            .field("trans_id", &self.trans_id)
            .field("block_rec_id", &self.block_rec_id)
            .field("dirty", &self.dirty)
            .finish()
    }
}
