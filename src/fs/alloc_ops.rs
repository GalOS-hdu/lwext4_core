//! Inode 和块的分配/释放/截断操作

use crate::{
    block::BlockDevice,
    error::Result,
};

use super::{inode_ref::InodeRef, Ext4FileSystem};

impl<D: BlockDevice> Ext4FileSystem<D> {
    /// 分配一个新的 inode
    ///
    /// # 参数
    ///
    /// * `is_dir` - 是否是目录
    ///
    /// # 返回
    ///
    /// 成功返回新分配的 inode 编号
    pub fn alloc_inode(&mut self, is_dir: bool) -> Result<u32> {
        use crate::ialloc::InodeAllocator;

        let mut allocator = InodeAllocator::new();
        let inode_num = allocator.alloc_inode(&mut self.bdev, &mut self.sb, is_dir)?;

        Ok(inode_num)
    }

    /// 释放一个 inode
    ///
    /// # 参数
    ///
    /// * `inode_num` - 要释放的 inode 编号
    /// * `is_dir` - 是否是目录
    pub fn free_inode(&mut self, inode_num: u32, is_dir: bool) -> Result<()> {
        use crate::ialloc::free_inode;

        free_inode(&mut self.bdev, &mut self.sb, inode_num, is_dir)?;

        Ok(())
    }

    /// 分配一个数据块
    ///
    /// # 参数
    ///
    /// * `goal` - 建议的块组 ID（用于局部性优化）
    ///
    /// # 返回
    ///
    /// 成功返回新分配的物理块号
    pub fn alloc_block(&mut self, goal: u64) -> Result<u64> {
        use crate::balloc::BlockAllocator;

        let mut allocator = BlockAllocator::new();
        let block_addr = allocator.alloc_block(&mut self.bdev, &mut self.sb, goal)?;

        Ok(block_addr)
    }

    /// 释放一个数据块
    ///
    /// # 参数
    ///
    /// * `block_addr` - 要释放的物理块号
    pub fn free_block(&mut self, block_addr: u64) -> Result<()> {
        use crate::balloc::free_block;

        free_block(&mut self.bdev, &mut self.sb, block_addr)?;

        Ok(())
    }

    /// 截断文件到指定大小
    ///
    /// # 参数
    ///
    /// * `inode_num` - inode 编号
    /// * `new_size` - 新的文件大小
    pub fn truncate_file(&mut self, inode_num: u32, new_size: u64) -> Result<()> {
        use crate::extent::remove_space;

        // 先获取block_size，避免借用冲突
        let block_size = self.sb.block_size() as u64;

        let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;
        let old_size = inode_ref.size()?;

        // 大小相同，无需操作
        if old_size == new_size {
            return Ok(());
        }

        log::debug!(
            "[TRUNCATE] inode {inode_num} truncate: {old_size} -> {new_size} bytes"
        );

        if old_size < new_size {
            // ===== 情况 1: 扩展文件（稀疏） =====
            log::debug!(
                "[TRUNCATE] Expanding file (sparse): {old_size} -> {new_size} bytes"
            );

            inode_ref.set_size(new_size)?;
            inode_ref.mark_dirty()?;

        } else {
            // ===== 情况 2: 缩小文件 =====
            log::debug!(
                "[TRUNCATE] Shrinking file: {old_size} -> {new_size} bytes"
            );

            // 步骤 1: 更新 i_size
            inode_ref.set_size(new_size)?;
            inode_ref.mark_dirty()?;
            drop(inode_ref);

            // 步骤 2: 如果新大小不是块对齐的，需要清零部分块
            let offset_in_block = (new_size % block_size) as usize;
            if new_size > 0 && offset_in_block != 0 {
                let last_block_num = ((new_size - 1) / block_size) as u32;

                log::debug!(
                    "[TRUNCATE] Zeroing partial block {last_block_num}: offset {offset_in_block} to {block_size}"
                );

                let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

                use crate::extent::get_blocks;
                use crate::balloc::BlockAllocator;

                let sb_ptr = inode_ref.superblock_mut() as *mut crate::superblock::Superblock;
                let sb_ref = unsafe { &mut *sb_ptr };

                let mut allocator = BlockAllocator::new();
                let (physical_block, _count) = get_blocks(
                    &mut inode_ref,
                    sb_ref,
                    &mut allocator,
                    last_block_num,
                    1,
                    false,
                )?;

                drop(inode_ref);

                if physical_block != 0 {
                    let mut block_buf = alloc::vec![0u8; block_size as usize];
                    self.bdev.read_block(physical_block, &mut block_buf)?;
                    block_buf[offset_in_block..].fill(0);
                    self.bdev.write_block(physical_block, &block_buf)?;

                    log::debug!(
                        "[TRUNCATE] Zeroed bytes [{offset_in_block}, {block_size}) in block {last_block_num} (physical block {physical_block})"
                    );
                } else {
                    log::debug!(
                        "[TRUNCATE] Block {last_block_num} is a hole, no need to zero"
                    );
                }
            }

            // 步骤 3: 计算需要释放的逻辑块范围
            let first_block_to_remove = if new_size == 0 {
                0
            } else {
                new_size.div_ceil(block_size) as u32
            };

            let last_block_to_remove = if old_size == 0 {
                0
            } else {
                ((old_size - 1) / block_size) as u32
            };

            // 步骤 4: 如果有需要释放的块，调用 remove_space
            if first_block_to_remove <= last_block_to_remove {
                log::debug!(
                    "[TRUNCATE] Freeing blocks: [{first_block_to_remove}, {last_block_to_remove}]"
                );

                let mut inode_ref = InodeRef::get(&mut self.bdev, &mut self.sb, inode_num)?;

                let sb_ptr = inode_ref.superblock_mut() as *mut crate::superblock::Superblock;
                let sb_ref = unsafe { &mut *sb_ptr };

                remove_space(&mut inode_ref, sb_ref, first_block_to_remove, last_block_to_remove)?;

                log::debug!(
                    "[TRUNCATE] Successfully freed {} blocks",
                    last_block_to_remove - first_block_to_remove + 1
                );
            } else {
                log::debug!("[TRUNCATE] No blocks to free");
            }
        }

        Ok(())
    }
}
