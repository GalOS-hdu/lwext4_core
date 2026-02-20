//! Journal 检查点管理
//!
//! 对应 lwext4 的 journal checkpoint 功能

use super::{types::*, JbdFs, JbdJournal, JbdTrans};
use crate::{
    block::{Block, BlockDev, BlockDevice},
    error::{Error, ErrorKind, Result},
    superblock::Superblock,
};
use alloc::vec::Vec;

/// 执行 journal 检查点操作
///
/// 对应 lwext4 的 `jbd_journal_do_checkpoint()`
///
/// # 参数
///
/// * `jbd_fs` - Journal 文件系统实例
/// * `jbd_journal` - Journal 管理器
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
///
/// # 检查点流程
///
/// 1. 遍历检查点队列中的所有事务
/// 2. 将事务中的数据块写回到文件系统
/// 3. 清理已完成的事务
/// 4. 释放 journal 空间
/// 5. 更新 journal superblock 的 start 指针
///
/// # 返回
///
/// 成功返回 ()，失败返回错误
pub fn do_checkpoint<D: BlockDevice>(
    jbd_fs: &mut JbdFs,
    jbd_journal: &mut JbdJournal,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
) -> Result<()> {
    // 如果检查点队列为空，直接返回
    if jbd_journal.checkpoint_queue_len() == 0 {
        return Ok(());
    }

    // 处理检查点队列中的事务
    let mut completed_transactions = 0;

    // 遍历检查点队列
    loop {
        // 检查是否还有事务需要处理
        let has_trans = !jbd_journal.cp_queue.is_empty();
        if !has_trans {
            break;
        }

        // 检查事务是否可以被检查点（需要临时借用）
        let can_checkpoint = {
            let trans = jbd_journal.cp_queue.front().unwrap();
            is_transaction_checkpointable(trans, jbd_fs, bdev, superblock)?
        };

        if !can_checkpoint {
            // 如果第一个事务不能被检查点，停止处理
            break;
        }

        // 将事务中的所有数据块写回到文件系统
        // 先收集需要清理的块记录 LBA
        let block_lbas_to_remove: Vec<u64> = {
            let trans = jbd_journal.cp_queue.front().unwrap();
            checkpoint_transaction(trans, jbd_fs, bdev, superblock)?;
            trans.tbrec_list.iter().map(|rec| rec.lba).collect()
        };

        // 清理全局块记录索引
        for lba in block_lbas_to_remove {
            jbd_journal.remove_block_record(lba);
        }

        // 从队列中移除已完成的事务
        jbd_journal.cp_queue.pop_front();
        completed_transactions += 1;
    }

    // 如果有事务被检查点，更新 journal superblock
    if completed_transactions > 0 {
        // 计算新的 start 位置
        update_journal_start(jbd_fs, jbd_journal)?;
        jbd_fs.mark_dirty();
    }

    Ok(())
}

/// 检查事务是否可以被检查点
///
/// # 参数
///
/// * `trans` - 事务
/// * `jbd_fs` - Journal 文件系统实例
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
///
/// # 返回
///
/// true 如果事务可以被检查点，false 否则
fn is_transaction_checkpointable<D: BlockDevice>(
    trans: &JbdTrans,
    _jbd_fs: &JbdFs,
    _bdev: &mut BlockDev<D>,
    _superblock: &mut Superblock,
) -> Result<bool> {
    // 检查事务是否有错误
    if trans.has_error() {
        // 有错误的事务不能被检查点
        return Ok(false);
    }

    // 检查事务中的所有缓冲区是否已经写入完成
    // 在简化版本中，我们假设所有缓冲区都已经准备好
    Ok(true)
}

/// 对事务执行检查点
///
/// # 参数
///
/// * `trans` - 事务
/// * `jbd_fs` - Journal 文件系统实例
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
///
/// # 说明
///
/// 将事务中的所有数据块从 journal 写回到文件系统
///
/// # 检查点流程
///
/// 1. 遍历事务的所有缓冲区
/// 2. 对于每个缓冲区：
///    a. 计算其在 journal 中的位置
///    b. 从 journal 读取数据
///    c. 写回到文件系统的目标位置
///
/// # 注意
///
/// 调用者需要在此函数返回后清理全局块记录索引
fn checkpoint_transaction<D: BlockDevice>(
    trans: &JbdTrans,
    jbd_fs: &JbdFs,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
) -> Result<()> {
    // 注意：在 ordered 或 writeback 模式下，数据可能已经在 commit 之前写入
    // 在 journal 模式下，我们需要从 journal 读取并写回到文件系统

    // 计算 journal 中数据块的起始位置
    // 跳过 descriptor blocks
    let mut current_jblock = trans.start_iblock;

    // 计算这个事务使用了多少个 descriptor blocks
    let data_blocks = trans.buffer_count() as u32;
    let descriptor_blocks = calculate_descriptor_blocks_count(data_blocks, jbd_fs.block_size());

    // 跳过 descriptor blocks，从第一个数据块开始
    current_jblock += descriptor_blocks;

    // 遍历所有缓冲区，从 journal 读取并写回到文件系统
    for buf in &trans.buf_queue {
        // 获取 journal 中这个数据块的物理位置
        let journal_phys_block = jbd_fs.inode_bmap(bdev, superblock, current_jblock)?;

        // 从 journal 读取数据
        let journal_data = {
            let mut journal_block = Block::get(bdev, journal_phys_block)?;
            journal_block.with_data(|data| Ok::<_, Error>(data.to_vec()))?
        }?;

        // 写回到文件系统的目标位置
        let fs_target_block = buf.fs_lba();
        let mut fs_block = Block::get(bdev, fs_target_block)?;
        fs_block.with_data_mut(|data| {
            let len = journal_data.len().min(data.len());
            data[..len].copy_from_slice(&journal_data[..len]);
        })?;

        // 移动到下一个 journal 块
        current_jblock = next_journal_block(current_jblock, jbd_fs.first(), jbd_fs.max_len());
    }

    Ok(())
}

/// 计算需要多少个 descriptor blocks（与 commit.rs 保持一致）
fn calculate_descriptor_blocks_count(data_blocks: u32, block_size: u32) -> u32 {
    let tag_size = core::mem::size_of::<jbd_block_tag>() as u32;
    let header_size = core::mem::size_of::<jbd_bhdr>() as u32;
    let tags_per_block = (block_size - header_size) / tag_size;

    if data_blocks == 0 {
        0
    } else {
        data_blocks.div_ceil(tags_per_block)
    }
}

/// 计算下一个 journal 块号（处理环形）
fn next_journal_block(current: u32, first: u32, max_len: u32) -> u32 {
    let next = current + 1;
    let last = first + max_len;
    if next >= last {
        first
    } else {
        next
    }
}

/// 更新 journal superblock 的 start 指针
///
/// # 参数
///
/// * `jbd_fs` - Journal 文件系统实例
/// * `jbd_journal` - Journal 管理器
///
/// # 说明
///
/// 在检查点完成后，更新 journal 的 start 指针，
/// 以释放已经检查点的空间供新事务使用
fn update_journal_start(
    jbd_fs: &mut JbdFs,
    jbd_journal: &JbdJournal,
) -> Result<()> {
    // 如果检查点队列为空，将 start 设置为当前位置
    if jbd_journal.checkpoint_queue_len() == 0 {
        let new_start = jbd_journal.start;
        jbd_fs.set_start(new_start);
    } else {
        // 否则，将 start 设置为第一个未检查点事务的起始位置
        if let Some(first_trans) = jbd_journal.cp_queue.front() {
            jbd_fs.set_start(first_trans.start_iblock);
        }
    }

    Ok(())
}

/// 强制执行检查点（同步）
///
/// 对应 lwext4 的 `jbd_journal_force_checkpoint()`
///
/// # 参数
///
/// * `jbd_fs` - Journal 文件系统实例
/// * `jbd_journal` - Journal 管理器
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
///
/// # 说明
///
/// 强制对所有未检查点的事务执行检查点，
/// 通常在 umount 或需要释放 journal 空间时调用
pub fn force_checkpoint<D: BlockDevice>(
    jbd_fs: &mut JbdFs,
    jbd_journal: &mut JbdJournal,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
) -> Result<()> {
    // 循环执行检查点，直到队列为空
    loop {
        let queue_len = jbd_journal.checkpoint_queue_len();
        if queue_len == 0 {
            break;
        }

        // 执行一轮检查点
        do_checkpoint(jbd_fs, jbd_journal, bdev, superblock)?;

        // 如果队列长度没有减少，说明有问题
        let new_queue_len = jbd_journal.checkpoint_queue_len();
        if new_queue_len >= queue_len {
            return Err(Error::new(
                ErrorKind::InvalidState,
                "Checkpoint queue not progressing",
            ));
        }
    }

    Ok(())
}

/// 检查是否需要执行检查点
///
/// # 参数
///
/// * `jbd_journal` - Journal 管理器
/// * `threshold` - 触发检查点的阈值（已使用空间百分比）
///
/// # 返回
///
/// true 如果应该执行检查点，false 否则
///
/// # 说明
///
/// 当 journal 使用空间超过阈值时，应该触发检查点
/// 以释放空间供新事务使用
pub fn should_checkpoint(jbd_journal: &JbdJournal, threshold: u32) -> bool {
    let total_blocks = jbd_journal.total_blocks();
    let used_blocks = if jbd_journal.start >= jbd_journal.first {
        jbd_journal.start - jbd_journal.first
    } else {
        total_blocks - (jbd_journal.first - jbd_journal.start)
    };

    // 计算使用百分比
    let usage_percent = (used_blocks as u64 * 100) / total_blocks as u64;

    usage_percent >= threshold as u64
}

/// 尝试执行检查点（非阻塞）
///
/// # 参数
///
/// * `jbd_fs` - Journal 文件系统实例
/// * `jbd_journal` - Journal 管理器
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
/// * `max_transactions` - 最多处理的事务数
///
/// # 返回
///
/// 实际处理的事务数
///
/// # 说明
///
/// 尝试处理最多 max_transactions 个事务的检查点，
/// 如果某个事务不能被检查点，则停止
pub fn try_checkpoint<D: BlockDevice>(
    jbd_fs: &mut JbdFs,
    jbd_journal: &mut JbdJournal,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
    max_transactions: usize,
) -> Result<usize> {
    let mut processed = 0;

    while processed < max_transactions && jbd_journal.checkpoint_queue_len() > 0 {
        // 检查是否可以被检查点（需要临时借用）
        let can_checkpoint = {
            match jbd_journal.cp_queue.front() {
                Some(t) => is_transaction_checkpointable(t, jbd_fs, bdev, superblock)?,
                None => break,
            }
        };

        if !can_checkpoint {
            break;
        }

        // 执行检查点
        // 先收集需要清理的块记录 LBA
        let block_lbas_to_remove: Vec<u64> = {
            let trans = jbd_journal.cp_queue.front().unwrap();
            checkpoint_transaction(trans, jbd_fs, bdev, superblock)?;
            trans.tbrec_list.iter().map(|rec| rec.lba).collect()
        };

        // 清理全局块记录索引
        for lba in block_lbas_to_remove {
            jbd_journal.remove_block_record(lba);
        }

        // 移除事务
        jbd_journal.cp_queue.pop_front();
        processed += 1;
    }

    // 如果处理了事务，更新 journal start
    if processed > 0 {
        update_journal_start(jbd_fs, jbd_journal)?;
        jbd_fs.mark_dirty();
    }

    Ok(processed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_checkpoint() {
        let mut journal = JbdJournal::new(0, 100, 4096);

        // 初始状态，不应该触发检查点
        assert!(!should_checkpoint(&journal, 50));

        // 使用 50% 空间
        journal.start = 50;
        assert!(should_checkpoint(&journal, 50));
        assert!(!should_checkpoint(&journal, 51));

        // 使用 75% 空间
        journal.start = 75;
        assert!(should_checkpoint(&journal, 50));
        assert!(should_checkpoint(&journal, 75));
        assert!(!should_checkpoint(&journal, 76));
    }

    #[test]
    fn test_checkpoint_api() {
        // 这些测试需要实际的块设备和文件系统
        // 主要验证 API 设计和编译
    }

    #[test]
    fn test_update_journal_start() {
        // 测试 journal start 更新逻辑
        let mut jbd_sb = jbd_sb::default();
        jbd_sb.header.magic = JBD_MAGIC_NUMBER.to_be();
        jbd_sb.first = 1u32.to_be();
        jbd_sb.start = 50u32.to_be();

        let mut jbd_fs = JbdFs {
            inode: 8,
            sb: jbd_sb,
            dirty: false,
        };

        let jbd_journal = JbdJournal::new(1, 100, 4096);

        // 空队列时，应该设置为当前 journal start
        update_journal_start(&mut jbd_fs, &jbd_journal).unwrap();
        assert_eq!(jbd_fs.start(), jbd_journal.start);
    }
}
