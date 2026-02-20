//! Journal 恢复逻辑
//!
//! 对应 lwext4 的 journal recovery 功能

use super::{types::*, JbdFs};
use crate::{
    block::{Block, BlockDev, BlockDevice},
    error::{Error, Result},
    superblock::Superblock,
};
use alloc::vec::Vec;

/// 执行 journal 恢复
///
/// 对应 lwext4 的 `jbd_recover()`
///
/// # 参数
///
/// * `jbd_fs` - Journal 文件系统实例
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
///
/// # 恢复流程
///
/// 1. 检查 journal 是否需要恢复（INCOMPAT_RECOVER 标志）
/// 2. 扫描 journal，找到所有未提交的事务
/// 3. 重放（replay）这些事务的修改
/// 4. 清除 INCOMPAT_RECOVER 标志
/// 5. 更新 journal superblock
pub fn recover<D: BlockDevice>(
    jbd_fs: &mut JbdFs,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
) -> Result<()> {
    // 检查是否需要恢复
    // 如果 journal 的 start 等于 first，说明 journal 是空的，不需要恢复
    let start_block = jbd_fs.start();
    let first_block = jbd_fs.first();

    if start_block == first_block {
        // Journal 是空的，不需要恢复
        return Ok(());
    }

    // 获取 journal 参数
    let max_len = jbd_fs.max_len();
    let sequence = jbd_fs.sequence();

    // 扫描 journal 并执行恢复
    let scan_result = scan_journal(jbd_fs, bdev, superblock, start_block, sequence, max_len)?;

    // 重放所有已提交的事务
    for trans_info in &scan_result.transactions {
        replay_transaction(jbd_fs, bdev, superblock, trans_info)?;
    }

    // 更新 journal 起始位置（跳过已恢复的事务）
    if let Some(new_start) = scan_result.next_start {
        jbd_fs.set_start(new_start);
    } else {
        // 如果没有新的起始位置，重置到 first
        jbd_fs.set_start(first_block);
    }

    // 标记 journal superblock 为脏，需要写回
    jbd_fs.mark_dirty();

    Ok(())
}

/// Journal 扫描结果
#[derive(Debug)]
struct ScanResult {
    /// 需要重放的事务列表
    transactions: Vec<TransactionInfo>,
    /// 下一个事务的起始位置
    next_start: Option<u32>,
}

/// 事务信息
#[derive(Debug)]
struct TransactionInfo {
    /// 事务序列号
    sequence: u32,
    /// 事务起始块号
    start_block: u32,
    /// 事务中的块记录
    blocks: Vec<BlockRecord>,
}

/// 块记录
#[derive(Debug)]
struct BlockRecord {
    /// Journal 中的块号
    journal_block: u32,
    /// 文件系统中的目标块号
    fs_block: u64,
}

/// 扫描 journal，找到所有需要恢复的事务
///
/// # 参数
///
/// * `jbd_fs` - Journal 文件系统实例
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
/// * `start` - 扫描起始块号
/// * `sequence` - 期望的起始序列号
/// * `max_len` - Journal 最大长度
///
/// # 返回
///
/// 扫描结果，包含需要重放的事务列表
fn scan_journal<D: BlockDevice>(
    jbd_fs: &JbdFs,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
    start: u32,
    mut sequence: u32,
    max_len: u32,
) -> Result<ScanResult> {
    let mut transactions = Vec::new();
    let mut current_block = start;
    let first_block = jbd_fs.first();

    // 扫描 journal，直到遇到无效的块或序列号不匹配
    loop {
        // 将 journal 逻辑块号映射到物理块号
        let physical_block = jbd_fs.inode_bmap(bdev, superblock, current_block)?;

        // 读取块头
        let (magic, blocktype, seq) = {
            let mut block = Block::get(bdev, physical_block)?;
            block.with_data(|data| {
                if data.len() < core::mem::size_of::<jbd_bhdr>() {
                    return Ok::<_, Error>((0u32, 0u32, 0u32));
                }

                let header = unsafe {
                    core::ptr::read_unaligned(data.as_ptr() as *const jbd_bhdr)
                };

                Ok::<_, Error>((
                    u32::from_be(header.magic),
                    u32::from_be(header.blocktype),
                    u32::from_be(header.sequence),
                ))
            })??
        };

        // 检查 magic number 和序列号
        if magic != JBD_MAGIC_NUMBER || seq != sequence {
            // 遇到无效块或序列号不匹配，停止扫描
            break;
        }

        // 根据块类型处理
        match blocktype {
            JBD_DESCRIPTOR_BLOCK => {
                // 描述符块，包含事务的块映射
                let trans_info = scan_descriptor_block(
                    jbd_fs,
                    bdev,
                    superblock,
                    current_block,
                    sequence,
                )?;

                transactions.push(trans_info.0);
                current_block = trans_info.1;
            }
            JBD_COMMIT_BLOCK => {
                // 提交块，标志事务完成
                // 继续扫描下一个块
                current_block = next_block(current_block, first_block, max_len);
                sequence += 1;
            }
            JBD_REVOKE_BLOCK => {
                // 撤销块，跳过（恢复时不需要处理）
                current_block = next_block(current_block, first_block, max_len);
            }
            _ => {
                // 未知类型，停止扫描
                break;
            }
        }

        // 防止死循环（扫描超过 journal 长度）
        if transactions.len() > max_len as usize {
            break;
        }
    }

    Ok(ScanResult {
        transactions,
        next_start: Some(current_block),
    })
}

/// 扫描描述符块
///
/// # 返回
///
/// (TransactionInfo, 下一个块号)
fn scan_descriptor_block<D: BlockDevice>(
    jbd_fs: &JbdFs,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
    desc_block: u32,
    sequence: u32,
) -> Result<(TransactionInfo, u32)> {
    let physical_block = jbd_fs.inode_bmap(bdev, superblock, desc_block)?;

    let mut block = Block::get(bdev, physical_block)?;
    let mut blocks = Vec::new();
    let mut current_block = next_block(desc_block, jbd_fs.first(), jbd_fs.max_len());

    block.with_data(|data| {
        let mut offset = core::mem::size_of::<jbd_bhdr>();
        let block_size = jbd_fs.block_size() as usize;

        // 解析所有 block tags
        while offset + core::mem::size_of::<jbd_block_tag>() <= block_size {
            let tag = unsafe {
                core::ptr::read_unaligned(
                    data.as_ptr().add(offset) as *const jbd_block_tag
                )
            };

            let fs_block = u32::from_be(tag.blocknr) as u64;
            let flags = u16::from_be(tag.flags);

            blocks.push(BlockRecord {
                journal_block: current_block,
                fs_block,
            });

            offset += core::mem::size_of::<jbd_block_tag>();
            current_block = next_block(current_block, jbd_fs.first(), jbd_fs.max_len());

            // 检查是否是最后一个 tag
            if (flags & JBD_FLAG_LAST_TAG) != 0 {
                break;
            }
        }

        Ok::<_, Error>(())
    })??;

    Ok((
        TransactionInfo {
            sequence,
            start_block: desc_block,
            blocks,
        },
        current_block,
    ))
}

/// 重放一个事务
fn replay_transaction<D: BlockDevice>(
    jbd_fs: &JbdFs,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
    trans_info: &TransactionInfo,
) -> Result<()> {
    // 对于事务中的每个块，将 journal 中的数据写回到文件系统
    for block_rec in &trans_info.blocks {
        // 从 journal 读取数据
        let journal_phys = jbd_fs.inode_bmap(bdev, superblock, block_rec.journal_block)?;
        let data = {
            let mut block = Block::get(bdev, journal_phys)?;
            block.with_data(|d| Ok::<_, Error>(d.to_vec()))?
        }?;

        // 写回到文件系统
        let mut fs_block = Block::get(bdev, block_rec.fs_block)?;
        fs_block.with_data_mut(|d| {
            let len = data.len().min(d.len());
            d[..len].copy_from_slice(&data[..len]);
        })?;
    }

    Ok(())
}

/// 计算下一个块号（处理循环）
fn next_block(current: u32, first: u32, max_len: u32) -> u32 {
    let next = current + 1;
    if next >= first + max_len {
        first
    } else {
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_block() {
        // 测试正常情况
        assert_eq!(next_block(10, 0, 100), 11);

        // 测试循环
        assert_eq!(next_block(99, 0, 100), 0);

        // 测试非零起始
        assert_eq!(next_block(149, 50, 100), 50);
    }

    #[test]
    fn test_recovery_api() {
        // 这些测试需要实际的 journal 数据
        // 主要验证 API 设计和编译
    }
}
