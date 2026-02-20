//! Journal 事务提交逻辑
//!
//! 对应 lwext4 的 journal commit 功能

use super::{checksum, types::*, JbdFs, JbdJournal, JbdTrans, JournalError};
use crate::{
    block::{Block, BlockDev, BlockDevice},
    error::{Error, Result},
    superblock::Superblock,
};
use alloc::vec::Vec;

/// 提交一个事务到 journal
///
/// 对应 lwext4 的 `jbd_journal_commit_trans()`
///
/// # 参数
///
/// * `jbd_fs` - Journal 文件系统实例
/// * `jbd_journal` - Journal 管理器
/// * `trans` - 要提交的事务
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
///
/// # 提交流程
///
/// 1. 分配 journal 空间
/// 2. 写入 descriptor block(s)（包含块映射）
/// 3. 写入数据块到 journal
/// 4. 写入 commit block（标记事务完成）
/// 5. 更新 journal superblock
///
/// # 返回
///
/// 成功返回 ()，失败返回错误
pub fn commit_transaction<D: BlockDevice>(
    jbd_fs: &mut JbdFs,
    jbd_journal: &mut JbdJournal,
    trans: &mut JbdTrans,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
) -> Result<()> {
    // 检查事务是否有数据
    if trans.buffer_count() == 0 {
        // 空事务，直接返回
        return Ok(());
    }

    // 计算需要的 journal 块数
    // descriptor blocks + data blocks + commit block
    let data_blocks = trans.buffer_count() as u32;
    let descriptor_blocks = calculate_descriptor_blocks(data_blocks, jbd_fs.block_size());
    let total_blocks = descriptor_blocks + data_blocks + 1; // +1 for commit block

    // 检查 journal 空间是否足够
    if !jbd_journal.has_space(total_blocks) {
        return Err(Error::from(JournalError::NoSpace));
    }

    // 分配 journal 空间
    let journal_start = jbd_journal.allocate_blocks(total_blocks)
        .ok_or(Error::from(JournalError::NoSpace))?;

    trans.start_iblock = journal_start;
    trans.alloc_blocks = total_blocks as i32;

    // 当前 journal 块号
    let mut current_jblock = journal_start;

    // 获取 UUID 用于校验和
    let uuid = jbd_fs.sb().uuid;

    // 写入 descriptor blocks 和数据块
    current_jblock = write_descriptor_and_data_blocks(
        jbd_fs,
        trans,
        bdev,
        superblock,
        current_jblock,
        &uuid,
    )?;

    // 写入 commit block
    write_commit_block(
        jbd_fs,
        trans,
        bdev,
        superblock,
        current_jblock,
        &uuid,
    )?;

    // 更新 journal superblock
    let new_sequence = jbd_fs.sequence() + 1;
    jbd_fs.set_sequence(new_sequence);
    jbd_fs.mark_dirty();

    // 将事务添加到检查点队列
    // (在实际实现中，这里应该移动事务的所有权)
    // jbd_journal.add_to_checkpoint(trans);

    Ok(())
}

/// 计算需要多少个 descriptor blocks
fn calculate_descriptor_blocks(data_blocks: u32, block_size: u32) -> u32 {
    let tag_size = core::mem::size_of::<jbd_block_tag>() as u32;
    let header_size = core::mem::size_of::<jbd_bhdr>() as u32;
    let tags_per_block = (block_size - header_size) / tag_size;

    data_blocks.div_ceil(tags_per_block)
}

/// 写入 descriptor blocks 和数据块
///
/// # 返回
///
/// 下一个可用的 journal 块号
fn write_descriptor_and_data_blocks<D: BlockDevice>(
    jbd_fs: &JbdFs,
    trans: &JbdTrans,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
    start_jblock: u32,
    uuid: &[u8; 16],
) -> Result<u32> {
    let block_size = jbd_fs.block_size();
    let tag_size = core::mem::size_of::<jbd_block_tag>();
    let header_size = core::mem::size_of::<jbd_bhdr>();
    let tail_size = core::mem::size_of::<jbd_block_tail>();

    // 检查是否启用了校验和特性
    let has_csum = jbd_fs.has_incompat_feature(JBD_FEATURE_INCOMPAT_CSUM_V2 | JBD_FEATURE_INCOMPAT_CSUM_V3);
    let available_space = if has_csum {
        block_size as usize - header_size - tail_size
    } else {
        block_size as usize - header_size
    };
    let tags_per_block = available_space / tag_size;

    let mut current_jblock = start_jblock;
    let sequence = jbd_fs.sequence();

    // 遍历所有缓冲区
    let buffers: Vec<_> = trans.buf_queue.iter().collect();

    for (chunk_idx, chunk) in buffers.chunks(tags_per_block).enumerate() {
        // 为这个 descriptor block 分配物理块
        let desc_phys_block = jbd_fs.inode_bmap(bdev, superblock, current_jblock)?;
        current_jblock += 1;

        // 写入 descriptor block
        {
            let mut desc_block = Block::get(bdev, desc_phys_block)?;
            desc_block.with_data_mut(|data| {
                // 写入 header
                let header = jbd_bhdr::new(JBD_DESCRIPTOR_BLOCK, sequence);
                unsafe {
                    core::ptr::write_unaligned(data.as_mut_ptr() as *mut jbd_bhdr, header);
                }

                // 写入 block tags
                let mut offset = header_size;
                for (i, buf) in chunk.iter().enumerate() {
                    let is_last = (i == chunk.len() - 1) && (chunk_idx == buffers.chunks(tags_per_block).count() - 1);

                    let tag = jbd_block_tag {
                        blocknr: (buf.fs_lba() as u32).to_be(),
                        checksum: 0,
                        flags: if is_last { JBD_FLAG_LAST_TAG.to_be() } else { 0 },
                        blocknr_high: 0,
                    };

                    unsafe {
                        core::ptr::write_unaligned(
                            data.as_mut_ptr().add(offset) as *mut jbd_block_tag,
                            tag,
                        );
                    }

                    offset += tag_size;
                }

                // 如果启用了校验和，写入 tail
                if has_csum {
                    let tail_offset = data.len() - tail_size;
                    let csum = checksum::calculate_descriptor_csum(uuid, &data[..tail_offset]);
                    let tail = jbd_block_tail {
                        checksum: csum.to_be(),
                    };
                    unsafe {
                        core::ptr::write_unaligned(
                            data.as_mut_ptr().add(tail_offset) as *mut jbd_block_tail,
                            tail,
                        );
                    }
                }

                Ok::<_, Error>(())
            })??;
        } // desc_block 在这里释放

        // 写入对应的数据块
        for buf in chunk {
            let data_phys_block = jbd_fs.inode_bmap(bdev, superblock, current_jblock)?;
            current_jblock += 1;

            // 从文件系统读取原始数据
            let fs_block_data = {
                let mut fs_block = Block::get(bdev, buf.fs_lba())?;
                fs_block.with_data(|d| Ok::<_, Error>(d.to_vec()))?
            }?;

            // 写入到 journal
            let mut journal_block = Block::get(bdev, data_phys_block)?;
            journal_block.with_data_mut(|d| {
                let len = fs_block_data.len().min(d.len());
                d[..len].copy_from_slice(&fs_block_data[..len]);
            })?;
        }
    }

    Ok(current_jblock)
}

/// 写入 commit block
fn write_commit_block<D: BlockDevice>(
    jbd_fs: &JbdFs,
    _trans: &JbdTrans,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
    commit_jblock: u32,
    uuid: &[u8; 16],
) -> Result<()> {
    let commit_phys_block = jbd_fs.inode_bmap(bdev, superblock, commit_jblock)?;
    let sequence = jbd_fs.sequence();

    let mut commit_block = Block::get(bdev, commit_phys_block)?;
    commit_block.with_data_mut(|data| {
        // 创建 commit header
        let mut commit_header = jbd_commit_header {
            header: jbd_bhdr::new(JBD_COMMIT_BLOCK, sequence),
            chksum_type: 0,
            chksum_size: 0,
            padding: [0; 2],
            chksum: [0; JBD_CHECKSUM_BYTES],
            commit_sec: 0,
            commit_nsec: 0,
        };

        // 如果启用了校验和，计算并填充
        if jbd_fs.has_incompat_feature(JBD_FEATURE_INCOMPAT_CSUM_V2 | JBD_FEATURE_INCOMPAT_CSUM_V3) {
            commit_header.chksum_type = 1; // CRC32C
            commit_header.chksum_size = 4;

            // 计算校验和（先写入 header，然后计算）
            unsafe {
                core::ptr::write_unaligned(
                    data.as_mut_ptr() as *mut jbd_commit_header,
                    commit_header,
                );
            }

            let csum = checksum::calculate_commit_csum(uuid, data);
            commit_header.chksum[0] = csum.to_be();
        }

        // 写入最终的 commit header
        unsafe {
            core::ptr::write_unaligned(
                data.as_mut_ptr() as *mut jbd_commit_header,
                commit_header,
            );
        }

        Ok::<_, Error>(())
    })??;

    Ok(())
}

/// 写入 revoke block（撤销块）
///
/// # 参数
///
/// * `jbd_fs` - Journal 文件系统实例
/// * `trans` - 事务
/// * `bdev` - 块设备引用
/// * `superblock` - 文件系统 superblock
/// * `revoke_jblock` - revoke block 的 journal 块号
/// * `uuid` - Journal UUID
///
/// # 说明
///
/// 当事务包含撤销记录时调用
fn write_revoke_block<D: BlockDevice>(
    jbd_fs: &JbdFs,
    trans: &JbdTrans,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
    revoke_jblock: u32,
    uuid: &[u8; 16],
) -> Result<()> {
    if trans.revoke_count() == 0 {
        return Ok(());
    }

    let revoke_phys_block = jbd_fs.inode_bmap(bdev, superblock, revoke_jblock)?;
    let sequence = jbd_fs.sequence();

    let mut revoke_block = Block::get(bdev, revoke_phys_block)?;
    revoke_block.with_data_mut(|data| {
        // 创建 revoke header
        let header = jbd_revoke_header {
            header: jbd_bhdr::new(JBD_REVOKE_BLOCK, sequence),
            count: (trans.revoke_count() as u32).to_be(),
        };

        unsafe {
            core::ptr::write_unaligned(
                data.as_mut_ptr() as *mut jbd_revoke_header,
                header,
            );
        }

        // 写入所有撤销记录
        let mut offset = core::mem::size_of::<jbd_revoke_header>();
        for lba in trans.revoke_root.keys() {
            unsafe {
                core::ptr::write_unaligned(
                    data.as_mut_ptr().add(offset) as *mut u64,
                    lba.to_be(),
                );
            }
            offset += core::mem::size_of::<u64>();
        }

        // 计算并写入校验和（如果启用）
        if jbd_fs.has_incompat_feature(JBD_FEATURE_INCOMPAT_CSUM_V2 | JBD_FEATURE_INCOMPAT_CSUM_V3) {
            let tail_size = core::mem::size_of::<jbd_revoke_tail>();
            let tail_offset = data.len() - tail_size;

            // 计算校验和（不包含尾部）
            let csum = checksum::calculate_revoke_csum(uuid, &data[..tail_offset]);

            // 写入 revoke block tail
            let tail = jbd_revoke_tail {
                checksum: csum.to_be(),
            };
            unsafe {
                core::ptr::write_unaligned(
                    data.as_mut_ptr().add(tail_offset) as *mut jbd_revoke_tail,
                    tail,
                );
            }
        }

        Ok::<_, Error>(())
    })??;

    Ok(())
}

/// 提交事务的便捷包装函数
///
/// 对应 lwext4 的 `jbd_trans_commit()`
pub fn trans_commit<D: BlockDevice>(
    jbd_fs: &mut JbdFs,
    jbd_journal: &mut JbdJournal,
    trans: &mut JbdTrans,
    bdev: &mut BlockDev<D>,
    superblock: &mut Superblock,
) -> Result<()> {
    commit_transaction(jbd_fs, jbd_journal, trans, bdev, superblock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_descriptor_blocks() {
        // 假设 block size = 4096, tag size = 12, header size = 12
        // tags_per_block = (4096 - 12) / 12 = 340

        assert_eq!(calculate_descriptor_blocks(0, 4096), 0);
        assert_eq!(calculate_descriptor_blocks(1, 4096), 1);
        assert_eq!(calculate_descriptor_blocks(340, 4096), 1);
        assert_eq!(calculate_descriptor_blocks(341, 4096), 2);
        assert_eq!(calculate_descriptor_blocks(680, 4096), 2);
        assert_eq!(calculate_descriptor_blocks(681, 4096), 3);
    }

    #[test]
    fn test_commit_api() {
        // 这些测试需要实际的块设备和文件系统
        // 主要验证 API 设计和编译
    }
}
