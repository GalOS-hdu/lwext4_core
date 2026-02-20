//! 块释放功能
//!
//! 对应 lwext4 的 `ext4_balloc_free_block()` 和 `ext4_balloc_free_blocks()`

use crate::{
    bitmap::*,
    block::{Block, BlockDev, BlockDevice},
    error::{Error, ErrorKind, Result},
    fs::BlockGroupRef,
    superblock::Superblock,
};

use super::{checksum::*, helpers::*};

/// 释放单个块
///
/// 对应 lwext4 的 `ext4_balloc_free_block()`
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 可变引用
/// * `baddr` - 要释放的块地址
///
/// # 返回
///
/// 成功返回 ()
///
/// # 注意
///
/// 此版本不更新 inode 的 blocks 计数，调用者需要自己处理
pub fn free_block<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &mut Superblock,
    baddr: u64,
) -> Result<()> {
    let bg_id = get_bgid_of_block(sb, baddr);
    let index_in_group = addr_to_idx_bg(sb, baddr);

    // 第一步：获取位图地址和块组描述符副本
    let (bitmap_block_addr, bg_copy) = {
        let mut bg_ref = BlockGroupRef::get(bdev, sb, bg_id)?;
        let bitmap_addr = bg_ref.block_bitmap()?;
        let bg_data = bg_ref.get_block_group_copy()?;
        (bitmap_addr, bg_data)
    };

    // 第二步：操作位图
    {
        let mut bitmap_block = Block::get(bdev, bitmap_block_addr)?;

        bitmap_block.with_data_mut(|bitmap_data| {
            // 验证位图校验和（如果启用）
            if !verify_bitmap_csum(sb, &bg_copy, bitmap_data) {
                // 记录警告但继续操作
            }

            // 清除位图中的位
            clear_bit(bitmap_data, index_in_group)?;

            // 更新位图校验和
            let mut bg_for_csum = bg_copy;
            set_bitmap_csum(sb, &mut bg_for_csum, bitmap_data);

            Ok::<_, Error>(())
        })??;
        // bitmap_block 在此处自动释放并写回
    }

    // 第三步：更新块组描述符
    {
        let mut bg_ref = BlockGroupRef::get(bdev, sb, bg_id)?;
        bg_ref.inc_free_blocks(1)?;
        // bg_ref 在此处自动释放并写回
    }

    // 更新 superblock 空闲块计数
    let mut sb_free_blocks = sb.free_blocks_count();
    sb_free_blocks += 1;
    sb.set_free_blocks_count(sb_free_blocks);
    sb.write(bdev)?;

    Ok(())
}

/// 释放多个连续或跨块组的块
///
/// 对应 lwext4 的 `ext4_balloc_free_blocks()`
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 可变引用
/// * `first` - 第一个要释放的块地址
/// * `count` - 要释放的块数量
///
/// # 返回
///
/// 成功返回 ()
///
/// # 注意
///
/// 此版本不更新 inode 的 blocks 计数，调用者需要自己处理
pub fn free_blocks<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &mut Superblock,
    first: u64,
    count: u32,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    let mut remaining = count;
    let mut current = first;

    // 计算第一个和最后一个块组
    let bg_first = get_bgid_of_block(sb, first);
    let bg_last = get_bgid_of_block(sb, first + count as u64 - 1);

    // 逐块组释放
    for bg_id in bg_first..=bg_last {
        // 计算此块组中要释放的第一个块的索引
        let idx_in_bg_first = addr_to_idx_bg(sb, current);

        // 计算此块组中可以释放的块数
        let block_size = sb.block_size();
        let mut free_cnt = block_size * 8 - idx_in_bg_first;

        // 如果是最后一个块组，只释放剩余的块
        if free_cnt > remaining {
            free_cnt = remaining;
        }

        // 第一步：获取位图地址和块组描述符副本
        let (bitmap_blk, bg_copy) = {
            let mut bg_ref = BlockGroupRef::get(bdev, sb, bg_id)?;
            let bitmap_addr = bg_ref.block_bitmap()?;
            let bg_data = bg_ref.get_block_group_copy()?;
            (bitmap_addr, bg_data)
        };

        // 第二步：操作位图
        {
            let mut bitmap_block = Block::get(bdev, bitmap_blk)?;

            bitmap_block.with_data_mut(|bitmap_data| {
                // 验证位图校验和（如果启用）
                if !verify_bitmap_csum(sb, &bg_copy, bitmap_data) {
                    // 记录警告但继续操作
                }

                // 清除位图中的多个位
                clear_bits(bitmap_data, idx_in_bg_first, free_cnt)?;

                // 更新位图校验和
                let mut bg_for_csum = bg_copy;
                set_bitmap_csum(sb, &mut bg_for_csum, bitmap_data);

                Ok::<_, Error>(())
            })??;
            // bitmap_block 在此处自动释放并写回
        }

        // 第三步：更新块组描述符
        {
            let mut bg_ref = BlockGroupRef::get(bdev, sb, bg_id)?;
            bg_ref.inc_free_blocks(free_cnt)?;
            // bg_ref 在此处自动释放并写回
        }

        // 更新计数
        remaining -= free_cnt;
        current += free_cnt as u64;

        // 更新 superblock 空闲块计数
        let mut sb_free_blocks = sb.free_blocks_count();
        sb_free_blocks += free_cnt as u64;
        sb.set_free_blocks_count(sb_free_blocks);
    }

    // 写回 superblock
    sb.write(bdev)?;

    // 确保所有块都已释放
    if remaining != 0 {
        return Err(Error::new(
            ErrorKind::Corrupted,
            "Not all blocks were freed",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_free_block_api() {
        // 这些测试需要实际的块设备和 ext4 文件系统
        // 主要是验证 API 的设计和编译
    }
}
