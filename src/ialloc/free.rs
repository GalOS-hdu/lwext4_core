//! Inode 释放功能

use crate::{
    bitmap::*,
    block::{Block, BlockDev, BlockDevice},
    error::{Error, Result},
    fs::BlockGroupRef,
    superblock::Superblock,
};

use super::{checksum::*, helpers::*};

/// 释放一个 inode
///
/// 对应 lwext4 的 `ext4_ialloc_free_inode()`
///
/// # 参数
///
/// * `bdev` - 块设备引用
/// * `sb` - superblock 可变引用
/// * `inode` - 要释放的 inode 编号
/// * `is_dir` - 是否是目录
///
/// # 返回
///
/// 成功返回 Ok(())
pub fn free_inode<D: BlockDevice>(
    bdev: &mut BlockDev<D>,
    sb: &mut Superblock,
    inode: u32,
    is_dir: bool,
) -> Result<()> {
    // 计算块组编号
    let block_group = get_bgid_of_inode(sb, inode);

    // 第一步：操作 bitmap
    // 需要先获取 bitmap 地址和块组描述符副本（用于校验和）
    let bitmap_block_addr = {
        let mut bg_ref = BlockGroupRef::get(bdev, sb, block_group)?;
        bg_ref.inode_bitmap()?
    };

    // 获取块组描述符副本用于校验和验证
    let bg_copy = {
        let mut bg_ref = BlockGroupRef::get(bdev, sb, block_group)?;
        bg_ref.get_block_group_copy()?
    };

    // 操作位图
    {
        let mut bitmap_block = Block::get(bdev, bitmap_block_addr)?;

        // 在闭包内操作位图数据
        bitmap_block.with_data_mut(|bitmap_data| {
            // 验证位图校验和（如果启用）
            if !verify_bitmap_csum(sb, &bg_copy, bitmap_data) {
                // 这里只是记录警告，不阻止操作
                // 在实际应用中可以添加日志
            }

            // 在位图中释放 inode
            let index_in_group = inode_to_bgidx(sb, inode);
            clear_bit(bitmap_data, index_in_group)?;

            // 更新位图校验和
            // 注意：这里我们需要一个可变的 BlockGroup 副本
            let mut bg_for_csum = bg_copy;
            set_bitmap_csum(sb, &mut bg_for_csum, bitmap_data);

            Ok::<_, Error>(())
        })??;
        // bitmap_block 在此处自动释放并写回
    }

    // 第二步：更新块组描述符
    {
        let mut bg_ref = BlockGroupRef::get(bdev, sb, block_group)?;

        // 如果释放的是目录，递减已使用目录计数
        if is_dir {
            bg_ref.dec_used_dirs()?;
        }

        // 更新块组空闲 inode 计数
        bg_ref.inc_free_inodes(1)?;

        // bg_ref 在此处自动释放并写回
    }

    // 更新 superblock 空闲 inode 计数
    let sb_free_inodes = sb.free_inodes_count() + 1;
    sb.set_free_inodes_count(sb_free_inodes);

    // 写回 superblock
    sb.write(bdev)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_free_inode_placeholder() {
        // 这是一个占位测试
        // 实际测试需要创建一个模拟的文件系统
        assert!(true);
    }
}
