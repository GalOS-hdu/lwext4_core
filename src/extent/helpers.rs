//! Extent 树辅助函数
//!
//! 提供操作 extent header、extent、index 的辅助宏函数

use crate::types::{ext4_extent, ext4_extent_header, ext4_extent_idx};
use core::mem::size_of;

/// 获取 extent header 中的第一个 extent
///
/// 对应 lwext4 的 EXT_FIRST_EXTENT 宏
///
/// # 参数
///
/// * `header` - extent header 指针
///
/// # 返回
///
/// 第一个 extent 的可变指针
///
/// # Safety
///
/// 调用者必须确保：
/// - header 指向有效的 ext4_extent_header
/// - header 之后有足够的空间容纳 extent 数组
#[inline]
pub unsafe fn EXT_FIRST_EXTENT(header: *const ext4_extent_header) -> *mut ext4_extent {
    unsafe {
        (header as *const u8)
            .add(size_of::<ext4_extent_header>())
            as *mut ext4_extent
    }
}

/// 获取 extent header 中的最后一个 extent
///
/// 对应 lwext4 的 EXT_LAST_EXTENT 宏
///
/// # 参数
///
/// * `header` - extent header 指针
///
/// # 返回
///
/// 最后一个 extent 的可变指针（指向 entries_count-1 位置）
///
/// # Safety
///
/// 调用者必须确保：
/// - header 指向有效的 ext4_extent_header
/// - entries_count > 0
#[inline]
pub unsafe fn EXT_LAST_EXTENT(header: *const ext4_extent_header) -> *mut ext4_extent {
    unsafe {
        let first = EXT_FIRST_EXTENT(header);
        let entries = u16::from_le((*header).entries) as isize;
        first.offset(entries - 1)
    }
}

/// 获取 extent header 中可容纳的最大 extent
///
/// 对应 lwext4 的 EXT_MAX_EXTENT 宏
///
/// # 参数
///
/// * `header` - extent header 指针
///
/// # 返回
///
/// 最大 extent 的指针（指向 max_entries_count-1 位置）
///
/// # Safety
///
/// 调用者必须确保 header 指向有效的 ext4_extent_header
#[inline]
pub unsafe fn EXT_MAX_EXTENT(header: *const ext4_extent_header) -> *mut ext4_extent {
    unsafe {
        let first = EXT_FIRST_EXTENT(header);
        let max = u16::from_le((*header).max) as isize;
        first.offset(max - 1)
    }
}

/// 检查 extent header 是否还有空间
///
/// 对应 lwext4 的 EXT_HAS_FREE_EXTENT 宏
///
/// # Safety
///
/// 调用者必须确保 header 指向有效的 ext4_extent_header
#[inline]
pub unsafe fn EXT_HAS_FREE_EXTENT(header: *const ext4_extent_header) -> bool {
    unsafe {
        let entries = u16::from_le((*header).entries);
        let max = u16::from_le((*header).max);
        entries < max
    }
}

/// 获取 extent header 中的第一个 index
///
/// 对应 lwext4 的 EXT_FIRST_INDEX 宏
///
/// # 参数
///
/// * `header` - extent header 指针
///
/// # 返回
///
/// 第一个 index 的可变指针
///
/// # Safety
///
/// 调用者必须确保：
/// - header 指向有效的 ext4_extent_header
/// - header 之后有足够的空间容纳 index 数组
#[inline]
pub unsafe fn EXT_FIRST_INDEX(header: *const ext4_extent_header) -> *mut ext4_extent_idx {
    unsafe {
        (header as *const u8)
            .add(size_of::<ext4_extent_header>())
            as *mut ext4_extent_idx
    }
}

/// 获取 extent header 中的最后一个 index
///
/// 对应 lwext4 的 EXT_LAST_INDEX 宏
///
/// # 参数
///
/// * `header` - extent header 指针
///
/// # 返回
///
/// 最后一个 index 的可变指针（指向 entries_count-1 位置）
///
/// # Safety
///
/// 调用者必须确保：
/// - header 指向有效的 ext4_extent_header
/// - entries_count > 0
#[inline]
pub unsafe fn EXT_LAST_INDEX(header: *const ext4_extent_header) -> *mut ext4_extent_idx {
    unsafe {
        let first = EXT_FIRST_INDEX(header);
        let entries = u16::from_le((*header).entries) as isize;
        first.offset(entries - 1)
    }
}

/// 获取 extent header 中可容纳的最大 index
///
/// 对应 lwext4 的 EXT_MAX_INDEX 宏
///
/// # 参数
///
/// * `header` - extent header 指针
///
/// # 返回
///
/// 最大 index 的指针（指向 max_entries_count-1 位置）
///
/// # Safety
///
/// 调用者必须确保 header 指向有效的 ext4_extent_header
#[inline]
pub unsafe fn EXT_MAX_INDEX(header: *const ext4_extent_header) -> *mut ext4_extent_idx {
    unsafe {
        let first = EXT_FIRST_INDEX(header);
        let max = u16::from_le((*header).max) as isize;
        first.offset(max - 1)
    }
}

/// 检查 index node 是否还有空间
///
/// 对应 lwext4 的 EXT_HAS_FREE_INDEX 宏
///
/// # Safety
///
/// 调用者必须确保 header 指向有效的 ext4_extent_header
#[inline]
pub unsafe fn EXT_HAS_FREE_INDEX(header: *const ext4_extent_header) -> bool {
    unsafe {
        let entries = u16::from_le((*header).entries);
        let max = u16::from_le((*header).max);
        entries < max
    }
}

/// 存储 index 的物理块号（48 位）
///
/// 对应 lwext4 的 ext4_idx_store_pblock
///
/// # 参数
///
/// * `idx` - index 引用
/// * `pblock` - 物理块号
pub fn ext4_idx_store_pblock(idx: &mut ext4_extent_idx, pblock: u64) {
    // 🔧 验证输入的块号是否超出 48-bit 限制
    if pblock > 0xFFFFFFFFFFFF {
        log::error!(
            "[ext4_idx_store_pblock] Invalid pblock: {pblock:#x} (exceeds 48-bit limit)"
        );
    }

    idx.leaf_lo = ((pblock & 0xFFFFFFFF) as u32).to_le();
    idx.leaf_hi = (((pblock >> 32) & 0xFFFF) as u16).to_le();

    // 🔧 验证写入结果
    let reconstructed = ext4_idx_pblock(idx);
    if reconstructed != pblock {
        log::error!(
            "[ext4_idx_store_pblock] Mismatch! input={:#x}, stored={:#x}, leaf_lo={:#x}, leaf_hi={:#x}",
            pblock, reconstructed, u32::from_le(idx.leaf_lo), u16::from_le(idx.leaf_hi)
        );
    }

    log::trace!(
        "[ext4_idx_store_pblock] Stored pblock={:#x} -> leaf_lo={:#x}, leaf_hi={:#x}",
        pblock, u32::from_le(idx.leaf_lo), u16::from_le(idx.leaf_hi)
    );
}

/// 读取 index 的物理块号
///
/// 对应 lwext4 的 ext4_idx_pblock
///
/// # 参数
///
/// * `idx` - index 引用
///
/// # 返回
///
/// 物理块号（48 位）
pub fn ext4_idx_pblock(idx: &ext4_extent_idx) -> u64 {
    let lo = u32::from_le(idx.leaf_lo) as u64;
    let hi = u16::from_le(idx.leaf_hi) as u64;
    let pblock = lo | (hi << 32);

    // 注意：leaf_hi 非零是正常情况，表示物理块号超过 32 位
    // ext4 支持最大 48 位物理块地址
    log::trace!(
        "[ext4_idx_pblock] Read pblock={:#x} (leaf_lo={:#x}, leaf_hi={:#x})",
        pblock, lo as u32, hi as u16
    );

    pblock
}

/// 存储 extent 的物理块号（48 位）
///
/// 对应 lwext4 的 ext4_ext_store_pblock
///
/// # 参数
///
/// * `extent` - extent 引用
/// * `pblock` - 物理块号
pub fn ext4_ext_store_pblock(extent: &mut ext4_extent, pblock: u64) {
    extent.start_lo = ((pblock & 0xFFFFFFFF) as u32).to_le();
    extent.start_hi = (((pblock >> 32) & 0xFFFF) as u16).to_le();
}

/// 读取 extent 的物理块号
///
/// 对应 lwext4 的 ext4_ext_pblock
///
/// # 参数
///
/// * `extent` - extent 引用
///
/// # 返回
///
/// 物理块号（48 位）
pub fn ext4_ext_pblock(extent: &ext4_extent) -> u64 {
    let lo = u32::from_le(extent.start_lo) as u64;
    let hi = u16::from_le(extent.start_hi) as u64;
    let pblock = lo | (hi << 32);

    // 添加调试日志来追踪读取的 extent
    log::trace!(
        "[EXTENT_READ] ext4_ext_pblock: start_lo=0x{:x}, start_hi=0x{:x}, logical={}, len={}, pblock=0x{:x}",
        extent.start_lo, extent.start_hi,
        u32::from_le(extent.block), u16::from_le(extent.len),
        pblock
    );

    pblock
}

/// 计算 inode 内部作为 index root 的最大条目数
///
/// 对应 lwext4 的 ext4_ext_space_root_idx
///
/// # 返回
///
/// 最大 index 条目数
pub fn ext4_ext_space_root_idx() -> u16 {
    // inode.blocks 60B - header 12B = 48B
    // 每个 ext4_extent_idx 12B
    // 48 / 12 = 4
    4
}

/// 计算 inode 内部作为 extent root 的最大条目数
///
/// 对应 lwext4 的 ext4_ext_space_root
///
/// # 返回
///
/// 最大 extent 条目数
pub fn ext4_ext_space_root() -> u16 {
    // inode.blocks 60B - header 12B = 48B
    // 每个 ext4_extent 12B
    // 48 / 12 = 4
    4
}

/// 计算独立块中作为 index node 的最大条目数
///
/// 对应 lwext4 的 ext4_ext_space_block_idx
///
/// # 参数
///
/// * `block_size` - 块大小（字节）
///
/// # 返回
///
/// 最大 index 条目数
pub fn ext4_ext_space_block_idx(block_size: u32) -> u16 {
    // block 4096B - header 12B - tail 4B = 4080B
    // 每个 ext4_extent_idx 12B
    // 4080 / 12 = 340
    let available = block_size - size_of::<ext4_extent_header>() as u32 - 4; // -4 for tail
    (available / size_of::<ext4_extent_idx>() as u32) as u16
}

/// 计算独立块中作为 extent leaf 的最大条目数
///
/// 对应 lwext4 的 ext4_ext_space_block
///
/// # 参数
///
/// * `block_size` - 块大小（字节）
///
/// # 返回
///
/// 最大 extent 条目数
pub fn ext4_ext_space_block(block_size: u32) -> u16 {
    // block 4096B - header 12B - tail 4B = 4080B
    // 每个 ext4_extent 12B
    // 4080 / 12 = 340
    let available = block_size - size_of::<ext4_extent_header>() as u32 - 4; // -4 for tail
    (available / size_of::<ext4_extent>() as u32) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::*;

    #[test]
    fn test_extent_macros() {
        let mut header = ext4_extent_header {
            magic: EXT4_EXTENT_MAGIC.to_le(),
            entries: 2u16.to_le(),
            max: 4u16.to_le(),
            depth: 0u16.to_le(),
            generation: 0u32.to_le(),
        };

        unsafe {
            // 测试 EXT_HAS_FREE_EXTENT
            assert!(EXT_HAS_FREE_EXTENT(&header));

            // 设置满
            header.entries = 4u16.to_le();
            assert!(!EXT_HAS_FREE_EXTENT(&header));
        }
    }

    #[test]
    fn test_index_macros() {
        let mut header = ext4_extent_header {
            magic: EXT4_EXTENT_MAGIC.to_le(),
            entries: 2u16.to_le(),
            max: 4u16.to_le(),
            depth: 1u16.to_le(),
            generation: 0u32.to_le(),
        };

        unsafe {
            // 测试 EXT_HAS_FREE_INDEX
            assert!(EXT_HAS_FREE_INDEX(&header));

            // 设置满
            header.entries = 4u16.to_le();
            assert!(!EXT_HAS_FREE_INDEX(&header));
        }
    }

    #[test]
    fn test_idx_pblock() {
        let mut idx = ext4_extent_idx {
            block: 0,
            leaf_lo: 0,
            leaf_hi: 0,
            unused: 0,
        };

        // 测试存储和读取
        let test_block = 0x0000ABCD12345678u64;
        ext4_idx_store_pblock(&mut idx, test_block);

        let read_block = ext4_idx_pblock(&idx);
        assert_eq!(read_block, test_block);

        // 测试边界情况
        let max_48bit = 0x0000FFFFFFFFFFFFu64;
        ext4_idx_store_pblock(&mut idx, max_48bit);
        let read_block = ext4_idx_pblock(&idx);
        assert_eq!(read_block, max_48bit);
    }

    #[test]
    fn test_ext_pblock() {
        let mut extent = ext4_extent {
            block: 0,
            len: 0,
            start_hi: 0,
            start_lo: 0,
        };

        // 测试存储和读取
        let test_block = 0x0000ABCD12345678u64;
        ext4_ext_store_pblock(&mut extent, test_block);

        let read_block = ext4_ext_pblock(&extent);
        assert_eq!(read_block, test_block);
    }

    #[test]
    fn test_space_calculations() {
        // 测试 root 空间计算
        assert_eq!(ext4_ext_space_root_idx(), 4);
        assert_eq!(ext4_ext_space_root(), 4);

        // 测试 4KB 块空间计算
        assert_eq!(ext4_ext_space_block_idx(4096), 340);
        assert_eq!(ext4_ext_space_block(4096), 340);
    }
}
