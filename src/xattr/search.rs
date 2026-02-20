//! xattr 搜索和查找功能
//!
//! 提供在 xattr 数据区中查找特定条目的功能

use crate::{
    consts::*,
    types::ext4_xattr_entry,
};
use core::mem::size_of;

/// xattr 搜索上下文
///
/// 对应 lwext4 的 `struct ext4_xattr_search`
///
/// 用于在 xattr 数据区中搜索特定条目
#[derive(Debug)]
pub struct XattrSearch<'a> {
    /// 第一个 entry
    pub first: usize,

    /// buffer 起始地址（相对偏移）
    pub base: usize,

    /// buffer 结束地址（相对偏移）
    pub end: usize,

    /// 当前找到的 entry（相对偏移，None 表示未找到）
    pub here: Option<usize>,

    /// 是否未找到
    pub not_found: bool,

    /// xattr 数据引用
    data: &'a [u8],
}

impl<'a> XattrSearch<'a> {
    /// 创建新的搜索上下文
    ///
    /// # 参数
    ///
    /// * `data` - xattr 数据区（可能是 inode 内部或 xattr block）
    /// * `first_offset` - 第一个 entry 的偏移
    ///
    /// # 返回
    ///
    /// 搜索上下文
    pub fn new(data: &'a [u8], first_offset: usize) -> Self {
        Self {
            first: first_offset,
            base: 0,
            end: data.len(),
            here: None,
            not_found: true,
            data,
        }
    }

    /// 查找指定名称的 entry
    ///
    /// 对应 lwext4 的 `ext4_xattr_find_entry()`
    ///
    /// # 参数
    ///
    /// * `name_index` - 命名空间索引
    /// * `name` - 属性名称（不含前缀）
    ///
    /// # 返回
    ///
    /// 成功返回 (entry_offset, value_offset, value_size)，失败返回 None
    pub fn find_entry(&mut self, name_index: u8, name: &[u8]) -> Option<(usize, usize, u32)> {
        self.not_found = true;
        self.here = None;

        let mut offset = self.first;

        // 遍历所有 entry
        loop {
            // 检查是否到达末尾
            if offset + size_of::<ext4_xattr_entry>() > self.end {
                break;
            }

            // 检查是否是最后一个 entry（前4字节为0）
            if is_last_entry(self.data, offset) {
                break;
            }

            // 读取 entry
            let entry = read_entry(self.data, offset);
            let entry_name_len = entry.e_name_len as usize;

            // 获取 entry 名称
            let name_offset = offset + size_of::<ext4_xattr_entry>();
            if name_offset + entry_name_len > self.data.len() {
                break;
            }

            let entry_name = &self.data[name_offset..name_offset + entry_name_len];

            // 比较 name_index 和 name
            if entry_name_len == name.len()
                && entry.e_name_index == name_index
                && entry_name == name
            {
                // 找到匹配的 entry
                self.here = Some(offset);
                self.not_found = false;

                let value_size = entry.value_size();
                let value_offset = if value_size > 0 {
                    u16::from_le(entry.e_value_offs) as usize
                } else {
                    0
                };

                return Some((offset, value_offset, value_size));
            }

            // 移动到下一个 entry
            offset = next_entry_offset(offset, entry_name_len);
        }

        None
    }

    /// 检查是否为空（没有任何 entry）
    ///
    /// 对应 lwext4 的 `ext4_xattr_is_empty()`
    pub fn is_empty(&self) -> bool {
        is_last_entry(self.data, self.first)
    }

    /// 计算可用空间
    ///
    /// 返回 entry 区域末尾和 value 区域开始之间的空闲字节数
    pub fn free_space(&self) -> usize {
        let mut min_value_offset = self.end;
        let mut last_entry_offset = self.first;

        let mut offset = self.first;

        // 遍历所有 entry 找到最小的 value 偏移和最后的 entry 位置
        loop {
            if offset + size_of::<ext4_xattr_entry>() > self.end {
                break;
            }

            if is_last_entry(self.data, offset) {
                last_entry_offset = offset;
                break;
            }

            let entry = read_entry(self.data, offset);
            let entry_name_len = entry.e_name_len as usize;

            // 如果值不在外部块，更新最小值偏移
            if entry.e_value_block == 0 && entry.value_size() > 0 {
                let value_offset = u16::from_le(entry.e_value_offs) as usize;
                if value_offset < min_value_offset {
                    min_value_offset = value_offset;
                }
            }

            last_entry_offset = offset;
            offset = next_entry_offset(offset, entry_name_len);
        }

        // entry 区域的末尾（需要加上最后一个 entry 的结尾标记）
        let entries_end = last_entry_offset + size_of::<ext4_xattr_entry>();

        // 可用空间 = 最小值偏移 - entry 区域末尾
        min_value_offset.saturating_sub(entries_end)
    }
}

/// 检查是否是最后一个 entry
///
/// 对应 C 宏 `EXT4_XATTR_IS_LAST_ENTRY(entry)`
///
/// # 参数
///
/// * `data` - xattr 数据
/// * `offset` - entry 偏移
///
/// # 返回
///
/// 如果是最后一个 entry 返回 true
#[inline]
fn is_last_entry(data: &[u8], offset: usize) -> bool {
    if offset + 4 > data.len() {
        return true;
    }

    let first_u32 = u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]);

    first_u32 == 0
}

/// 读取 entry
///
/// # 参数
///
/// * `data` - xattr 数据
/// * `offset` - entry 偏移
///
/// # 返回
///
/// ext4_xattr_entry 结构体
#[inline]
fn read_entry(data: &[u8], offset: usize) -> ext4_xattr_entry {
    let entry_size = size_of::<ext4_xattr_entry>();
    if offset + entry_size > data.len() {
        return ext4_xattr_entry::default();
    }

    let entry_bytes = &data[offset..offset + entry_size];

    // 安全地从字节数组构造 ext4_xattr_entry
    unsafe {
        core::ptr::read(entry_bytes.as_ptr() as *const ext4_xattr_entry)
    }
}

/// 计算下一个 entry 的偏移
///
/// 对应 C 宏 `EXT4_XATTR_NEXT(entry)` 和 `EXT4_XATTR_LEN(name_len)`
///
/// # 参数
///
/// * `current_offset` - 当前 entry 偏移
/// * `name_len` - 当前 entry 名称长度
///
/// # 返回
///
/// 下一个 entry 的偏移
#[inline]
fn next_entry_offset(current_offset: usize, name_len: usize) -> usize {
    // EXT4_XATTR_LEN = (name_len + ROUND + sizeof(entry)) & ~ROUND
    let len = (name_len + EXT4_XATTR_ROUND as usize + size_of::<ext4_xattr_entry>())
        & !(EXT4_XATTR_ROUND as usize);
    current_offset + len
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_is_last_entry() {
        let data = [0u8; 16];
        assert!(is_last_entry(&data, 0));

        let data = [1, 0, 0, 0, 0, 0, 0, 0];
        assert!(!is_last_entry(&data, 0));
    }

    #[test]
    fn test_next_entry_offset() {
        // entry 结构体大小为 16 字节
        // 名称长度 4，对齐后 4
        // 总长度 = (4 + 3 + 16) & ~3 = 23 & ~3 = 20
        let offset = next_entry_offset(0, 4);
        assert_eq!(offset, 20);

        // 名称长度 7，对齐后 8
        // 总长度 = (7 + 3 + 16) & ~3 = 26 & ~3 = 24
        let offset = next_entry_offset(0, 7);
        assert_eq!(offset, 24);
    }

    #[test]
    fn test_search_empty() {
        // 空数据（全0）
        let data = [0u8; 32];
        let search = XattrSearch::new(&data, 0);
        assert!(search.is_empty());
    }

    #[test]
    fn test_search_find_entry() {
        // 构造一个简单的 entry
        let mut data = vec![0u8; 256];

        // 构造 entry：
        // e_name_len = 4 ("test")
        // e_name_index = 1 (user)
        // e_value_offs = 100
        // e_value_size = 5
        data[0] = 4; // e_name_len
        data[1] = 1; // e_name_index
        data[2..4].copy_from_slice(&100u16.to_le_bytes()); // e_value_offs
        data[4..8].copy_from_slice(&0u32.to_le_bytes()); // e_value_block
        data[8..12].copy_from_slice(&5u32.to_le_bytes()); // e_value_size

        // 名称 "test"
        data[16..20].copy_from_slice(b"test");

        // 值 "hello" 在偏移 100
        data[100..105].copy_from_slice(b"hello");

        // 下一个 entry 标记为结束（全0）
        // 当前 entry 长度 = (4 + 3 + 16) & ~3 = 20

        let mut search = XattrSearch::new(&data, 0);
        let result = search.find_entry(1, b"test");

        assert!(result.is_some());
        let (entry_offset, value_offset, value_size) = result.unwrap();
        assert_eq!(entry_offset, 0);
        assert_eq!(value_offset, 100);
        assert_eq!(value_size, 5);
        assert!(!search.not_found);
    }

    #[test]
    fn test_search_not_found() {
        let mut data = vec![0u8; 256];

        // 构造一个 entry，名称为 "test"
        data[0] = 4;
        data[1] = 1;
        data[16..20].copy_from_slice(b"test");

        let mut search = XattrSearch::new(&data, 0);
        let result = search.find_entry(1, b"other");

        assert!(result.is_none());
        assert!(search.not_found);
    }
}
