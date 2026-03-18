//! 字节缓冲区与结构体之间的安全转换工具
//!
//! 封装了常见的 unsafe 操作：
//! - `read_struct` — 从字节缓冲区解析 `#[repr(C)]` 结构体
//! - `as_bytes` / `as_bytes_mut` — 将结构体视为字节切片

use crate::error::{Error, ErrorKind, Result};

/// 从字节缓冲区解析一个 `#[repr(C)]` 结构体（不要求对齐）
///
/// # Safety invariant
///
/// 调用者需确保 `T` 是 `#[repr(C)]` 且全零是其合法值（POD 类型）。
/// 本 crate 中所有磁盘格式结构体均满足此条件。
#[inline]
pub fn read_struct<T: Copy>(buf: &[u8]) -> Result<T> {
    let size = core::mem::size_of::<T>();
    if buf.len() < size {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "buffer too small to read struct",
        ));
    }
    // SAFETY: T 是 Copy + repr(C)，buf 长度已检查，使用 read_unaligned 不要求对齐
    Ok(unsafe { core::ptr::read_unaligned(buf.as_ptr() as *const T) })
}

/// 将 `#[repr(C)]` 结构体视为不可变字节切片
///
/// # Safety invariant
///
/// 调用者需确保 `T` 是 `#[repr(C)]` 类型。
#[inline]
pub fn as_bytes<T: Sized>(val: &T) -> &[u8] {
    // SAFETY: T 是 repr(C) POD 类型，size_of::<T>() 字节均有效
    unsafe {
        core::slice::from_raw_parts(
            val as *const T as *const u8,
            core::mem::size_of::<T>(),
        )
    }
}

/// 从字节缓冲区的指定偏移量解析一个 `#[repr(C)]` 结构体
#[inline]
pub fn read_struct_at<T: Copy>(buf: &[u8], offset: usize) -> Result<T> {
    let size = core::mem::size_of::<T>();
    if offset + size > buf.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "buffer too small to read struct at offset",
        ));
    }
    Ok(unsafe { core::ptr::read_unaligned(buf.as_ptr().add(offset) as *const T) })
}

/// 将 `#[repr(C)]` 结构体写入字节缓冲区的指定偏移量
#[inline]
pub fn write_struct_at<T: Copy>(buf: &mut [u8], offset: usize, val: &T) -> Result<()> {
    let size = core::mem::size_of::<T>();
    if offset + size > buf.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "buffer too small to write struct at offset",
        ));
    }
    unsafe { core::ptr::write_unaligned(buf.as_mut_ptr().add(offset) as *mut T, *val) };
    Ok(())
}

/// 将 `#[repr(C)]` 结构体视为可变字节切片
///
/// # Safety invariant
///
/// 调用者需确保 `T` 是 `#[repr(C)]` 类型。
#[allow(dead_code)] // 与 as_bytes 成对的可变版本，供后续需要就地修改结构体时使用
#[inline]
pub fn as_bytes_mut<T: Sized>(val: &mut T) -> &mut [u8] {
    // SAFETY: T 是 repr(C) POD 类型，size_of::<T>() 字节均有效
    unsafe {
        core::slice::from_raw_parts_mut(
            val as *mut T as *mut u8,
            core::mem::size_of::<T>(),
        )
    }
}
