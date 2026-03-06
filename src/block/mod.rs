//! 块设备抽象
//!
//! 提供块设备接口和块级 I/O 操作。
//!
//! ## I/O 路径
//!
//! - `device.rs` — BlockDevice trait 和 BlockDev 包装器
//! - `io.rs` — 缓存感知的块/字节级 I/O（read_block/write_block/read_bytes/write_bytes）
//! - `handle.rs` — Block RAII 句柄，保证块引用的一致性
//! - `lock.rs` — 并发锁接口（DeviceLock/NoLock）

mod device;
mod io;
mod handle;
mod lock;

pub use device::{BlockDevice, BlockDev};
pub use handle::Block;
pub use lock::{DeviceLock, NoLock};
