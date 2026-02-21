//! ext4 数据结构定义
//!
//! 这个模块包含了直接对应磁盘格式的数据结构。
//!
//! ## 设计原则
//!
//! 1. **磁盘格式结构** - 保留 C 风格命名（便于对照ext4规范）
//! 2. **内存表示** - 使用 `#[repr(C)]` 确保布局正确
//! 3. **辅助方法** - 提供 Rust 风格的访问器和工具函数

#![allow(non_camel_case_types)]  // 允许C风格命名

use crate::consts::*;

/// 为 `#[repr(C)]` POD 结构体统一实现 `Default`（全零初始化）
///
/// 所有磁盘格式结构体都是纯数据类型，全零是合法的初始值。
macro_rules! impl_zeroed_default {
    ($($t:ty),+ $(,)?) => {
        $(
            impl Default for $t {
                fn default() -> Self {
                    // SAFETY: 所有目标类型都是 #[repr(C)] POD 类型，全零合法
                    unsafe { core::mem::zeroed() }
                }
            }
        )+
    };
}

impl_zeroed_default!(
    ext4_sblock,
    ext4_inode,
    ext4_dir_idx_climit,
    ext4_dir_idx_dot_en,
    ext4_dir_idx_rinfo,
    ext4_dir_idx_entry,
    ext4_dir_idx_root,
    ext4_dir_idx_node,
    ext4_fake_dir_entry,
    ext4_dir_idx_tail,
    ext4_dir_entry_tail,
    ext4_group_desc,
    ext4_extent_header,
    ext4_extent,
    ext4_extent_idx,
    ext4_extent_tail,
    ext4_xattr_header,
    ext4_xattr_ibody_header,
    ext4_xattr_entry,
);

//=============================================================================
// 磁盘格式结构定义
//=============================================================================

/// Superblock 结构
///
/// 对应 ext4 磁盘格式中的 superblock (ext4_super_block)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_sblock {
    pub inodes_count: u32,           // 0: 总 inode 数
    pub blocks_count_lo: u32,        // 4: 总块数（低32位）
    pub r_blocks_count_lo: u32,      // 8: 保留块数（低32位）
    pub free_blocks_count_lo: u32,   // 12: 空闲块数（低32位）
    pub free_inodes_count: u32,      // 16: 空闲 inode 数
    pub first_data_block: u32,       // 20: 第一个数据块
    pub log_block_size: u32,         // 24: 块大小（2^(10+log_block_size)）
    pub log_cluster_size: u32,       // 28: 簇大小
    pub blocks_per_group: u32,       // 32: 每组块数
    pub clusters_per_group: u32,     // 36: 每组簇数
    pub inodes_per_group: u32,       // 40: 每组 inode 数
    pub mtime: u32,                  // 44: 挂载时间
    pub wtime: u32,                  // 48: 写入时间
    pub mnt_count: u16,              // 52: 挂载次数
    pub max_mnt_count: u16,          // 54: 最大挂载次数
    pub magic: u16,                  // 56: 魔数 (0xEF53)
    pub state: u16,                  // 58: 文件系统状态
    pub errors: u16,                 // 60: 错误处理方式
    pub minor_rev_level: u16,        // 62: 次版本号
    pub lastcheck: u32,              // 64: 最后检查时间
    pub checkinterval: u32,          // 68: 检查间隔
    pub creator_os: u32,             // 72: 创建者操作系统
    pub rev_level: u32,              // 76: 版本级别
    pub def_resuid: u16,             // 80: 默认保留 uid
    pub def_resgid: u16,             // 82: 默认保留 gid

    // 扩展字段
    pub first_ino: u32,              // 84: 第一个非保留 inode
    pub inode_size: u16,             // 88: inode 大小
    pub block_group_nr: u16,         // 90: 本超级块所在的块组号
    pub feature_compat: u32,         // 92: 兼容特性
    pub feature_incompat: u32,       // 96: 不兼容特性
    pub feature_ro_compat: u32,      // 100: 只读兼容特性

    pub uuid: [u8; 16],              // 104: 128位UUID
    pub volume_name: [u8; 16],       // 120: 卷名称
    pub last_mounted: [u8; 64],      // 136: 最后挂载路径
    pub algorithm_usage_bitmap: u32, // 200: 压缩算法位图

    pub prealloc_blocks: u8,         // 204: 预分配块数
    pub prealloc_dir_blocks: u8,     // 205: 目录预分配块数
    pub reserved_gdt_blocks: u16,    // 206: 保留的GDT块数

    pub journal_uuid: [u8; 16],      // 208: 日志UUID
    pub journal_inum: u32,           // 224: 日志inode号
    pub journal_dev: u32,            // 228: 日志设备号
    pub last_orphan: u32,            // 232: 孤儿inode链表头
    pub hash_seed: [u32; 4],         // 236: HTREE哈希种子
    pub def_hash_version: u8,        // 252: 默认哈希版本
    pub jnl_backup_type: u8,         // 253: 日志备份类型
    pub desc_size: u16,              // 254: 组描述符大小
    pub default_mount_opts: u32,     // 256: 默认挂载选项
    pub first_meta_bg: u32,          // 260: 第一个元数据块组
    pub mkfs_time: u32,              // 264: 创建时间
    pub jnl_blocks: [u32; 17],       // 268: 日志备份

    // 64位支持字段
    pub blocks_count_hi: u32,        // 336: 总块数（高32位）
    pub r_blocks_count_hi: u32,      // 340: 保留块数（高32位）
    pub free_blocks_count_hi: u32,   // 344: 空闲块数（高32位）
    pub min_extra_isize: u16,        // 348: 最小额外inode大小
    pub want_extra_isize: u16,       // 350: 期望额外inode大小
    pub flags: u32,                  // 352: 标志
    pub raid_stride: u16,            // 356: RAID步长
    pub mmp_interval: u16,           // 358: MMP检查间隔
    pub mmp_block: u64,              // 360: MMP块号
    pub raid_stripe_width: u32,      // 368: RAID条带宽度
    pub log_groups_per_flex: u8,     // 372: flex_bg组大小log2
    pub checksum_type: u8,           // 373: 校验和类型
    pub reserved_pad: u16,           // 374: 保留填充
    pub kbytes_written: u64,         // 376: 已写入的KB数
    pub snapshot_inum: u32,          // 384: 快照inode号
    pub snapshot_id: u32,            // 388: 快照ID
    pub snapshot_r_blocks_count: u64, // 392: 快照保留块数
    pub snapshot_list: u32,          // 400: 快照链表头
    pub error_count: u32,            // 404: 错误计数
    pub first_error_time: u32,       // 408: 第一次错误时间
    pub first_error_ino: u32,        // 412: 第一次错误inode
    pub first_error_block: u64,      // 416: 第一次错误块号
    pub first_error_func: [u8; 32],  // 424: 第一次错误函数
    pub first_error_line: u32,       // 456: 第一次错误行号
    pub last_error_time: u32,        // 460: 最后错误时间
    pub last_error_ino: u32,         // 464: 最后错误inode
    pub last_error_line: u32,        // 468: 最后错误行号
    pub last_error_block: u64,       // 472: 最后错误块号
    pub last_error_func: [u8; 32],   // 480: 最后错误函数
    pub mount_opts: [u8; 64],        // 512: 挂载选项
    pub usr_quota_inum: u32,         // 576: 用户配额inode
    pub grp_quota_inum: u32,         // 580: 组配额inode
    pub overhead_blocks: u32,        // 584: 开销块数
    pub backup_bgs: [u32; 2],        // 588: 备份块组
    pub encrypt_algos: [u8; 4],      // 596: 加密算法
    pub encrypt_pw_salt: [u8; 16],   // 600: 加密密码盐
    pub lpf_ino: u32,                // 616: lost+found inode
    pub prj_quota_inum: u32,         // 620: 项目配额inode
    pub checksum_seed: u32,          // 624: 校验和种子
    pub reserved: [u32; 98],         // 628: 保留字段
    pub checksum: u32,               // 1020: superblock校验和
}

impl ext4_sblock {
    /// 获取块大小（字节）
    pub fn block_size(&self) -> u32 {
        1024 << u32::from_le(self.log_block_size)
    }

    /// 获取 inode 大小
    pub fn inode_size(&self) -> u16 {
        let size = u16::from_le(self.inode_size);
        if size == 0 {
            128  // 默认值
        } else {
            size
        }
    }

    /// 获取总块数（合并高低32位）
    pub fn blocks_count(&self) -> u64 {
        (u32::from_le(self.blocks_count_lo) as u64)
            | ((u32::from_le(self.blocks_count_hi) as u64) << 32)
    }

    /// 获取空闲块数（合并高低32位）
    pub fn free_blocks_count(&self) -> u64 {
        (u32::from_le(self.free_blocks_count_lo) as u64)
            | ((u32::from_le(self.free_blocks_count_hi) as u64) << 32)
    }

    /// 计算块组数量
    pub fn block_group_count(&self) -> u32 {
        let blocks_count = self.blocks_count();
        let blocks_per_group = u32::from_le(self.blocks_per_group) as u64;
        blocks_count.div_ceil(blocks_per_group) as u32
    }

    /// 验证魔数
    pub fn is_valid(&self) -> bool {
        u16::from_le(self.magic) == EXT4_SUPERBLOCK_MAGIC
    }
}

/// Inode 结构
///
/// 对应 ext4 磁盘格式中的 inode (ext4_inode)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_inode {
    pub mode: u16,                   // 0: 文件模式
    pub uid: u16,                    // 2: 所有者 uid（低16位）
    pub size_lo: u32,                // 4: 文件大小（低32位）
    pub atime: u32,                  // 8: 访问时间
    pub ctime: u32,                  // 12: inode改变时间
    pub mtime: u32,                  // 16: 修改时间
    pub dtime: u32,                  // 20: 删除时间
    pub gid: u16,                    // 24: 组 gid（低16位）
    pub links_count: u16,            // 26: 硬链接数
    pub blocks_count_lo: u32,        // 28: 512B块数（低32位）
    pub flags: u32,                  // 32: 标志
    pub osd1: u32,                   // 36: OS相关1
    pub blocks: [u32; EXT4_INODE_BLOCKS], // 40: 块指针数组（15个）
    pub generation: u32,             // 100: 文件版本
    pub file_acl_lo: u32,            // 104: 文件 ACL（低32位）
    pub size_hi: u32,                // 108: 文件大小（高32位）
    pub obso_faddr: u32,             // 112: 废弃的fragment地址

    pub blocks_high: u16,            // 116: 块数高16位
    pub file_acl_high: u16,          // 118: ACL高16位
    pub uid_high: u16,               // 120: uid高16位
    pub gid_high: u16,               // 122: gid高16位
    pub checksum_lo: u16,            // 124: 校验和低16位
    pub reserved: u16,               // 126: 保留

    pub extra_isize: u16,            // 128: 额外inode大小
    pub checksum_hi: u16,            // 130: 校验和高16位
    pub ctime_extra: u32,            // 132: 额外change时间
    pub mtime_extra: u32,            // 136: 额外modification时间
    pub atime_extra: u32,            // 140: 额外access时间
    pub crtime: u32,                 // 144: 创建时间
    pub crtime_extra: u32,           // 148: 额外创建时间
    pub version_hi: u32,             // 152: 版本高32位
    pub projid: u32,                 // 156: 项目ID
}

impl ext4_inode {
    /// 获取文件大小（合并高低32位）
    pub fn file_size(&self) -> u64 {
        (u32::from_le(self.size_lo) as u64)
            | ((u32::from_le(self.size_hi) as u64) << 32)
    }

    /// 获取块数（512字节块，合并高低位）
    pub fn blocks_count(&self) -> u64 {
        (u32::from_le(self.blocks_count_lo) as u64)
            | ((self.blocks_high as u64) << 32)
    }

    /// 是否是目录
    pub fn is_dir(&self) -> bool {
        (u16::from_le(self.mode) & EXT4_INODE_MODE_TYPE_MASK) == EXT4_INODE_MODE_DIRECTORY
    }

    /// 是否是普通文件
    pub fn is_file(&self) -> bool {
        (u16::from_le(self.mode) & EXT4_INODE_MODE_TYPE_MASK) == EXT4_INODE_MODE_FILE
    }

    /// 是否是符号链接
    pub fn is_symlink(&self) -> bool {
        (u16::from_le(self.mode) & EXT4_INODE_MODE_TYPE_MASK) == EXT4_INODE_MODE_SOFTLINK
    }

    /// inode.blocks 字段的字节大小（15 * 4 = 60 字节）
    const BLOCKS_BYTE_SIZE: usize = EXT4_INODE_BLOCKS * core::mem::size_of::<u32>();

    /// 将 `blocks` 数组视为字节切片（extent 树根节点数据）
    ///
    /// inode 使用 extent 树时，`blocks[0..15]` 存储 extent 树根节点
    pub fn extent_root_data(&self) -> &[u8] {
        // SAFETY: blocks 是 [u32; 15]，repr(C)，视为 60 字节的 &[u8] 安全
        unsafe {
            core::slice::from_raw_parts(
                self.blocks.as_ptr() as *const u8,
                Self::BLOCKS_BYTE_SIZE,
            )
        }
    }

    /// 将 `blocks` 数组视为可变字节切片（extent 树根节点数据）
    pub fn extent_root_data_mut(&mut self) -> &mut [u8] {
        // SAFETY: 同 extent_root_data
        unsafe {
            core::slice::from_raw_parts_mut(
                self.blocks.as_mut_ptr() as *mut u8,
                Self::BLOCKS_BYTE_SIZE,
            )
        }
    }

    /// 获取 extent 树根节点头部的引用
    pub fn extent_header(&self) -> &ext4_extent_header {
        // SAFETY: blocks 至少 60 字节，ext4_extent_header 是 12 字节，对齐满足（u32 对齐）
        unsafe { &*(self.blocks.as_ptr() as *const ext4_extent_header) }
    }

    /// 获取 extent 树根节点头部的可变引用
    pub fn extent_header_mut(&mut self) -> &mut ext4_extent_header {
        // SAFETY: 同 extent_header
        unsafe { &mut *(self.blocks.as_mut_ptr() as *mut ext4_extent_header) }
    }
}

/// 目录项结构
///
/// 对应 ext4 磁盘格式中的目录项 (ext4_dir_entry_2)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_entry {
    pub inode: u32,                  // inode 编号
    pub rec_len: u16,                // 记录长度
    pub name_len: u8,                // 名称长度
    pub file_type: u8,               // 文件类型
    // 后面跟着变长的 name 字段
}

/// 目录项别名（与 ext4_dir_entry 相同）
pub type ext4_dir_en = ext4_dir_entry;

//=============================================================================
// 目录索引（HTree）相关结构
//=============================================================================

/// HTree 索引计数和限制结构
///
/// 对应 ext4 磁盘格式中的 ext4_dir_idx_climit
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_idx_climit {
    pub limit: u16,                  // 最大条目数
    pub count: u16,                  // 当前条目数
}

impl ext4_dir_idx_climit {
    /// 获取限制值
    pub fn limit(&self) -> u16 {
        u16::from_le(self.limit)
    }

    /// 获取计数值
    pub fn count(&self) -> u16 {
        u16::from_le(self.count)
    }

    /// 设置限制值
    pub fn set_limit(&mut self, val: u16) {
        self.limit = val.to_le();
    }

    /// 设置计数值
    pub fn set_count(&mut self, val: u16) {
        self.count = val.to_le();
    }
}

/// HTree 根节点的点（.）目录项
///
/// 对应 ext4 磁盘格式中的 ext4_fake_dir_entry
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_idx_dot_en {
    pub inode: u32,                  // inode 编号
    pub entry_len: u16,              // 记录长度
    pub name_len: u8,                // 名称长度（1 for "."）
    pub inode_type: u8,              // 文件类型
    pub name: [u8; 4],               // 名称 ".\0\0\0"
}

impl ext4_dir_idx_dot_en {
    /// 获取 inode 号
    pub fn inode(&self) -> u32 {
        u32::from_le(self.inode)
    }

    /// 获取记录长度
    pub fn entry_len(&self) -> u16 {
        u16::from_le(self.entry_len)
    }
}

/// HTree 根信息结构
///
/// 对应 ext4 磁盘格式中的 ext4_dir_idx_root_info
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_idx_rinfo {
    pub reserved_zero: u32,          // 保留字段，必须为 0
    pub hash_version: u8,            // 哈希版本
    pub info_length: u8,             // 信息长度（8字节）
    pub indirect_levels: u8,         // 间接层数
    pub unused_flags: u8,            // 未使用的标志
}

impl ext4_dir_idx_rinfo {
    /// 获取哈希版本
    pub fn hash_version(&self) -> u8 {
        self.hash_version
    }

    /// 获取信息长度
    pub fn info_length(&self) -> u8 {
        self.info_length
    }

    /// 获取间接层数
    pub fn indirect_levels(&self) -> u8 {
        self.indirect_levels
    }

    /// 设置哈希版本
    pub fn set_hash_version(&mut self, version: u8) {
        self.hash_version = version;
    }

    /// 设置信息长度
    pub fn set_info_length(&mut self, len: u8) {
        self.info_length = len;
    }

    /// 设置间接层数
    pub fn set_indirect_levels(&mut self, levels: u8) {
        self.indirect_levels = levels;
    }
}

/// HTree 索引条目
///
/// 对应 ext4 磁盘格式中的 ext4_dir_idx_entry
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_idx_entry {
    pub hash: u32,                   // 哈希值
    pub block: u32,                  // 块号
}

impl ext4_dir_idx_entry {
    /// 获取哈希值
    pub fn hash(&self) -> u32 {
        u32::from_le(self.hash)
    }

    /// 获取块号
    pub fn block(&self) -> u32 {
        u32::from_le(self.block)
    }

    /// 设置哈希值
    pub fn set_hash(&mut self, hash: u32) {
        self.hash = hash.to_le();
    }

    /// 设置块号
    pub fn set_block(&mut self, block: u32) {
        self.block = block.to_le();
    }
}

/// HTree 根节点结构
///
/// 对应 ext4 磁盘格式中的 ext4_dir_idx_root
/// 包含 "." 和 ".." 目录项以及根信息和索引条目
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_idx_root {
    pub dots: [ext4_dir_idx_dot_en; 2], // "." 和 ".." 目录项
    pub info: ext4_dir_idx_rinfo,    // 根信息
    pub en: [ext4_dir_idx_entry; 0], // 索引条目数组（变长）
}

/// HTree 索引节点结构
///
/// 对应 ext4 磁盘格式中的 ext4_dir_idx_node
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_idx_node {
    pub fake: ext4_fake_dir_entry,   // 假目录项
    pub entries: [ext4_dir_idx_entry; 0], // 索引条目数组（变长）
}

/// 假目录项（用于 HTree 索引节点）
///
/// 对应 ext4 磁盘格式中的 ext4_fake_dir_entry
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_fake_dir_entry {
    pub inode: u32,                  // inode 编号（通常为 0）
    pub entry_len: u16,              // 记录长度
    pub name_len: u8,                // 名称长度（0）
    pub inode_type: u8,              // 文件类型
}

/// HTree 索引尾部（校验和）
///
/// 对应 ext4 磁盘格式中的 ext4_dir_idx_tail
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_idx_tail {
    pub reserved: u32,               // 保留字段
    pub checksum: u32,               // 校验和
}

impl ext4_dir_idx_tail {
    /// 获取校验和
    pub fn checksum(&self) -> u32 {
        u32::from_le(self.checksum)
    }

    /// 设置校验和
    pub fn set_checksum(&mut self, csum: u32) {
        self.checksum = csum.to_le();
    }
}

/// 目录项尾部（校验和）
///
/// 对应 ext4 磁盘格式中的 ext4_dir_entry_tail
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_dir_entry_tail {
    pub reserved_zero1: u32,         // 保留字段 1
    pub rec_len: u16,                // 记录长度（通常为 12）
    pub reserved_zero2: u8,          // 保留字段 2
    pub reserved_ft: u8,             // 保留文件类型（0xDE）
    pub checksum: u32,               // 校验和
}

impl ext4_dir_entry_tail {
    /// 获取校验和
    pub fn checksum(&self) -> u32 {
        u32::from_le(self.checksum)
    }

    /// 设置校验和
    pub fn set_checksum(&mut self, csum: u32) {
        self.checksum = csum.to_le();
    }

    /// 获取记录长度
    pub fn rec_len(&self) -> u16 {
        u16::from_le(self.rec_len)
    }

    /// 设置记录长度
    pub fn set_rec_len(&mut self, len: u16) {
        self.rec_len = len.to_le();
    }
}

/// 块组描述符
///
/// 对应 ext4 磁盘格式中的块组描述符 (ext4_group_desc)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ext4_group_desc {
    pub block_bitmap_lo: u32,        // 块位图块号（低32位）
    pub inode_bitmap_lo: u32,        // inode位图块号（低32位）
    pub inode_table_lo: u32,         // inode表起始块号（低32位）
    pub free_blocks_count_lo: u16,   // 空闲块数（低16位）
    pub free_inodes_count_lo: u16,   // 空闲inode数（低16位）
    pub used_dirs_count_lo: u16,     // 目录数（低16位）
    pub flags: u16,                  // 标志
    pub exclude_bitmap_lo: u32,      // 排除位图块号（低32位）
    pub block_bitmap_csum_lo: u16,   // 块位图校验和（低16位）
    pub inode_bitmap_csum_lo: u16,   // inode位图校验和（低16位）
    pub itable_unused_lo: u16,       // 未使用inode数（低16位）
    pub checksum: u16,               // 校验和

    // 64位扩展字段
    pub block_bitmap_hi: u32,        // 块位图块号（高32位）
    pub inode_bitmap_hi: u32,        // inode位图块号（高32位）
    pub inode_table_hi: u32,         // inode表起始块号（高32位）
    pub free_blocks_count_hi: u16,   // 空闲块数（高16位）
    pub free_inodes_count_hi: u16,   // 空闲inode数（高16位）
    pub used_dirs_count_hi: u16,     // 目录数（高16位）
    pub itable_unused_hi: u16,       // 未使用inode数（高16位）
    pub exclude_bitmap_hi: u32,      // 排除位图块号（高32位）
    pub block_bitmap_csum_hi: u16,   // 块位图校验和（高16位）
    pub inode_bitmap_csum_hi: u16,   // inode位图校验和（高16位）
    pub reserved: u32,               // 保留
}

impl ext4_group_desc {
    /// 获取块位图块号（合并高低32位）
    pub fn block_bitmap(&self) -> u64 {
        (u32::from_le(self.block_bitmap_lo) as u64)
            | ((u32::from_le(self.block_bitmap_hi) as u64) << 32)
    }

    /// 获取 inode 位图块号（合并高低32位）
    pub fn inode_bitmap(&self) -> u64 {
        (u32::from_le(self.inode_bitmap_lo) as u64)
            | ((u32::from_le(self.inode_bitmap_hi) as u64) << 32)
    }

    /// 获取 inode 表起始块号（合并高低32位）
    pub fn inode_table(&self) -> u64 {
        (u32::from_le(self.inode_table_lo) as u64)
            | ((u32::from_le(self.inode_table_hi) as u64) << 32)
    }
}

//=============================================================================
// Extent 相关结构
//=============================================================================

/// Extent 树头部
///
/// 对应 ext4 磁盘格式中的 ext4_extent_header
/// 位于每个 extent 树节点的开头
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct ext4_extent_header {
    pub magic: u16,      // 魔数 0xF30A
    pub entries: u16,    // 当前节点中的有效 entry 数量
    pub max: u16,        // 节点中最大 entry 数量
    pub depth: u16,      // 树的深度，0 表示叶子节点
    pub generation: u32, // generation ID
}

impl ext4_extent_header {
    /// 检查魔数是否有效
    pub fn is_valid(&self) -> bool {
        u16::from_le(self.magic) == 0xF30A
    }

    /// 获取 entry 数量
    pub fn entries_count(&self) -> u16 {
        u16::from_le(self.entries)
    }

    /// 获取最大 entry 数量
    pub fn max_entries(&self) -> u16 {
        u16::from_le(self.max)
    }

    /// 获取深度
    pub fn depth(&self) -> u16 {
        u16::from_le(self.depth)
    }

    /// 是否是叶子节点
    pub fn is_leaf(&self) -> bool {
        self.depth() == 0
    }
}

/// Extent 叶子节点
///
/// 对应 ext4 磁盘格式中的 ext4_extent
/// 描述一段连续的物理块
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct ext4_extent {
    pub block: u32,    // 逻辑块号（文件内偏移）
    pub len: u16,      // extent 长度（块数）
    pub start_hi: u16, // 物理块号高 16 位
    pub start_lo: u32, // 物理块号低 32 位
}

impl ext4_extent {
    /// 获取逻辑块号
    pub fn logical_block(&self) -> u32 {
        u32::from_le(self.block)
    }

    /// 获取 extent 长度（块数）
    pub fn len(&self) -> u16 {
        u16::from_le(self.len)
    }

    /// 获取物理块号（合并高低位）
    pub fn physical_block(&self) -> u64 {
        (u32::from_le(self.start_lo) as u64)
            | ((u16::from_le(self.start_hi) as u64) << 32)
    }

    /// 检查 extent 是否初始化
    /// 未初始化的 extent 长度高位为 1
    pub fn is_initialized(&self) -> bool {
        const EXT4_EXT_UNWRITTEN_MASK: u16 = 0x8000;
        (u16::from_le(self.len) & EXT4_EXT_UNWRITTEN_MASK) == 0
    }

    /// 获取实际长度（去除初始化标志位）
    pub fn actual_len(&self) -> u16 {
        const EXT4_EXT_MAX_LEN: u16 = 0x7FFF;
        u16::from_le(self.len) & EXT4_EXT_MAX_LEN
    }
}

/// Extent 索引节点
///
/// 对应 ext4 磁盘格式中的 ext4_extent_idx
/// 指向下一层的 extent 树节点
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct ext4_extent_idx {
    pub block: u32,   // 逻辑块号（覆盖范围的起始）
    pub leaf_lo: u32, // 指向的块号低 32 位
    pub leaf_hi: u16, // 指向的块号高 16 位
    pub unused: u16,  // 保留
}

impl ext4_extent_idx {
    /// 获取逻辑块号
    pub fn logical_block(&self) -> u32 {
        u32::from_le(self.block)
    }

    /// 获取指向的物理块号（合并高低位）
    pub fn leaf_block(&self) -> u64 {
        let lo = u32::from_le(self.leaf_lo) as u64;
        let hi = u16::from_le(self.leaf_hi) as u64;
        let pblock = lo | (hi << 32);

        // 验证读取的物理块号是否合理
        // 如果leaf_hi非零，可能是损坏的extent index数据
        if hi > 0 {
            log::warn!(
                "[ext4_extent_idx::leaf_block] Reading extent index with non-zero leaf_hi: leaf_lo={:#x}, leaf_hi={:#x} ({} decimal), pblock={:#x}",
                lo as u32, hi as u16, hi, pblock
            );
        }

        pblock
    }
}

/// Extent 尾部结构
///
/// 用于存储 extent 块的 CRC32C 校验和
/// 位于所有 extent/index 条目之后
///
/// 对应 ext4 磁盘格式中的 ext4_extent_tail
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct ext4_extent_tail {
    /// CRC32C 校验和：crc32c(uuid + inum + extent_block)
    pub checksum: u32,
}

//=============================================================================
// Extended Attributes (xattr) 结构定义
//=============================================================================

/// xattr 块头部
///
/// 对应 ext4 磁盘格式中的 ext4_xattr_header
/// 位于独立 xattr 块的开头
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct ext4_xattr_header {
    pub h_magic: u32,       // 魔数：EXT4_XATTR_MAGIC (0xEA020000)
    pub h_refcount: u32,    // 引用计数（块共享）
    pub h_blocks: u32,      // 使用的块数（通常为 1）
    pub h_hash: u32,        // 所有条目的哈希值
    pub h_checksum: u32,    // CRC32C 校验和
    pub h_reserved: [u32; 3], // 保留字段
}

/// xattr inode 内部头部
///
/// 对应 ext4 磁盘格式中的 ext4_xattr_ibody_header
/// 位于 inode 的额外空间开头
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct ext4_xattr_ibody_header {
    pub h_magic: u32,       // 魔数：EXT4_XATTR_MAGIC
}

/// xattr 条目
///
/// 对应 ext4 磁盘格式中的 ext4_xattr_entry
/// 描述一个扩展属性的元数据
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct ext4_xattr_entry {
    pub e_name_len: u8,     // 名称长度
    pub e_name_index: u8,   // 命名空间索引
    pub e_value_offs: u16,  // 值在块中的偏移
    pub e_value_block: u32, // 值所在的块号（未使用，总是 0）
    pub e_value_size: u32,  // 值的大小
    pub e_hash: u32,        // 名称和值的哈希
}

impl ext4_xattr_entry {
    /// 获取名称长度
    pub fn name_len(&self) -> u8 {
        self.e_name_len
    }

    /// 获取命名空间索引
    pub fn name_index(&self) -> u8 {
        self.e_name_index
    }

    /// 获取值偏移
    pub fn value_offs(&self) -> u16 {
        u16::from_le(self.e_value_offs)
    }

    /// 获取值大小
    pub fn value_size(&self) -> u32 {
        u32::from_le(self.e_value_size)
    }

    /// 获取哈希值
    pub fn hash(&self) -> u32 {
        u32::from_le(self.e_hash)
    }
}
