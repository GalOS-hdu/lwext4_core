//! Extent 路径类型定义

use crate::types::ext4_extent_header;
use alloc::vec::Vec;

/// 表示从根到叶子的路径上的一个节点
///
/// 对应 lwext4 的 `struct ext4_extent_path`
#[derive(Debug)]
pub struct ExtentPathNode {
    /// 节点所在的物理块地址
    pub block_addr: u64,

    /// 节点深度（0 = 叶子）
    pub depth: u16,

    /// Extent header
    pub header: ext4_extent_header,

    /// 当前索引位置（在索引节点中）
    pub index_pos: usize,

    /// 节点类型
    pub node_type: ExtentNodeType,
}

/// Extent 节点类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtentNodeType {
    /// 根节点（在 inode 中）
    Root,

    /// 索引节点
    Index,

    /// 叶子节点
    Leaf,
}

/// Extent 路径
///
/// 表示从 inode 根节点到目标 extent 的完整路径
///
/// 对应 lwext4 的 `struct ext4_extent_path` 数组
#[derive(Debug)]
pub struct ExtentPath {
    /// 路径上的所有节点（从根到叶）
    pub nodes: Vec<ExtentPathNode>,

    /// 最大深度
    pub max_depth: u16,
}

impl ExtentPath {
    /// 创建新的 extent 路径
    pub fn new(max_depth: u16) -> Self {
        Self {
            nodes: Vec::with_capacity(max_depth as usize + 1),
            max_depth,
        }
    }

    /// 获取当前深度
    pub fn depth(&self) -> u16 {
        if self.nodes.is_empty() {
            0
        } else {
            self.nodes.len() as u16 - 1
        }
    }

    /// 获取叶子节点
    pub fn leaf(&self) -> Option<&ExtentPathNode> {
        self.nodes.last()
    }

    /// 获取叶子节点（可变）
    pub fn leaf_mut(&mut self) -> Option<&mut ExtentPathNode> {
        self.nodes.last_mut()
    }

    /// 添加节点到路径
    pub fn push(&mut self, node: ExtentPathNode) {
        self.nodes.push(node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extent_path_creation() {
        let path = ExtentPath::new(2);
        assert_eq!(path.max_depth, 2);
        assert_eq!(path.depth(), 0);
    }

    #[test]
    fn test_extent_node_type() {
        let node_type = ExtentNodeType::Leaf;
        assert_eq!(node_type, ExtentNodeType::Leaf);
        assert_ne!(node_type, ExtentNodeType::Index);
    }
}
