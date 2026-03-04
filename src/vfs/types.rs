// VFS
use serde::{Deserialize, Serialize};
use std::sync::Arc;
/// Pagination query parameters
#[derive(Debug, Clone, Default)]
pub struct VfsPaginationParams<'a> {
    pub page: i64,
    pub page_size: i64,
    pub sort_by: Option<&'a str>,
    pub order: Option<&'a str>,
    pub keyword: Option<&'a str>,
}
/// File Information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VfsFileInfo {
    pub name: Arc<str>,
    pub path: Arc<str>,
    pub is_dir: bool,
    pub size: u64,
    pub modified: Option<chrono::DateTime<chrono::Utc>>,
    //()
    #[serde(default)]
    pub favorite_color: i32, // 0=none, 1-7=colors
    pub has_active_share: Option<bool>,
    pub has_active_direct: Option<bool>,
    #[serde(default)]
    pub trashed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub original_path: Option<Arc<str>>,
}
/// File metadata (extended information)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VfsMetadata {
    pub path: Arc<str>,
    pub is_dir: bool,
    pub size: u64,
    pub modified: Option<chrono::DateTime<chrono::Utc>>,
    pub content_type: Option<Arc<str>>,
    pub etag: Option<Arc<str>>,
}
/// Batch operation result
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsBatchResult {
    pub success: Vec<Arc<str>>,
    pub failed: Vec<VfsBatchError>,
}
/// Batch Operation Error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VfsBatchError {
    pub path: Arc<str>,
    pub error: Arc<str>,
}
