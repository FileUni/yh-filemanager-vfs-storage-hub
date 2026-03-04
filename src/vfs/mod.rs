// VFS
//
// VFS
// OpenDAL
// Basic definitions
pub mod error;
pub mod journal;
pub mod maintenance_mode;
pub mod traits;
pub mod types;
pub use error::{VfsError, VfsResult};
pub use journal::{VfsJournalEvent, VfsJournalRecorder};
pub use maintenance_mode::*;
pub use traits::VfsStorage;
pub use types::{VfsBatchError, VfsBatchResult, VfsFileInfo, VfsMetadata, VfsPaginationParams};
// Implementation Layer
pub mod batch;
pub mod connector;
pub mod hub;
pub mod maintenance;
pub mod pool;
pub mod scoped;
pub mod task;
pub mod wal;
pub use hub::VfsStorageHub;
pub use pool::VfsPool;
pub use scoped::ScopedVfsStorageEngine;
/// Logical temp file path prefix
pub const LOGICAL_TEMP_PREFIX: &str = "/.virtual/tmp";
