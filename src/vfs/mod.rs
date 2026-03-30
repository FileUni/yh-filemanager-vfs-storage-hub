//! Virtual file system (VFS) core.
pub mod error;
pub mod journal;
pub mod maintenance_mode;
pub mod metrics;
pub mod traits;
pub mod types;
pub use error::{VfsError, VfsResult};
pub use journal::{
    VfsJournalEvent, VfsJournalRecorder, get_global_cache_journal_recorder,
    set_global_cache_journal_recorder,
};
pub use maintenance_mode::*;
pub use metrics::{VfsMetricsSnapshot, global_vfs_metrics, snapshot_global_vfs_metrics};
pub use traits::VfsStorage;
pub use types::{VfsBatchError, VfsBatchResult, VfsFileInfo, VfsMetadata, VfsPaginationParams};
pub mod batch;
pub mod cache;
pub mod connector;
pub mod hub;
pub mod maintenance;
pub mod mounted;
pub mod pool;
pub mod protected;
pub mod scoped;
pub mod task;
pub mod wal;

#[cfg(target_os = "android")]
pub mod android_saf;

#[cfg(target_os = "ios")]
pub mod ios_scoped_fs;
pub use hub::VfsStorageHub;
pub use mounted::{
    MountRuntime, MountedUserStorage, build_remote_storage, build_user_storage_with_mounts,
    sync_remote_mount_once,
};
pub use pool::VfsPool;
pub use scoped::ScopedVfsStorageEngine;
/// Logical temp file path prefix
pub const LOGICAL_TEMP_PREFIX: &str = "/.virtual/tmp";
