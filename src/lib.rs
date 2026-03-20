// VFS Storage Hub Core Crate
pub mod business;
pub mod config;
pub mod utils;
pub mod vfs;
pub mod vfs_manager;
pub use business::services::init_vfs_tables;
pub use config::{
    VfsStorageHubConfig, get_vfs_hub_config, get_vfs_hub_config_manager,
    init_vfs_hub_config_manager,
};
pub use utils::temp_file::{get_global_temp_manager, init_global_temp_manager};
pub use vfs::{
    LOGICAL_TEMP_PREFIX, ScopedVfsStorageEngine, VfsBatchError, VfsBatchResult, VfsError,
    VfsFileInfo, VfsMetadata, VfsMetricsSnapshot, VfsStorage, VfsStorageHub,
    get_global_cache_journal_recorder, global_vfs_metrics, set_global_cache_journal_recorder,
    snapshot_global_vfs_metrics,
};
pub use vfs_manager::VfsManager;
