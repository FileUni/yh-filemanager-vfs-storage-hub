//! VFS utilities.
pub mod cache;
pub mod compression;
pub mod share_access;
pub mod temp_file;
pub use cache::VfsCache;
pub use compression::{
    CompressionFormat, CompressionOptions, DecompressionOptions, compress_task, decompress_task,
    stream_compress_reader,
};
pub use share_access::{
    is_direct_share_name_allowed, is_direct_share_path_allowed, is_path_within_root,
    normalize_share_relative_path, resolve_share_descendant_path,
};
pub use temp_file::{
    VfsTempDirGuard, VfsTempError, VfsTempFileGuard, VfsTempFileManager, get_global_temp_manager,
    init_global_temp_manager,
};
