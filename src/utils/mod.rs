// Utility Layer
//
// Provides utilities for temporary file management, compression/decompression, caching, etc.
pub mod cache;
pub mod compression;
pub mod temp_file;
pub use cache::VfsCache;
pub use compression::{
    CompressionFormat, CompressionOptions, DecompressionOptions, compress_task, decompress_task,
    stream_compress_reader,
};
pub use temp_file::{
    VfsTempDirGuard, VfsTempError, VfsTempFileGuard, VfsTempFileManager, get_global_temp_manager,
    init_global_temp_manager,
};
