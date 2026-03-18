// User-scoped VFS operation engine.
pub mod batch;
pub mod engine;
pub mod internal;
pub mod ops;
pub mod read;
pub mod storage_impl;
pub mod write;
pub use engine::ScopedVfsStorageEngine;
