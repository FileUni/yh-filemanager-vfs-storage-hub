// Scoped VFS Storage Engine Module
//
// VFS
//  This module provides the VFS operation engine implementation within a specific user scope.
pub mod batch;
pub mod engine;
pub mod internal;
pub mod ops;
pub mod read;
pub mod storage_impl;
pub mod write;
pub use engine::ScopedVfsStorageEngine;
