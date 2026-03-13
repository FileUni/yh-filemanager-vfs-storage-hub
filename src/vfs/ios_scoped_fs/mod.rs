//! iOS security-scoped filesystem backend for OpenDAL.
//!
//! This backend provides path-based access to a user-picked directory by
//! resolving and holding a security-scoped bookmark.

mod backend;
mod core;
mod deleter;
mod lister;
mod reader;
mod writer;

pub use backend::IosScopedFsBuilder as IosScopedFs;

pub(super) const IOS_SCOPED_FS_SCHEME: &str = "ios_scoped_fs";
pub(super) const IOS_BOOKMARK_PREFIX: &str = "bookmark_b64:";
