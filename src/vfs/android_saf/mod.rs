//! Android SAF (Storage Access Framework) backend for OpenDAL.
//!
//! This backend provides path-based file operations backed by a user-granted
//! SAF document tree (`content://...`). It is designed for Google Play
//! compliance: no `MANAGE_EXTERNAL_STORAGE` is required.

mod backend;
mod core;
mod deleter;
mod jni;
mod lister;
mod reader;
mod writer;

pub use backend::AndroidSafBuilder as AndroidSaf;

pub(super) const ANDROID_SAF_SCHEME: &str = "android_saf";
pub(super) const ANDROID_SAF_MIME_DIR: &str = "vnd.android.document/directory";
