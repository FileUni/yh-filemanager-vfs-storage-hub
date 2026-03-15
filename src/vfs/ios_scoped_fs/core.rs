use super::{IOS_BOOKMARK_PREFIX, IOS_SCOPED_FS_SCHEME};
use base64::{engine::general_purpose, Engine as _};
use objc2::runtime::Bool;
use objc2_foundation::{NSData, NSURLBookmarkResolutionOptions, NSURL};
use opendal::raw::{oio, AccessorInfo};
use opendal::{Capability, Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug)]
struct SecurityScopedAccessGuard {
    url: objc2::rc::Retained<NSURL>,
}

impl Drop for SecurityScopedAccessGuard {
    fn drop(&mut self) {
        // SAFETY: This is an iOS runtime API.
        unsafe {
            self.url.stopAccessingSecurityScopedResource();
        }
    }
}

#[derive(Debug)]
pub struct IosScopedFsCore {
    pub(super) info: Arc<AccessorInfo>,
    pub(super) root: PathBuf,
    _guard: SecurityScopedAccessGuard,
    pub(super) buf_pool: oio::PooledBuf,
}

impl IosScopedFsCore {
    pub fn new(root: &str) -> Result<Self> {
        let raw = root.trim();
        let b64 = raw.strip_prefix(IOS_BOOKMARK_PREFIX).ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "ios_scoped_fs root must start with 'bookmark_b64:'",
            )
            .with_context("root", raw)
        })?;

        let bytes = general_purpose::STANDARD.decode(b64.trim()).map_err(|e| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "failed to decode iOS bookmark base64",
            )
            .set_source(e)
        })?;

        let data = unsafe { NSData::dataWithBytes_length(bytes.as_ptr().cast(), bytes.len()) };

        let mut is_stale = Bool::new(false);
        let is_stale_ptr: *mut Bool = &mut is_stale;

        let url = unsafe {
            NSURL::URLByResolvingBookmarkData_options_relativeToURL_bookmarkDataIsStale_error(
                &data,
                NSURLBookmarkResolutionOptions::WithSecurityScope
                    | NSURLBookmarkResolutionOptions::WithoutUI
                    | NSURLBookmarkResolutionOptions::WithoutMounting,
                None,
                is_stale_ptr,
            )
        }
        .map_err(|e| {
            Error::new(
                ErrorKind::PermissionDenied,
                "failed to resolve security-scoped bookmark",
            )
            .set_source(e)
        })?;

        if is_stale.as_bool() {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "security-scoped bookmark is stale; user must re-pick the directory",
            ));
        }

        let ok = unsafe { url.startAccessingSecurityScopedResource() };
        if !ok {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "startAccessingSecurityScopedResource returned false",
            ));
        }

        let root_path = url.to_file_path().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "failed to get file path from NSURL")
        })?;

        if !root_path.is_absolute() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "resolved root path is not absolute",
            )
            .with_context("root", root_path.to_string_lossy()));
        }

        let info = Self::build_info(raw, &root_path);

        Ok(Self {
            info,
            root: root_path,
            _guard: SecurityScopedAccessGuard { url },
            buf_pool: oio::PooledBuf::new(16).with_initial_capacity(256 * 1024),
        })
    }

    fn build_info(raw_root: &str, root_path: &Path) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_scheme(IOS_SCOPED_FS_SCHEME)
            .set_root(raw_root)
            .set_name("ios_scoped_fs")
            .set_native_capability(Capability {
                stat: true,
                read: true,
                write: true,
                write_can_empty: true,
                create_dir: true,
                delete: true,
                list: true,
                copy: true,
                rename: true,
                shared: true,
                ..Default::default()
            });
        let _ = root_path;
        info.into()
    }

    pub fn abs_path(&self, path: &str) -> Result<PathBuf> {
        let p = path.trim_start_matches('/').trim_end_matches('/');
        // Root dir is represented by empty path.
        if p.is_empty() {
            return Ok(self.root.clone());
        }
        Ok(self.root.join(p))
    }
}
