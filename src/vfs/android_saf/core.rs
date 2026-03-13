use super::jni;
use super::{ANDROID_SAF_MIME_DIR, ANDROID_SAF_SCHEME};
use opendal::raw::{AccessorInfo, Capability, OpRead, OpWrite, oio};
use opendal::{Error, ErrorKind, Metadata, Result};
use std::sync::Arc;

#[derive(Debug)]
pub struct AndroidSafCore {
    pub(super) info: Arc<AccessorInfo>,
    pub(super) tree_uri: Arc<str>,
    pub(super) root_doc_id: Arc<str>,
    pub(super) buf_pool: oio::PooledBuf,
}

impl AndroidSafCore {
    pub fn new(tree_uri: &str) -> Result<Self> {
        let tree_uri = tree_uri.trim();
        if !tree_uri.starts_with("content://") {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "android_saf root must be a SAF tree uri (content://...)",
            )
            .with_context("root", tree_uri));
        }

        // Ensure we have a persisted read/write permission.
        jni::take_persistable_permission(tree_uri)?;

        let root_doc_id = jni::get_tree_document_id(tree_uri)?;
        let info = Self::build_info(tree_uri);

        Ok(Self {
            info,
            tree_uri: Arc::from(tree_uri),
            root_doc_id: Arc::from(root_doc_id),
            buf_pool: oio::PooledBuf::new(16).with_initial_capacity(256 * 1024),
        })
    }

    fn build_info(tree_uri: &str) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_scheme(ANDROID_SAF_SCHEME)
            .set_root(tree_uri)
            .set_name("android_saf")
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
                // We are a device-local backend.
                shared: true,
                ..Default::default()
            });
        // Ensure directory mime constant stays in sync and referenced.
        let _ = ANDROID_SAF_MIME_DIR;
        info.into()
    }

    pub async fn stat(&self, path: &str) -> Result<Metadata> {
        let tree_uri = Arc::clone(&self.tree_uri);
        let root_doc_id = Arc::clone(&self.root_doc_id);
        let path = path.to_string();
        tokio::task::spawn_blocking(move || jni::stat(&tree_uri, &root_doc_id, &path))
            .await
            .map_err(opendal::raw::new_task_join_error)?
    }

    pub async fn ensure_dir(&self, path: &str, create_intermediate: bool) -> Result<()> {
        let tree_uri = Arc::clone(&self.tree_uri);
        let root_doc_id = Arc::clone(&self.root_doc_id);
        let path = path.to_string();
        tokio::task::spawn_blocking(move || {
            jni::ensure_dir(&tree_uri, &root_doc_id, &path, create_intermediate)
        })
        .await
        .map_err(opendal::raw::new_task_join_error)?
    }

    pub async fn list(&self, path: &str) -> Result<Vec<oio::Entry>> {
        let tree_uri = Arc::clone(&self.tree_uri);
        let root_doc_id = Arc::clone(&self.root_doc_id);
        let path = path.to_string();
        tokio::task::spawn_blocking(move || jni::list(&tree_uri, &root_doc_id, &path))
            .await
            .map_err(opendal::raw::new_task_join_error)?
    }

    pub async fn copy(&self, from: &str, to: &str) -> Result<()> {
        let tree_uri = Arc::clone(&self.tree_uri);
        let root_doc_id = Arc::clone(&self.root_doc_id);
        let from = from.to_string();
        let to = to.to_string();
        tokio::task::spawn_blocking(move || jni::copy(&tree_uri, &root_doc_id, &from, &to))
            .await
            .map_err(opendal::raw::new_task_join_error)?
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let tree_uri = Arc::clone(&self.tree_uri);
        let root_doc_id = Arc::clone(&self.root_doc_id);
        let from = from.to_string();
        let to = to.to_string();
        tokio::task::spawn_blocking(move || jni::rename(&tree_uri, &root_doc_id, &from, &to))
            .await
            .map_err(opendal::raw::new_task_join_error)?
    }

    pub async fn delete_path(&self, path: &str) -> Result<()> {
        let tree_uri = Arc::clone(&self.tree_uri);
        let root_doc_id = Arc::clone(&self.root_doc_id);
        let path = path.to_string();
        tokio::task::spawn_blocking(move || jni::delete(&tree_uri, &root_doc_id, &path))
            .await
            .map_err(opendal::raw::new_task_join_error)?
    }

    pub async fn open_for_read(&self, path: &str, args: &OpRead) -> Result<(tokio::fs::File, usize)> {
        let offset = args.range().offset();
        let size = args.range().size().unwrap_or(u64::MAX) as usize;

        let tree_uri = Arc::clone(&self.tree_uri);
        let root_doc_id = Arc::clone(&self.root_doc_id);
        let path = path.to_string();
        let fd = tokio::task::spawn_blocking(move || {
            jni::open_read_fd(&tree_uri, &root_doc_id, &path)
        })
        .await
        .map_err(opendal::raw::new_task_join_error)??;

        // SAF file descriptors are unix fds.
        let std_file = unsafe { std::fs::File::from_raw_fd(fd) };
        let mut file = tokio::fs::File::from_std(std_file);

        if offset != 0 {
            use tokio::io::AsyncSeekExt;
            file.seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(opendal::raw::new_std_io_error)?;
        }

        Ok((file, size))
    }

    pub async fn open_for_write(&self, path: &str, args: &OpWrite) -> Result<tokio::fs::File> {
        let if_not_exists = args.if_not_exists();

        let tree_uri = Arc::clone(&self.tree_uri);
        let root_doc_id = Arc::clone(&self.root_doc_id);
        let path = path.to_string();
        let fd = tokio::task::spawn_blocking(move || {
            jni::open_write_fd(&tree_uri, &root_doc_id, &path, if_not_exists)
        })
        .await
        .map_err(opendal::raw::new_task_join_error)??;

        let std_file = unsafe { std::fs::File::from_raw_fd(fd) };
        Ok(tokio::fs::File::from_std(std_file))
    }
}

// Unix-only import used by from_raw_fd.
#[cfg(target_os = "android")]
use std::os::unix::io::FromRawFd;
