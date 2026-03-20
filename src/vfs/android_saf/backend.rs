use super::core::AndroidSafCore;
use super::deleter::AndroidSafDeleter;
use super::lister::AndroidSafLister;
use super::reader::AndroidSafReader;
use super::writer::AndroidSafWriter;
use opendal::raw::{
    Access, AccessorInfo, OpCopy, OpCreateDir, OpList, OpRead, OpRename, OpStat, OpWrite, RpCopy,
    RpCreateDir, RpDelete, RpList, RpRead, RpRename, RpStat, RpWrite, oio,
};
use opendal::{Builder, Error, ErrorKind, Metadata, Result};
use std::sync::Arc;

/// Android SAF backend builder.
///
/// `root` holds an Android SAF tree URI, for example: `content://...`.
#[derive(Debug, Default)]
pub struct AndroidSafBuilder {
    tree_uri: Option<String>,
}

impl AndroidSafBuilder {
    pub fn root(mut self, tree_uri: &str) -> Self {
        let trimmed = tree_uri.trim();
        self.tree_uri = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
        self
    }
}

impl Builder for AndroidSafBuilder {
    type Config = ();

    fn build(self) -> Result<impl Access> {
        let tree_uri = self.tree_uri.ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "android_saf root (tree uri) is not specified",
            )
        })?;

        // Core init performs basic validation and ensures we have persistable permission.
        let core = AndroidSafCore::new(&tree_uri)?;

        Ok(AndroidSafBackend {
            core: Arc::new(core),
        })
    }
}

#[derive(Debug, Clone)]
pub struct AndroidSafBackend {
    core: Arc<AndroidSafCore>,
}

impl Access for AndroidSafBackend {
    type Reader = AndroidSafReader;
    type Writer = AndroidSafWriter;
    type Lister = AndroidSafLister;
    type Deleter = oio::OneShotDeleter<AndroidSafDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.ensure_dir(path, true).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let meta: Metadata = self.core.stat(path).await?;
        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let (file, size) = self.core.open_for_read(path, &args).await?;
        let reader = AndroidSafReader::new(Arc::clone(&self.core), file, size);
        Ok((RpRead::new(), reader))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.append() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "android_saf doesn't support append writes",
            ));
        }

        // For now we only support single-stream writes.
        if args.concurrent() > 1 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "android_saf doesn't support concurrent writes",
            ));
        }

        let file = self.core.open_for_write(path, &args).await?;
        let writer = AndroidSafWriter::new(Arc::clone(&self.core), path.to_string(), file);
        Ok((RpWrite::default(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(AndroidSafDeleter::new(Arc::clone(&self.core))),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        // List non-exist dir should return empty.
        let entries = match self.core.list(path).await {
            Ok(v) => v,
            Err(err) if err.kind() == ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };
        Ok((RpList::default(), AndroidSafLister::new(entries)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        self.core.copy(from, to).await?;
        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.core.rename(from, to).await?;
        Ok(RpRename::default())
    }
}

// Accessor info is constructed by `AndroidSafCore`.
