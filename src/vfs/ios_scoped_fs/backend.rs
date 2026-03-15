use super::core::IosScopedFsCore;
use super::deleter::IosScopedFsDeleter;
use super::lister::IosScopedFsLister;
use super::reader::IosScopedFsReader;
use super::writer::IosScopedFsWriter;
use super::IOS_SCOPED_FS_SCHEME;
use opendal::raw::{
    Access, AccessorInfo, OpCopy, OpCreateDir, OpList, OpRead, OpRename, OpStat, OpWrite,
    RpCopy, RpCreateDir, RpDelete, RpList, RpRead, RpRename, RpStat, RpWrite, oio,
};
use opendal::{Builder, EntryMode, Error, ErrorKind, Metadata, Result};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct IosScopedFsBuilder {
    root: Option<String>,
}

impl IosScopedFsBuilder {
    pub fn root(mut self, root: &str) -> Self {
        let trimmed = root.trim();
        self.root = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
        self
    }
}

impl Builder for IosScopedFsBuilder {
    type Config = ();

    fn build(self) -> Result<impl Access> {
        let root = self.root.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "ios_scoped_fs root is not specified")
        })?;
        let core = IosScopedFsCore::new(&root)?;
        Ok(IosScopedFsBackend {
            core: Arc::new(core),
        })
    }
}

#[derive(Debug, Clone)]
pub struct IosScopedFsBackend {
    core: Arc<IosScopedFsCore>,
}

impl Access for IosScopedFsBackend {
    type Reader = IosScopedFsReader;
    type Writer = IosScopedFsWriter;
    type Lister = IosScopedFsLister;
    type Deleter = oio::OneShotDeleter<IosScopedFsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let abs = self.core.abs_path(path)?;
        tokio::fs::create_dir_all(&abs)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let abs = self.core.abs_path(path)?;
        let meta = tokio::fs::metadata(&abs)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        let mode = if meta.is_dir() {
            EntryMode::DIR
        } else if meta.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let mut m = Metadata::new(mode).with_content_length(meta.len());
        if let Ok(modified) = meta.modified() {
            if let Ok(ts) = opendal::raw::Timestamp::try_from(modified) {
                m = m.with_last_modified(ts);
            }
        }
        Ok(RpStat::new(m))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let abs = self.core.abs_path(path)?;
        let mut f = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&abs)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        if args.range().offset() != 0 {
            use tokio::io::AsyncSeekExt;
            f.seek(std::io::SeekFrom::Start(args.range().offset()))
                .await
                .map_err(opendal::raw::new_std_io_error)?;
        }
        let size = args.range().size().unwrap_or(u64::MAX) as usize;
        Ok((
            RpRead::new(),
            IosScopedFsReader::new(Arc::clone(&self.core), f, size),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.append() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "ios_scoped_fs doesn't support append writes",
            ));
        }
        if args.concurrent() > 1 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "ios_scoped_fs doesn't support concurrent writes",
            ));
        }
        let abs = self.core.abs_path(path)?;
        if let Some(parent) = abs.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(opendal::raw::new_std_io_error)?;
        }
        let mut open_options = tokio::fs::OpenOptions::new();
        if args.if_not_exists() {
            open_options.create_new(true);
        } else {
            open_options.create(true);
        }
        open_options.write(true).truncate(true);
        let f = open_options
            .open(&abs)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        Ok((
            RpWrite::default(),
            IosScopedFsWriter::new(Arc::clone(&self.core), path.to_string(), f),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(IosScopedFsDeleter::new(Arc::clone(&self.core))),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let abs = self.core.abs_path(path)?;
        let mut out: Vec<oio::Entry> = Vec::new();

        let mut rd = match tokio::fs::read_dir(&abs).await {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok((RpList::default(), IosScopedFsLister::new(Vec::new())))
            }
            Err(err) => return Err(opendal::raw::new_std_io_error(err)),
        };

        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(opendal::raw::new_std_io_error)?
        {
            let file_type = entry
                .file_type()
                .await
                .map_err(opendal::raw::new_std_io_error)?;
            let is_dir = file_type.is_dir();
            let name = entry.file_name().to_string_lossy().to_string();
            let mut rel = if path.trim_end_matches('/') == "/" || path.trim().is_empty() {
                name
            } else {
                format!("{}/{}", path.trim_end_matches('/'), name)
            };
            if is_dir {
                rel.push('/');
            }

            let meta = entry
                .metadata()
                .await
                .map_err(opendal::raw::new_std_io_error)?;
            let mode = if is_dir { EntryMode::DIR } else { EntryMode::FILE };
            let mut m = Metadata::new(mode).with_content_length(meta.len());
            if let Ok(modified) = meta.modified() {
                if let Ok(ts) = opendal::raw::Timestamp::try_from(modified) {
                    m = m.with_last_modified(ts);
                }
            }
            out.push(oio::Entry::new(&rel, m));
        }

        Ok((RpList::default(), IosScopedFsLister::new(out)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let from_abs = self.core.abs_path(from)?;
        let to_abs = self.core.abs_path(to)?;
        tokio::fs::metadata(&from_abs)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        if let Some(parent) = to_abs.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(opendal::raw::new_std_io_error)?;
        }
        tokio::fs::copy(from_abs, to_abs)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from_abs = self.core.abs_path(from)?;
        let to_abs = self.core.abs_path(to)?;
        tokio::fs::metadata(&from_abs)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        if let Some(parent) = to_abs.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(opendal::raw::new_std_io_error)?;
        }
        tokio::fs::rename(from_abs, to_abs)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        Ok(RpRename::default())
    }
}

// Ensure scheme constant is referenced.
const _: &str = IOS_SCOPED_FS_SCHEME;
