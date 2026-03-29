use super::core::IosScopedFsCore;
use bytes::Buf;
use opendal::raw::oio;
use opendal::{Buffer, EntryMode, Error, ErrorKind, Metadata, Result};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub struct IosScopedFsWriter {
    core: Arc<IosScopedFsCore>,
    path: String,
    file: tokio::fs::File,
}

impl IosScopedFsWriter {
    pub fn new(core: Arc<IosScopedFsCore>, path: &str, file: tokio::fs::File) -> Self {
        Self {
            core,
            path: path.to_string(),
            file,
        }
    }
}

unsafe impl Sync for IosScopedFsWriter {}

impl oio::Write for IosScopedFsWriter {
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        while bs.has_remaining() {
            let n = self
                .file
                .write(bs.chunk())
                .await
                .map_err(opendal::raw::new_std_io_error)?;
            bs.advance(n);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.file
            .flush()
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        self.file
            .sync_all()
            .await
            .map_err(opendal::raw::new_std_io_error)?;

        let abs = self.core.abs_path(&self.path)?;
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
        Ok(m)
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "ios_scoped_fs doesn't support abort",
        ))
    }
}
