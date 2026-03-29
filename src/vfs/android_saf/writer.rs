use super::core::AndroidSafCore;
use bytes::Buf;
use opendal::raw::oio;
use opendal::{Buffer, Error, ErrorKind, Metadata, Result};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub struct AndroidSafWriter {
    core: Arc<AndroidSafCore>,
    path: String,
    file: tokio::fs::File,
}

impl AndroidSafWriter {
    pub fn new(core: Arc<AndroidSafCore>, path: &str, file: tokio::fs::File) -> Self {
        Self {
            core,
            path: path.to_string(),
            file,
        }
    }
}

// SAF writers are only used via `&mut self` behind OpenDAL.
// tokio::fs::File is not Sync, but our usage is synchronized by the caller.
unsafe impl Sync for AndroidSafWriter {}

impl oio::Write for AndroidSafWriter {
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
        self.core.stat(&self.path).await
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "android_saf doesn't support abort",
        ))
    }
}
