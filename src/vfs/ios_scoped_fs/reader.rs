use super::core::IosScopedFsCore;
use opendal::raw::oio;
use opendal::{Buffer, Result};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::ReadBuf;

#[derive(Debug)]
pub struct IosScopedFsReader {
    core: Arc<IosScopedFsCore>,
    file: tokio::fs::File,
    read: usize,
    size: usize,
    buf_size: usize,
}

impl IosScopedFsReader {
    pub fn new(core: Arc<IosScopedFsCore>, file: tokio::fs::File, size: usize) -> Self {
        Self {
            core,
            file,
            read: 0,
            size,
            buf_size: 2 * 1024 * 1024,
        }
    }
}

impl oio::Read for IosScopedFsReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size {
            return Ok(Buffer::new());
        }

        let mut bs = self.core.buf_pool.get();
        bs.reserve(self.buf_size);

        let size = (self.size - self.read).min(self.buf_size);
        let buf = &mut bs.spare_capacity_mut()[..size];
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: we will fill at most `size` bytes.
        unsafe {
            read_buf.assume_init(size);
        }

        let n = self
            .file
            .read_buf(&mut read_buf)
            .await
            .map_err(opendal::raw::new_std_io_error)?;
        self.read += n;

        let filled = read_buf.filled().len();
        // SAFETY: we make sure that bs contains `filled` bytes.
        unsafe {
            bs.set_len(filled);
        }

        let frozen = bs.split().freeze();
        self.core.buf_pool.put(bs);

        Ok(Buffer::from(frozen))
    }
}
