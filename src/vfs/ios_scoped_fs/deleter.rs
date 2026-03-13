use super::core::IosScopedFsCore;
use opendal::raw::{OpDelete, oio};
use opendal::Result;
use std::sync::Arc;

#[derive(Debug)]
pub struct IosScopedFsDeleter {
    core: Arc<IosScopedFsCore>,
}

impl IosScopedFsDeleter {
    pub fn new(core: Arc<IosScopedFsCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for IosScopedFsDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let abs = self.core.abs_path(&path)?;
        match tokio::fs::metadata(&abs).await {
            Ok(meta) => {
                if meta.is_dir() {
                    tokio::fs::remove_dir(&abs)
                        .await
                        .map_err(opendal::raw::new_std_io_error)?;
                } else {
                    tokio::fs::remove_file(&abs)
                        .await
                        .map_err(opendal::raw::new_std_io_error)?;
                }
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(opendal::raw::new_std_io_error(err)),
        }
    }
}
