use super::core::AndroidSafCore;
use opendal::Result;
use opendal::raw::{OpDelete, oio};
use std::sync::Arc;

#[derive(Debug)]
pub struct AndroidSafDeleter {
    core: Arc<AndroidSafCore>,
}

impl AndroidSafDeleter {
    pub fn new(core: Arc<AndroidSafCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for AndroidSafDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        self.core.delete_path(&path).await
    }
}
