// VFS
use sea_orm::DbErr;
#[derive(Debug, thiserror::Error)]
pub enum VfsError {
    #[error("Storage error: {0}")]
    OpenDal(Box<opendal::Error>),
    #[error("Database error: {0}")]
    Database(#[from] DbErr),
    #[error("VFS logical error: {0}")]
    Internal(String),
    #[error("Path not found: {0}")]
    NotFound(String),
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    #[error("Storage quota exceeded")]
    QuotaExceeded,
    #[error("System maintenance in progress (WAL recovery), please try again later")]
    MaintenanceMode,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
// Convert from opendal::Error
impl From<opendal::Error> for VfsError {
    fn from(err: opendal::Error) -> Self {
        if err.kind() == opendal::ErrorKind::NotFound {
            return VfsError::NotFound(err.to_string());
        }
        VfsError::OpenDal(Box::new(err))
    }
}
pub type VfsResult<T> = Result<T, VfsError>;
impl Clone for VfsError {
    fn clone(&self) -> Self {
        VfsError::Internal(self.to_string())
    }
}
