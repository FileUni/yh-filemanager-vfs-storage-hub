use sea_orm::DbErr;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum VfsCommonError {
    #[error("Database error: {0}")]
    Database(#[from] DbErr),
    #[error("Internal error: {0}")]
    Internal(String),
}
pub type VfsCommonResult<T> = Result<T, VfsCommonError>;
