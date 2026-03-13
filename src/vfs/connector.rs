// Connector Management
//
// Build connectors for various storage backends based on opendal
use crate::config::VfsConnectorConfig;
use crate::vfs::VfsError;
use crate::vfs::VfsResult;
use opendal::Operator;
use opendal::layers::{LoggingLayer, RetryLayer};
/// Build operator
pub async fn build_operator(config: &VfsConnectorConfig) -> VfsResult<Operator> {
    let driver = config.get_driver();
    let op = match driver {
        "fs" => {
            let mut builder = opendal::services::Fs::default();
            if let Some(root) = &config.root {
                builder = builder.root(root);
            }
            Operator::new(builder)?.finish()
        }
        "android_saf" => {
            #[cfg(target_os = "android")]
            {
                let mut builder = crate::vfs::android_saf::AndroidSaf::default();
                if let Some(root) = &config.root {
                    builder = builder.root(root);
                }
                Operator::new(builder)?.finish()
            }
            #[cfg(not(target_os = "android"))]
            {
                return Err(VfsError::Internal(
                    "android_saf connector is supported only on Android".to_string(),
                ));
            }
        }
        "ios_scoped_fs" => {
            #[cfg(target_os = "ios")]
            {
                let mut builder = crate::vfs::ios_scoped_fs::IosScopedFs::default();
                if let Some(root) = &config.root {
                    builder = builder.root(root);
                }
                Operator::new(builder)?.finish()
            }
            #[cfg(not(target_os = "ios"))]
            {
                return Err(VfsError::Internal(
                    "ios_scoped_fs connector is supported only on iOS".to_string(),
                ));
            }
        }
        "s3" => {
            let mut builder = opendal::services::S3::default();
            if let Some(root) = &config.root {
                builder = builder.root(root);
            }
            if let Some(endpoint) = config.options.as_ref().and_then(|m| m.get("endpoint")) {
                builder = builder.endpoint(endpoint);
            }
            if let Some(region) = config.options.as_ref().and_then(|m| m.get("region")) {
                builder = builder.region(region);
            }
            if let Some(bucket) = config.options.as_ref().and_then(|m| m.get("bucket")) {
                builder = builder.bucket(bucket);
            }
            if let Some(ak) = config.options.as_ref().and_then(|m| m.get("access_key")) {
                builder = builder.access_key_id(ak);
            }
            if let Some(sk) = config.options.as_ref().and_then(|m| m.get("secret_key")) {
                builder = builder.secret_access_key(sk);
            }
            Operator::new(builder)?.finish()
        }
        "webdav" => {
            let mut builder = opendal::services::Webdav::default();
            if let Some(endpoint) = config.options.as_ref().and_then(|m| m.get("endpoint")) {
                builder = builder.endpoint(endpoint);
            }
            if let Some(username) = config.options.as_ref().and_then(|m| m.get("username")) {
                builder = builder.username(username);
            }
            if let Some(password) = config.options.as_ref().and_then(|m| m.get("password")) {
                builder = builder.password(password);
            }
            if let Some(root) = &config.root {
                builder = builder.root(root);
            }
            Operator::new(builder)?.finish()
        }
        "memory" => {
            let builder = opendal::services::Memory::default();
            Operator::new(builder)?.finish()
        }
        _ => {
            return Err(VfsError::Internal(format!(
                "Unsupported driver: {}",
                driver
            )));
        }
    };
    // Add basic layers
    //business::services::file_index_service OpenDAL
    // Database-based metadata indexing has been implemented, so OpenDAL's local metadata cache is not needed here
    let op = op
        .layer(LoggingLayer::default())
        .layer(RetryLayer::default());
    Ok(op)
}
