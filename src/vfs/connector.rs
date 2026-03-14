// Connector Management
//
// Build connectors for various storage backends based on OpenDAL
use crate::config::VfsConnectorConfig;
use crate::vfs::{VfsError, VfsResult};
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::Operator;

use std::collections::BTreeMap;

/// Build operator
pub async fn build_operator(config: &VfsConnectorConfig) -> VfsResult<Operator> {
    let driver = config.get_driver();
    let op = match driver {
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
        _ => {
            let mut opts: BTreeMap<String, String> = BTreeMap::new();

            if let Some(options) = &config.options {
                for (k, v) in options {
                    opts.insert(k.to_string(), v.to_string());
                }
            }

            // Backward compatibility for existing configs.
            // OpenDAL expects `access_key_id` and `secret_access_key`.
            if driver == "s3" {
                if !opts.contains_key("access_key_id") {
                    if let Some(value) = opts.get("access_key").cloned() {
                        opts.insert("access_key_id".to_string(), value);
                    }
                }
                if !opts.contains_key("secret_access_key") {
                    if let Some(value) = opts.get("secret_key").cloned() {
                        opts.insert("secret_access_key".to_string(), value);
                    }
                }
                // Drop legacy keys to avoid ambiguity.
                opts.remove("access_key");
                opts.remove("secret_key");
            }

            if let Some(root) = &config.root {
                opts.insert("root".to_string(), root.as_ref().to_string());
            }

            Operator::via_iter(driver, opts.into_iter())?
        }
    };

    // Add basic layers
    // business::services::file_index_service OpenDAL
    // Database-based metadata indexing has been implemented, so OpenDAL's local metadata cache is not needed here
    Ok(op.layer(LoggingLayer::default()).layer(RetryLayer::default()))
}
