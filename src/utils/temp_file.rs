use crate::config::VfsTempFileConfig;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use yh_console_log::yhlog;
/// VFS temporary file manager
///
/// User-isolated temporary file management with active file tracking and automatic cleanup
#[derive(Debug, Clone)]
pub struct VfsTempFileManager {
    temp_dir: PathBuf,
    max_age_seconds: u64,
    active_files: Arc<RwLock<HashSet<PathBuf>>>,
    cancel_token: CancellationToken,
}
impl VfsTempFileManager {
    /// Create a new temporary file manager
    pub fn new() -> Self {
        Self::default()
    }
    /// Create manager from configuration
    pub fn from_config(config: &VfsTempFileConfig) -> Self {
        let temp_dir_str = yh_config_infra::config_require_str!(config.dir, "vfs_storage_hub", "temp_file.dir");
        if temp_dir_str.trim().is_empty() {
            eprintln!("[vfs_storage_hub] temp_file.dir cannot be empty. Please configure it in .toml");
            std::process::exit(78);
        }
        let temp_dir = PathBuf::from(temp_dir_str);
        Self {
            temp_dir,
            max_age_seconds: config.get_max_age(),
            active_files: Arc::new(RwLock::new(HashSet::new())),
            cancel_token: CancellationToken::new(),
        }
    }
    /// Create manager with specified temporary directory
    pub fn with_temp_dir<P: AsRef<Path>>(temp_dir: P) -> Self {
        let temp_dir = temp_dir.as_ref().to_path_buf();
        Self {
            temp_dir,
            max_age_seconds: 3600,
            active_files: Arc::new(RwLock::new(HashSet::new())),
            cancel_token: CancellationToken::new(),
        }
    }
    /// Set maximum file age
    pub fn with_max_age(mut self, seconds: u64) -> Self {
        self.max_age_seconds = seconds;
        self
    }
    /// Get user-specific temporary directory
    pub fn get_user_temp_dir(&self, user_id: &str) -> PathBuf {
        self.temp_dir.join(user_id)
    }
    /// Create temporary file for user
    pub async fn create_user_temp_file(&self, user_id: &str, prefix: &str) -> Result<(PathBuf, VfsTempFileGuard), VfsTempError> {
        let user_temp_dir = self.get_user_temp_dir(user_id);
        fs::create_dir_all(&user_temp_dir).await.map_err(VfsTempError::Io)?;
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%f").to_string();
        let random_suffix: String = (0..8).map(|_| char::from(b'a' + (std::process::id() as u8 % 26))).collect();
        let temp_filename = format!("{}_{}_{}", prefix, timestamp, random_suffix);
        let temp_path = user_temp_dir.join(temp_filename);
        fs::File::create(&temp_path).await.map_err(VfsTempError::Io)?;
        // Mark as active file to prevent accidental deletion by cleanup task
        self.active_files.write().await.insert(temp_path.to_path_buf());
        yhlog("debug", &format!("Created user temp file: {}", temp_path.display()));
        // Return file path and guard
        let guard = VfsTempFileGuard {
            path: temp_path.to_path_buf(),
            active_files: Arc::clone(&self.active_files),
            cleanup_on_drop: true,
            vfs_cleanup: None,
        };
        Ok((temp_path, guard))
    }
    /// Create temporary directory for user
    pub async fn create_user_temp_dir(&self, user_id: &str, prefix: &str) -> Result<(PathBuf, VfsTempDirGuard), VfsTempError> {
        let user_temp_dir = self.get_user_temp_dir(user_id);
        fs::create_dir_all(&user_temp_dir).await.map_err(VfsTempError::Io)?;
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%f").to_string();
        let random_suffix: String = (0..8).map(|_| char::from(b'a' + (std::process::id() as u8 % 26))).collect();
        let temp_dir_name = format!("{}_{}_{}", prefix, timestamp, random_suffix);
        let temp_dir_path = user_temp_dir.join(temp_dir_name);
        fs::create_dir_all(&temp_dir_path).await.map_err(VfsTempError::Io)?;
        // Mark as active directory to prevent accidental deletion by cleanup task
        self.active_files.write().await.insert(temp_dir_path.to_path_buf());
        yhlog("debug", &format!("Created user temp directory: {}", temp_dir_path.display()));
        // Return directory path and guard
        let guard = VfsTempDirGuard {
            path: temp_dir_path.to_path_buf(),
            active_files: Arc::clone(&self.active_files),
            cleanup_on_drop: true,
            vfs_cleanup: None,
        };
        Ok((temp_dir_path, guard))
    }
    /// Immediately clean up specified temporary file or directory
    pub async fn cleanup_immediately(&self, path: &Path) -> Result<(), VfsTempError> {
        // First remove from active list
        self.active_files.write().await.remove(path);
        if path.exists() {
            if path.is_dir() {
                tokio::fs::remove_dir_all(path).await.map_err(VfsTempError::Io)?;
            } else {
                tokio::fs::remove_file(path).await.map_err(VfsTempError::Io)?;
            }
            yhlog("debug", &format!("Immediately cleaned up: {}", path.display()));
        }
        Ok(())
    }
    /// Clean up expired temporary files (scheduled task)
    pub async fn cleanup_temp_files(&self) -> Result<usize, VfsTempError> {
        if !self.temp_dir.exists() {
            return Ok(0);
        }
        let cutoff_time = std::time::SystemTime::now() - std::time::Duration::from_secs(self.max_age_seconds);
        let active_files = self.active_files.read().await;
        let mut cleaned_count = 0;
        // Use unified cleanup function
        cleanup_temp_files_at_path_with_tracking(&self.temp_dir, &cutoff_time, &active_files, &mut cleaned_count).await?;
        if cleaned_count > 0 {
            yhlog("info", &format!("Cleaned {} expired temporary files", cleaned_count));
        }
        Ok(cleaned_count)
    }
    /// Stop automatic cleanup task
    pub fn shutdown(&self) {
        self.cancel_token.cancel();
    }
}
impl Default for VfsTempFileManager {
    fn default() -> Self {
        Self {
            temp_dir: std::env::temp_dir().join("vfs-storage-hub"),
            max_age_seconds: 3600,
            active_files: Arc::new(RwLock::new(HashSet::new())),
            cancel_token: CancellationToken::new(),
        }
    }
}
/// RAII
pub struct VfsTempFileGuard {
    path: PathBuf,
    active_files: Arc<RwLock<HashSet<PathBuf>>>,
    cleanup_on_drop: bool,
    // Optional VFS operator and logical path, if present cleanup via VFS
    vfs_cleanup: Option<(opendal::Operator, String)>,
}
impl VfsTempFileGuard {
    pub fn without_cleanup(mut self) -> Self {
        self.cleanup_on_drop = false;
        self
    }
    /// Set VFS cleanup info
    pub fn with_vfs_cleanup(mut self, op: opendal::Operator, logical_path: String) -> Self {
        self.vfs_cleanup = Some((op, logical_path));
        self
    }
}
impl Drop for VfsTempFileGuard {
    fn drop(&mut self) {
        // Remove from active list
        if let Ok(mut active) = self.active_files.try_write() {
            active.remove(&self.path);
        }
        // If auto-cleanup is configured
        if self.cleanup_on_drop {
            if let Some((op, logical_path)) = self.vfs_cleanup.take() {
                // VFS takes over cleanup
                tokio::spawn(async move {
                    if let Err(e) = op.delete(&logical_path).await {
                        yhlog("warn", &format!("VFS Cleanup: Failed to delete virtual temp file {}: {}", logical_path, e));
                    }
                });
            } else {
                // Direct physical operation - use spawn_blocking to avoid blocking async runtime
                let path = std::mem::take(&mut self.path);
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = std::fs::remove_file(&path) {
                        // Only warn if file exists
                        if e.kind() != std::io::ErrorKind::NotFound {
                            yhlog("warn", &format!("Failed to cleanup temp file {:?}: {}", path, e));
                        }
                    }
                });
            }
        }
    }
}
/// RAII/ Temporary directory guard (RAII pattern)
pub struct VfsTempDirGuard {
    path: PathBuf,
    active_files: Arc<RwLock<HashSet<PathBuf>>>,
    cleanup_on_drop: bool,
    vfs_cleanup: Option<(opendal::Operator, String)>,
}
impl VfsTempDirGuard {
    /// Create guard without auto-cleanup
    pub fn without_cleanup(mut self) -> Self {
        self.cleanup_on_drop = false;
        self
    }
    /// Set VFS cleanup info
    pub fn with_vfs_cleanup(mut self, op: opendal::Operator, logical_path: String) -> Self {
        self.vfs_cleanup = Some((op, logical_path));
        self
    }
}
impl Drop for VfsTempDirGuard {
    fn drop(&mut self) {
        // Remove from active list
        if let Ok(mut active) = self.active_files.try_write() {
            active.remove(&self.path);
        }
        // If auto-cleanup is configured
        if self.cleanup_on_drop {
            if let Some((op, logical_path)) = self.vfs_cleanup.take() {
                // VFS takes over cleanup
                tokio::spawn(async move {
                    if let Err(e) = op.remove_all(&logical_path).await {
                        yhlog("warn", &format!("VFS Cleanup: Failed to remove virtual temp directory {}: {}", logical_path, e));
                    }
                });
            } else {
                // Direct physical operation - use spawn_blocking to avoid blocking async runtime
                let path = std::mem::take(&mut self.path);
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = std::fs::remove_dir_all(&path)
                        && e.kind() != std::io::ErrorKind::NotFound
                    {
                        yhlog("warn", &format!("Failed to cleanup temp directory {:?}: {}", path, e));
                    }
                });
            }
        }
    }
}
/// Temporary file error types
#[derive(thiserror::Error, Debug)]
pub enum VfsTempError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Other error: {0}")]
    Other(String),
}
/// Internal function to clean up temporary files at specified path (with active file tracking)
async fn cleanup_temp_files_at_path_with_tracking(temp_dir: &Path, cutoff_time: &std::time::SystemTime, active_files: &HashSet<PathBuf>, cleaned_count: &mut usize) -> Result<(), VfsTempError> {
    if !temp_dir.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(temp_dir).await.map_err(VfsTempError::Io)?;
    while let Some(entry) = entries.next_entry().await.map_err(VfsTempError::Io)? {
        let path = entry.path();
        if path.is_dir() {
            // Recursively process subdirectories
            Box::pin(cleanup_temp_files_at_path_with_tracking(&path, cutoff_time, active_files, cleaned_count)).await?;
            // If directory is empty, try to delete it
            if let Ok(mut sub_entries) = tokio::fs::read_dir(&path).await
                && sub_entries.next_entry().await.is_ok_and(|e| e.is_none())
            {
                match tokio::fs::remove_dir(&path).await {
                    Ok(_) => {
                        yhlog("debug", &format!("Removed empty temporary directory: {}", path.display()));
                    }
                    Err(e) => {
                        yhlog("warn", &format!("Failed to remove empty temporary directory {}: {}", path.display(), e));
                    }
                }
            }
        } else if path.is_file() {
            // Skip active files
            if active_files.contains(&path) {
                continue;
            }
            // Check if file is expired
            if let Ok(metadata) = entry.metadata().await
                && let Ok(modified) = metadata.modified()
                && modified < *cutoff_time
            {
                match tokio::fs::remove_file(&path).await {
                    Ok(_) => {
                        *cleaned_count += 1;
                        yhlog("debug", &format!("Removed expired temporary file: {}", path.display()));
                    }
                    Err(e) => {
                        yhlog("warn", &format!("Failed to remove temporary file {}: {}", path.display(), e));
                    }
                }
            }
        }
    }
    Ok(())
}
/// Global temporary file manager instance
static GLOBAL_TEMP_MANAGER: once_cell::sync::Lazy<tokio::sync::RwLock<Option<Arc<VfsTempFileManager>>>> =
    once_cell::sync::Lazy::new(|| tokio::sync::RwLock::new(None));
/// Initialize global temporary file manager
pub async fn init_global_temp_manager(manager: VfsTempFileManager) {
    let mut guard = GLOBAL_TEMP_MANAGER.write().await;
    *guard = Some(Arc::new(manager));
}
/// Get global temporary file manager
pub async fn get_global_temp_manager() -> Option<Arc<VfsTempFileManager>> {
    GLOBAL_TEMP_MANAGER.read().await.as_ref().map(Arc::clone)
}
///( None)
/// Synchronously attempt to get global temporary file manager (Warning: may return None if lock is held)
pub fn get_global_temp_manager_sync() -> Option<Arc<VfsTempFileManager>> {
    GLOBAL_TEMP_MANAGER
        .try_read()
        .ok()
        .and_then(|r| r.as_ref().map(Arc::clone))
}
