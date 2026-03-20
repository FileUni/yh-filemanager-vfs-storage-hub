use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CachePathPolicy {
    allow_thumbnail_paths: bool,
    skip_extensions: HashSet<Arc<str>>,
}

impl CachePathPolicy {
    pub fn new(allow_thumbnail_paths: bool, skip_extensions: &[Arc<str>]) -> Self {
        Self {
            allow_thumbnail_paths,
            skip_extensions: skip_extensions
                .iter()
                .map(|ext| Arc::<str>::from(normalize_extension(ext)))
                .collect(),
        }
    }

    pub fn allows(&self, path: &str) -> bool {
        if is_temp_namespace_path(path) || is_recycle_bin_path(path) {
            return false;
        }
        if !self.allow_thumbnail_paths && is_thumbnail_path(path) {
            return false;
        }
        match normalized_file_extension(path) {
            Some(ext) => !self.skip_extensions.contains(ext.as_str()),
            None => true,
        }
    }
}

fn normalize_extension(ext: &str) -> String {
    ext.trim().trim_start_matches('.').to_ascii_lowercase()
}

fn normalized_file_extension(path: &str) -> Option<String> {
    let file_name = path.rsplit('/').next()?;
    let (_, ext) = file_name.rsplit_once('.')?;
    let normalized = ext.trim();
    (!normalized.is_empty()).then(|| normalize_extension(normalized))
}

fn is_thumbnail_path(path: &str) -> bool {
    let normalized = path.trim_end_matches('/');
    normalized == "/.thumbs"
        || normalized == "/.thumbs_cache"
        || normalized.ends_with("/.thumbs")
        || normalized.contains("/.thumbs/")
        || normalized.starts_with("/.thumbs_cache/")
        || normalized.ends_with("/.thumbs_cache")
        || normalized.contains("/.thumbs_cache/")
}

fn is_temp_namespace_path(path: &str) -> bool {
    path.starts_with("/.virtual/tmp") || path.contains("/.virtual/tmp/")
}

fn is_recycle_bin_path(path: &str) -> bool {
    path == "/.recycle_bin"
        || path.starts_with("/.recycle_bin/")
        || path.ends_with("/.recycle_bin")
        || path.contains("/.recycle_bin/")
}
