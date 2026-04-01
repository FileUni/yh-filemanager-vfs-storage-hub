use crate::VfsStorageHub;
use sea_orm::DatabaseConnection;
use std::path::Path;
use std::sync::Arc;

use super::MediaHardwareAccelerationRuntimeConfig;

#[derive(Debug, Clone)]
pub struct ThumbnailRuntimeToolConfig {
    pub vips_path: String,
    pub imagemagick_path: String,
    pub ffmpeg_path: String,
    pub libreoffice_path: String,
    pub blender_path: String,
}

impl ThumbnailRuntimeToolConfig {
    pub fn get_vips_path(&self) -> &str {
        self.vips_path.as_str()
    }

    pub fn get_ffmpeg_path(&self) -> &str {
        self.ffmpeg_path.as_str()
    }

    pub fn get_libreoffice_path(&self) -> &str {
        self.libreoffice_path.as_str()
    }

    pub fn get_imagemagick_path(&self) -> &str {
        self.imagemagick_path.as_str()
    }

    pub fn get_blender_path(&self) -> &str {
        self.blender_path.as_str()
    }
}

#[derive(Debug, Clone)]
pub struct ThumbnailRuntimeImageConfig {
    pub enabled: bool,
    pub small_skip_mb: u64,
    pub max_size_mb: u64,
    pub imagemagick_max_mb: u64,
    pub timeout_secs: u64,
    pub backend: String,
}

impl ThumbnailRuntimeImageConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_max_size_mb(&self) -> u64 {
        self.max_size_mb
    }

    pub fn get_imagemagick_max_mb(&self) -> u64 {
        self.imagemagick_max_mb
    }

    pub fn get_timeout_secs(&self) -> u64 {
        self.timeout_secs
    }

    pub fn get_small_skip_mb(&self) -> u64 {
        self.small_skip_mb
    }

    pub fn is_builtin_backend(&self) -> bool {
        self.backend.trim().eq_ignore_ascii_case("builtin")
    }
}

#[derive(Debug, Clone)]
pub struct ThumbnailRuntimeTypeConfig {
    pub enabled: bool,
    pub small_skip_mb: u64,
    pub max_size_mb: u64,
    pub imagemagick_max_mb: u64,
    pub timeout_secs: u64,
    pub seek_mode: Option<Arc<str>>,
    pub seek_seconds: Option<u64>,
    pub seek_ratio: Option<f64>,
    pub max_chars: Option<u64>,
}

impl ThumbnailRuntimeTypeConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_max_size_mb(&self) -> u64 {
        self.max_size_mb
    }

    pub fn get_imagemagick_max_mb(&self) -> u64 {
        self.imagemagick_max_mb
    }

    pub fn get_timeout_secs(&self) -> u64 {
        self.timeout_secs
    }

    pub fn get_small_skip_mb(&self) -> u64 {
        self.small_skip_mb
    }

    pub fn get_seek_mode(&self) -> &str {
        self.seek_mode.as_deref().unwrap_or("auto")
    }

    pub fn get_seek_ratio(&self) -> f32 {
        self.seek_ratio.unwrap_or(0.0) as f32
    }

    pub fn get_seek_seconds(&self) -> u64 {
        self.seek_seconds.unwrap_or(0)
    }

    pub fn get_max_chars(&self) -> u64 {
        self.max_chars.unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct ThumbnailRuntimeConfig {
    pub enabled: bool,
    pub cache_mode: String,
    pub cache_dir: String,
    pub thumb_size_px: u32,
    pub thumb_format: String,
    pub thumb_quality: u8,
    pub tools: ThumbnailRuntimeToolConfig,
    pub image: ThumbnailRuntimeImageConfig,
    pub video: ThumbnailRuntimeTypeConfig,
    pub pdf: ThumbnailRuntimeTypeConfig,
    pub office: ThumbnailRuntimeTypeConfig,
    pub text: ThumbnailRuntimeTypeConfig,
    pub model3d: ThumbnailRuntimeTypeConfig,
    pub media_hardware: MediaHardwareAccelerationRuntimeConfig,
}

impl ThumbnailRuntimeConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_tools(&self) -> &ThumbnailRuntimeToolConfig {
        &self.tools
    }

    pub fn get_image(&self) -> &ThumbnailRuntimeImageConfig {
        &self.image
    }

    pub fn get_video(&self) -> &ThumbnailRuntimeTypeConfig {
        &self.video
    }

    pub fn get_pdf(&self) -> &ThumbnailRuntimeTypeConfig {
        &self.pdf
    }

    pub fn get_office(&self) -> &ThumbnailRuntimeTypeConfig {
        &self.office
    }

    pub fn get_text(&self) -> &ThumbnailRuntimeTypeConfig {
        &self.text
    }

    pub fn get_model3d(&self) -> &ThumbnailRuntimeTypeConfig {
        &self.model3d
    }

    pub fn get_thumb_size_px(&self) -> u32 {
        self.thumb_size_px
    }

    pub fn get_thumb_quality(&self) -> u8 {
        self.thumb_quality
    }

    pub fn get_cache_dir(&self) -> &str {
        self.cache_dir.as_str()
    }

    pub fn get_cache_mode(&self) -> &str {
        self.cache_mode.as_str()
    }

    pub fn get_thumb_format(&self) -> &str {
        self.thumb_format.as_str()
    }

    pub fn get_media_hardware(&self) -> &MediaHardwareAccelerationRuntimeConfig {
        &self.media_hardware
    }
}

#[derive(Debug, Clone)]
pub struct LatexPreviewRuntimeConfig {
    pub enable_latexmk: bool,
    pub latexmk_path: String,
    pub latexmk_timeout_secs: u64,
    pub max_input_size_mb: u64,
    pub max_output_size_mb: u64,
    pub allow_shell_escape: bool,
}

impl LatexPreviewRuntimeConfig {
    pub fn is_enable_latexmk(&self) -> bool {
        self.enable_latexmk
    }

    pub fn get_latexmk_path(&self) -> &str {
        self.latexmk_path.as_str()
    }

    pub fn get_latexmk_timeout_secs(&self) -> u64 {
        self.latexmk_timeout_secs
    }

    pub fn get_max_input_size_mb(&self) -> u64 {
        self.max_input_size_mb
    }

    pub fn get_max_output_size_mb(&self) -> u64 {
        self.max_output_size_mb
    }

    pub fn is_allow_shell_escape(&self) -> bool {
        self.allow_shell_escape
    }
}

#[derive(Clone)]
pub struct ThumbnailServiceContext {
    pub db: Arc<DatabaseConnection>,
    pub storage_hub: Arc<VfsStorageHub>,
    pub thumbnail: ThumbnailRuntimeConfig,
    pub latex: LatexPreviewRuntimeConfig,
}

impl ThumbnailServiceContext {
    pub fn new(
        db: Arc<DatabaseConnection>,
        storage_hub: Arc<VfsStorageHub>,
        thumbnail: ThumbnailRuntimeConfig,
        latex: LatexPreviewRuntimeConfig,
    ) -> Self {
        Self {
            db,
            storage_hub,
            thumbnail,
            latex,
        }
    }
}

pub(crate) fn normalize_logical_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

pub(crate) fn guess_mime_type(path: &Path) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string()
}
