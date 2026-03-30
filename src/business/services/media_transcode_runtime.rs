#[derive(Debug, Clone)]
pub struct MediaTranscodingVideoRuntimeConfig {
    pub enabled: bool,
    pub delivery: String,
    pub segment_duration_secs: u64,
    pub video_codec: String,
    pub audio_codec: String,
    pub preset: String,
    pub crf: u8,
    pub max_width: u32,
    pub max_height: u32,
    pub max_fps: u32,
    pub audio_bitrate_kbps: u32,
}

impl MediaTranscodingVideoRuntimeConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_delivery(&self) -> &str {
        self.delivery.as_str()
    }

    pub fn get_segment_duration_secs(&self) -> u64 {
        self.segment_duration_secs
    }

    pub fn get_video_codec(&self) -> &str {
        self.video_codec.as_str()
    }

    pub fn get_audio_codec(&self) -> &str {
        self.audio_codec.as_str()
    }

    pub fn get_preset(&self) -> &str {
        self.preset.as_str()
    }

    pub fn get_crf(&self) -> u8 {
        self.crf
    }

    pub fn get_max_width(&self) -> u32 {
        self.max_width
    }

    pub fn get_max_height(&self) -> u32 {
        self.max_height
    }

    pub fn get_max_fps(&self) -> u32 {
        self.max_fps
    }

    pub fn get_audio_bitrate_kbps(&self) -> u32 {
        self.audio_bitrate_kbps
    }
}

#[derive(Debug, Clone)]
pub struct MediaHardwareAccelerationRuntimeConfig {
    pub enabled: bool,
    pub backend: String,
    pub device: String,
    pub allow_fallback_to_software: bool,
}

impl MediaHardwareAccelerationRuntimeConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_backend(&self) -> &str {
        self.backend.as_str()
    }

    pub fn get_device(&self) -> &str {
        self.device.as_str()
    }

    pub fn is_allow_fallback_to_software(&self) -> bool {
        self.allow_fallback_to_software
    }
}

#[derive(Debug, Clone)]
pub struct MediaTranscodingRuntimeConfig {
    pub enabled: bool,
    pub cache_dir: String,
    pub cleanup_ttl_secs: u64,
    pub timeout_secs: u64,
    pub max_concurrent_tasks: usize,
    pub allow_software_fallback: bool,
    pub ffmpeg_path: String,
    pub video: MediaTranscodingVideoRuntimeConfig,
    pub hardware: MediaHardwareAccelerationRuntimeConfig,
}

impl MediaTranscodingRuntimeConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_cache_dir(&self) -> &str {
        self.cache_dir.as_str()
    }

    pub fn get_cleanup_ttl_secs(&self) -> u64 {
        self.cleanup_ttl_secs
    }

    pub fn get_timeout_secs(&self) -> u64 {
        self.timeout_secs
    }

    pub fn get_max_concurrent_tasks(&self) -> usize {
        self.max_concurrent_tasks
    }

    pub fn is_allow_software_fallback(&self) -> bool {
        self.allow_software_fallback
    }

    pub fn get_ffmpeg_path(&self) -> &str {
        self.ffmpeg_path.as_str()
    }

    pub fn get_video(&self) -> &MediaTranscodingVideoRuntimeConfig {
        &self.video
    }

    pub fn get_hardware(&self) -> &MediaHardwareAccelerationRuntimeConfig {
        &self.hardware
    }
}
