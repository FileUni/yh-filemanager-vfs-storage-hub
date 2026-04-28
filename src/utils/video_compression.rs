use crate::utils::compression::copy_to_local_temp;
use crate::vfs::VfsStorage;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;
use tokio_util::io::ReaderStream;
use yh_config_infra::config_require_manager;
use yh_external_process_manager::{TaskPriority, get_global_manager};

const DEFAULT_OUTPUT_SUFFIX: &str = "_compressed";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoCompressionOptions {
    pub include_subdirectories: bool,
    pub output_container: String,
    pub video_codec: String,
    pub profile: Option<String>,
    pub crf: Option<u8>,
    pub max_width: Option<u32>,
    pub max_height: Option<u32>,
    pub max_fps: Option<u32>,
    pub output_suffix: Option<String>,
    pub delete_source: bool,
    pub overwrite_existing: bool,
}

impl VideoCompressionOptions {
    pub fn normalized_container(&self) -> &str {
        match self.output_container.trim().to_ascii_lowercase().as_str() {
            "mkv" => "mkv",
            _ => "mp4",
        }
    }

    pub fn normalized_codec(&self) -> &str {
        match self.video_codec.trim().to_ascii_lowercase().as_str() {
            "hevc" | "h265" => "hevc",
            "av1" => "av1",
            _ => "h264",
        }
    }

    pub fn normalized_suffix(&self) -> String {
        let suffix = self
            .output_suffix
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_OUTPUT_SUFFIX);
        suffix.to_string()
    }

    pub fn validate(&self) -> Result<()> {
        if !matches!(self.normalized_container(), "mp4" | "mkv") {
            return Err(anyhow!("Unsupported output container"));
        }
        if !matches!(self.normalized_codec(), "h264" | "hevc" | "av1") {
            return Err(anyhow!("Unsupported video codec"));
        }
        if let Some(crf) = self.crf
            && crf > 63
        {
            return Err(anyhow!("CRF must be between 0 and 63"));
        }
        if let Some(width) = self.max_width
            && width == 0
        {
            return Err(anyhow!("Maximum width must be greater than 0"));
        }
        if let Some(height) = self.max_height
            && height == 0
        {
            return Err(anyhow!("Maximum height must be greater than 0"));
        }
        if let Some(fps) = self.max_fps
            && fps == 0
        {
            return Err(anyhow!("Maximum frame rate must be greater than 0"));
        }
        let suffix = self.normalized_suffix();
        if suffix.contains('/') || suffix.contains('\\') {
            return Err(anyhow!("Output suffix must not contain path separators"));
        }
        Ok(())
    }
}

pub async fn ensure_video_compression_ready() -> Result<()> {
    #[cfg(any(target_os = "android", target_os = "ios"))]
    {
        return Err(anyhow!(
            "Video compression is unavailable on Android and iOS runtimes"
        ));
    }

    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        let cfg = crate::config::get_vfs_hub_config().await;
        let media_cfg = cfg.get_media_transcoding();
        if !media_cfg.is_enabled() || !media_cfg.get_video().is_enabled() {
            return Err(anyhow!(
                "Video compression is disabled because media transcoding is not enabled"
            ));
        }
        let ffmpeg_path = cfg.get_effective_ffmpeg_path().trim();
        if ffmpeg_path.is_empty() {
            return Err(anyhow!("FFmpeg path is empty"));
        }
        Ok(())
    }
}

pub async fn collect_video_file_paths(
    storage: &dyn VfsStorage,
    input_paths: &[String],
    include_subdirectories: bool,
    output_suffix: &str,
) -> Result<Vec<String>> {
    let mut collected = Vec::new();
    for path in input_paths {
        let logical = normalize_logical_path(path);
        let info = storage.stat(&logical).await?;
        if info.is_dir {
            let entries = if include_subdirectories {
                storage.list_recursive(&logical).await?
            } else {
                storage.list(&logical).await?
            };
            for entry in entries {
                if entry.is_dir {
                    continue;
                }
                if is_video_file_path(&entry.path)
                    && !is_generated_output(&entry.path, output_suffix)
                {
                    collected.push(entry.path.to_string());
                }
            }
            continue;
        }

        if is_video_file_path(&logical) && !is_generated_output(&logical, output_suffix) {
            collected.push(logical);
        }
    }
    collected.sort();
    collected.dedup();
    Ok(collected)
}

pub async fn resolve_output_path(
    storage: &dyn VfsStorage,
    source_path: &str,
    options: &VideoCompressionOptions,
) -> Result<String> {
    let output_path = build_output_path(source_path, options)?;
    if output_path == source_path {
        return Err(anyhow!(
            "The output path matches the source path. Change the suffix or container."
        ));
    }
    if options.overwrite_existing {
        return Ok(output_path);
    }
    Ok(storage.get_unique_path(&output_path).await?)
}

pub async fn compress_vfs_video_to_vfs(
    storage: &dyn VfsStorage,
    user_id: &str,
    source_path: &str,
    output_path: &str,
    options: &VideoCompressionOptions,
) -> Result<()> {
    ensure_video_compression_ready().await?;
    options.validate()?;

    #[cfg(any(target_os = "android", target_os = "ios"))]
    {
        let _ = (storage, user_id, source_path, output_path, options);
        Err(anyhow!(
            "Video compression is unavailable on Android and iOS runtimes"
        ))
    }

    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        let cfg = crate::config::get_vfs_hub_config().await;
        let runtime_cfg = cfg
            .get_media_transcoding()
            .to_runtime_config(cfg.get_effective_ffmpeg_path());
        let job = EffectiveVideoCompressionJob::from_options(options, &runtime_cfg);
        let temp_manager = config_require_manager!(
            crate::utils::get_global_temp_manager().await,
            "vfs_storage_hub"
        );
        let (temp_dir, _guard) = temp_manager
            .create_user_temp_dir(user_id, "video-compress")
            .await?;

        let input_name = file_name_from_path(source_path);
        let local_input = temp_dir.join(input_name);
        copy_to_local_temp(storage, source_path, &local_input).await?;

        let local_output = temp_dir.join(format!("output.{}", job.container));
        run_ffmpeg_video_compression(&local_input, &local_output, &runtime_cfg, &job).await?;
        upload_local_file_to_vfs(storage, &local_output, output_path).await?;
        Ok(())
    }
}

fn normalize_logical_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

fn file_name_from_path(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn file_stem_from_path(path: &str) -> String {
    Path::new(file_name_from_path(path))
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_else(|| file_name_from_path(path))
        .to_string()
}

fn path_parent(path: &str) -> &str {
    path.rsplit_once('/').map(|value| value.0).unwrap_or("")
}

fn is_generated_output(path: &str, output_suffix: &str) -> bool {
    let stem = file_stem_from_path(path);
    stem.ends_with(output_suffix)
}

pub fn is_video_file_path(path: &str) -> bool {
    let ext = Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    matches!(
        ext.as_str(),
        "mp4"
            | "mov"
            | "mkv"
            | "avi"
            | "webm"
            | "m4v"
            | "mpg"
            | "mpeg"
            | "wmv"
            | "flv"
            | "ts"
            | "m2ts"
    )
}

fn build_output_path(source_path: &str, options: &VideoCompressionOptions) -> Result<String> {
    let stem = file_stem_from_path(source_path);
    if stem.is_empty() {
        return Err(anyhow!("Invalid source file name"));
    }
    let suffix = options.normalized_suffix();
    let output_file_name = format!("{stem}{suffix}.{}", options.normalized_container());
    let parent = path_parent(source_path);
    Ok(if parent.is_empty() {
        format!("/{}", output_file_name)
    } else {
        format!("{}/{}", parent, output_file_name)
    })
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
#[derive(Debug, Clone)]
struct EffectiveVideoCompressionJob {
    container: String,
    codec: String,
    profile: Option<String>,
    preset: String,
    crf: u8,
    max_width: u32,
    max_height: u32,
    max_fps: u32,
    audio_bitrate_kbps: u32,
    output_pixel_format: &'static str,
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
impl EffectiveVideoCompressionJob {
    fn from_options(
        options: &VideoCompressionOptions,
        runtime_cfg: &crate::business::services::MediaTranscodingRuntimeConfig,
    ) -> Self {
        let profile = options
            .profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let codec = options.normalized_codec().to_string();
        let output_pixel_format = resolve_output_pixel_format(&codec, profile.as_deref());
        Self {
            container: options.normalized_container().to_string(),
            codec,
            profile,
            preset: runtime_cfg.get_video().get_preset().to_string(),
            crf: options.crf.unwrap_or(runtime_cfg.get_video().get_crf()),
            max_width: options
                .max_width
                .unwrap_or(runtime_cfg.get_video().get_max_width()),
            max_height: options
                .max_height
                .unwrap_or(runtime_cfg.get_video().get_max_height()),
            max_fps: options
                .max_fps
                .unwrap_or(runtime_cfg.get_video().get_max_fps()),
            audio_bitrate_kbps: runtime_cfg.get_video().get_audio_bitrate_kbps(),
            output_pixel_format,
        }
    }
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn resolve_output_pixel_format(codec: &str, profile: Option<&str>) -> &'static str {
    let normalized_profile = profile.unwrap_or("").to_ascii_lowercase();
    match (codec, normalized_profile.as_str()) {
        ("hevc", "main10") => "yuv420p10le",
        ("av1", "high" | "professional") => "yuv420p10le",
        _ => "yuv420p",
    }
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn run_ffmpeg_video_compression(
    input_path: &Path,
    output_path: &Path,
    runtime_cfg: &crate::business::services::MediaTranscodingRuntimeConfig,
    job: &EffectiveVideoCompressionJob,
) -> Result<()> {
    let mut first_error = None;
    if should_try_hardware(runtime_cfg, job) {
        match run_ffmpeg_command(input_path, output_path, runtime_cfg, job, true).await {
            Ok(()) => return Ok(()),
            Err(error) => first_error = Some(error),
        }
    }

    match run_ffmpeg_command(input_path, output_path, runtime_cfg, job, false).await {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(first_error) = first_error {
                Err(anyhow!(
                    "Hardware compression failed: {}; software compression failed: {}",
                    first_error,
                    error
                ))
            } else {
                Err(error)
            }
        }
    }
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn should_try_hardware(
    runtime_cfg: &crate::business::services::MediaTranscodingRuntimeConfig,
    job: &EffectiveVideoCompressionJob,
) -> bool {
    if !runtime_cfg.get_hardware().is_enabled() {
        return false;
    }
    if job.output_pixel_format != "yuv420p" {
        return false;
    }
    match (runtime_cfg.get_hardware().get_backend(), job.codec.as_str()) {
        ("none", _) => false,
        ("vaapi" | "qsv" | "nvenc" | "videotoolbox" | "amf", "h264" | "hevc") => true,
        _ => false,
    }
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn run_ffmpeg_command(
    input_path: &Path,
    output_path: &Path,
    runtime_cfg: &crate::business::services::MediaTranscodingRuntimeConfig,
    job: &EffectiveVideoCompressionJob,
    use_hardware: bool,
) -> Result<()> {
    let ffmpeg_path = runtime_cfg.get_ffmpeg_path().trim();
    if ffmpeg_path.is_empty() {
        return Err(anyhow!("FFmpeg path is empty"));
    }
    let output_parent = output_path
        .parent()
        .ok_or_else(|| anyhow!("Invalid output path"))?;
    tokio::fs::create_dir_all(output_parent).await?;

    let manager = config_require_manager!(get_global_manager(), "external_process_manager");
    manager
        .run_with_permit(TaskPriority::Low, || async move {
            let mut cmd = Command::new(ffmpeg_path);
            cmd.stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .arg("-y")
                .arg("-v")
                .arg("error");

            if use_hardware {
                apply_hardware_input_args(&mut cmd, runtime_cfg)?;
            }

            cmd.arg("-i")
                .arg(input_path)
                .arg("-map")
                .arg("0:v:0")
                .arg("-map")
                .arg("0:a:0?")
                .arg("-sn")
                .arg("-vf")
                .arg(build_video_filter(
                    job.max_width,
                    job.max_height,
                    job.max_fps,
                    job.output_pixel_format,
                    use_hardware,
                    runtime_cfg.get_hardware().get_backend(),
                ))
                .arg("-pix_fmt")
                .arg(job.output_pixel_format)
                .arg("-movflags")
                .arg("+faststart")
                .arg("-c:a")
                .arg("aac")
                .arg("-b:a")
                .arg(format!("{}k", job.audio_bitrate_kbps))
                .arg("-ac")
                .arg("2");

            if let Some(profile) = job.profile.as_deref() {
                cmd.arg("-profile:v").arg(profile);
            }

            apply_video_encoder_args(
                &mut cmd,
                job,
                use_hardware,
                runtime_cfg.get_hardware().get_backend(),
            );
            cmd.arg("-f").arg(job.container.as_str()).arg(output_path);

            run_command_with_timeout(cmd, runtime_cfg.get_timeout_secs()).await
        })
        .await
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn apply_hardware_input_args(
    cmd: &mut Command,
    runtime_cfg: &crate::business::services::MediaTranscodingRuntimeConfig,
) -> Result<()> {
    let hardware = runtime_cfg.get_hardware();
    match hardware.get_backend() {
        "vaapi" => {
            let device = hardware.get_device().trim();
            if device.is_empty() {
                return Err(anyhow!("VAAPI device is required"));
            }
            cmd.arg("-vaapi_device").arg(device);
        }
        "qsv" => {
            let device = hardware.get_device().trim();
            if !device.is_empty() {
                cmd.arg("-qsv_device").arg(device);
            }
        }
        "videotoolbox" => {
            cmd.arg("-hwaccel").arg("videotoolbox");
        }
        "nvenc" => {
            cmd.arg("-hwaccel").arg("cuda");
        }
        "amf" | "none" => {}
        _ => {}
    }
    Ok(())
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn build_video_filter(
    max_width: u32,
    max_height: u32,
    max_fps: u32,
    output_pixel_format: &str,
    use_hardware: bool,
    backend: &str,
) -> String {
    let base = format!(
        "fps={},scale=w='min(iw,{})':h='min(ih,{})':force_original_aspect_ratio=decrease:force_divisible_by=2",
        max_fps, max_width, max_height
    );
    if use_hardware && backend == "vaapi" {
        return format!("{base},format=nv12,hwupload");
    }
    format!("{base},format={output_pixel_format}")
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn apply_video_encoder_args(
    cmd: &mut Command,
    job: &EffectiveVideoCompressionJob,
    use_hardware: bool,
    backend: &str,
) {
    if use_hardware {
        match (backend, job.codec.as_str()) {
            ("vaapi", "h264") => {
                cmd.arg("-c:v")
                    .arg("h264_vaapi")
                    .arg("-qp")
                    .arg(job.crf.to_string());
                return;
            }
            ("vaapi", "hevc") => {
                cmd.arg("-c:v")
                    .arg("hevc_vaapi")
                    .arg("-qp")
                    .arg(job.crf.to_string());
                return;
            }
            ("qsv", "h264") => {
                cmd.arg("-c:v")
                    .arg("h264_qsv")
                    .arg("-global_quality")
                    .arg(job.crf.to_string());
                return;
            }
            ("qsv", "hevc") => {
                cmd.arg("-c:v")
                    .arg("hevc_qsv")
                    .arg("-global_quality")
                    .arg(job.crf.to_string());
                return;
            }
            ("nvenc", "h264") => {
                cmd.arg("-c:v")
                    .arg("h264_nvenc")
                    .arg("-cq")
                    .arg(job.crf.to_string());
                return;
            }
            ("nvenc", "hevc") => {
                cmd.arg("-c:v")
                    .arg("hevc_nvenc")
                    .arg("-cq")
                    .arg(job.crf.to_string());
                return;
            }
            ("videotoolbox", "h264") => {
                cmd.arg("-c:v").arg("h264_videotoolbox");
                return;
            }
            ("videotoolbox", "hevc") => {
                cmd.arg("-c:v").arg("hevc_videotoolbox");
                return;
            }
            ("amf", "h264") => {
                cmd.arg("-c:v").arg("h264_amf");
                return;
            }
            ("amf", "hevc") => {
                cmd.arg("-c:v").arg("hevc_amf");
                return;
            }
            _ => {}
        }
    }

    match job.codec.as_str() {
        "hevc" => {
            cmd.arg("-c:v")
                .arg("libx265")
                .arg("-preset")
                .arg(job.preset.as_str())
                .arg("-crf")
                .arg(job.crf.to_string());
        }
        "av1" => {
            cmd.arg("-c:v")
                .arg("libsvtav1")
                .arg("-preset")
                .arg(job.preset.as_str())
                .arg("-crf")
                .arg(job.crf.to_string());
        }
        _ => {
            cmd.arg("-c:v")
                .arg("libx264")
                .arg("-preset")
                .arg(job.preset.as_str())
                .arg("-crf")
                .arg(job.crf.to_string());
        }
    }
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn upload_local_file_to_vfs(
    storage: &dyn VfsStorage,
    local_path: &Path,
    output_path: &str,
) -> Result<()> {
    let file = tokio::fs::File::open(local_path).await?;
    let stream = ReaderStream::new(file).map(|result| result.map_err(crate::vfs::VfsError::Io));
    storage.write_stream(output_path, Box::pin(stream)).await?;
    Ok(())
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn run_command_with_timeout(mut cmd: Command, timeout_secs: u64) -> Result<()> {
    let output =
        tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output()).await;
    match output {
        Ok(Ok(res)) if res.status.success() => Ok(()),
        Ok(Ok(res)) => {
            let stderr = String::from_utf8_lossy(&res.stderr);
            let stdout = String::from_utf8_lossy(&res.stdout);
            let message = stderr
                .lines()
                .chain(stdout.lines())
                .map(str::trim)
                .find(|line| !line.is_empty())
                .unwrap_or("ffmpeg exited with non-zero status");
            Err(anyhow!(message.to_string()))
        }
        Ok(Err(error)) => Err(anyhow!(error)),
        Err(_) => Err(anyhow!("Video compression timed out")),
    }
}
