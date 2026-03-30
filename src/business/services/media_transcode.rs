use anyhow::{Result, anyhow};
use dashmap::DashMap;
use futures::StreamExt;
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::RwLock;
use uuid::Uuid;
use yh_console_log::yhlog;

use crate::VfsStorage;

use super::media_transcode_runtime::{
    MediaTranscodingRuntimeConfig, MediaTranscodingVideoRuntimeConfig,
};

static VIDEO_SESSION_BY_ID: Lazy<DashMap<String, Arc<VideoTranscodeSessionRecord>>> =
    Lazy::new(DashMap::new);
static VIDEO_SESSION_BY_KEY: Lazy<DashMap<String, Arc<VideoTranscodeSessionRecord>>> =
    Lazy::new(DashMap::new);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoTranscodeSessionState {
    Pending,
    Ready,
    Failed,
}

#[derive(Debug, Clone)]
pub struct VideoTranscodeSessionSnapshot {
    pub session_id: String,
    pub state: VideoTranscodeSessionState,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VideoTranscodeAsset {
    pub path: PathBuf,
    pub content_type: &'static str,
}

#[derive(Debug, Clone)]
enum VideoTranscodeSessionWorkerState {
    Pending,
    Ready,
    Failed(Arc<str>),
}

struct VideoTranscodeSessionRecord {
    key: Arc<str>,
    session_id: Arc<str>,
    session_dir: PathBuf,
    manifest_path: PathBuf,
    expires_at_epoch_secs: AtomicI64,
    state: RwLock<VideoTranscodeSessionWorkerState>,
}

impl VideoTranscodeSessionRecord {
    fn new(
        key: Arc<str>,
        session_id: Arc<str>,
        session_dir: PathBuf,
        cleanup_ttl_secs: u64,
    ) -> Self {
        let manifest_path = session_dir.join("index.m3u8");
        Self {
            key,
            session_id,
            session_dir,
            manifest_path,
            expires_at_epoch_secs: AtomicI64::new(now_epoch_secs() + cleanup_ttl_secs as i64),
            state: RwLock::new(VideoTranscodeSessionWorkerState::Pending),
        }
    }

    fn touch(&self, cleanup_ttl_secs: u64) {
        self.expires_at_epoch_secs.store(
            now_epoch_secs() + cleanup_ttl_secs as i64,
            Ordering::Relaxed,
        );
    }

    fn is_expired(&self) -> bool {
        self.expires_at_epoch_secs.load(Ordering::Relaxed) <= now_epoch_secs()
    }

    async fn snapshot(&self) -> VideoTranscodeSessionSnapshot {
        match &*self.state.read().await {
            VideoTranscodeSessionWorkerState::Pending => VideoTranscodeSessionSnapshot {
                session_id: self.session_id.to_string(),
                state: VideoTranscodeSessionState::Pending,
                error_message: None,
            },
            VideoTranscodeSessionWorkerState::Ready => VideoTranscodeSessionSnapshot {
                session_id: self.session_id.to_string(),
                state: VideoTranscodeSessionState::Ready,
                error_message: None,
            },
            VideoTranscodeSessionWorkerState::Failed(message) => VideoTranscodeSessionSnapshot {
                session_id: self.session_id.to_string(),
                state: VideoTranscodeSessionState::Failed,
                error_message: Some(message.to_string()),
            },
        }
    }
}

pub async fn ensure_web_video_hls_session(
    storage: Arc<dyn VfsStorage>,
    user_id: &str,
    logical_path: &str,
    cfg: MediaTranscodingRuntimeConfig,
) -> Result<VideoTranscodeSessionSnapshot> {
    cleanup_expired_video_sessions().await;
    let key = Arc::<str>::from(build_video_session_key(user_id, logical_path, &cfg));
    if let Some(existing) = VIDEO_SESSION_BY_KEY.get(key.as_ref()) {
        let record = Arc::clone(existing.value());
        drop(existing);
        record.touch(cfg.get_cleanup_ttl_secs());
        let snapshot = record.snapshot().await;
        if snapshot.state == VideoTranscodeSessionState::Ready
            && tokio::fs::metadata(&record.manifest_path).await.is_ok()
        {
            return Ok(snapshot);
        }
        if snapshot.state == VideoTranscodeSessionState::Pending {
            return Ok(snapshot);
        }
        remove_video_session_record(&record).await;
    }

    let active_jobs = count_pending_video_jobs().await;
    if active_jobs >= cfg.get_max_concurrent_tasks() {
        return Err(anyhow!(
            "System busy: media transcoding slots are full ({})",
            cfg.get_max_concurrent_tasks()
        ));
    }

    let session_id = Arc::<str>::from(Uuid::now_v7().to_string());
    let session_dir = PathBuf::from(cfg.get_cache_dir()).join(session_id.as_ref());
    let record = Arc::new(VideoTranscodeSessionRecord::new(
        Arc::clone(&key),
        Arc::clone(&session_id),
        session_dir,
        cfg.get_cleanup_ttl_secs(),
    ));
    VIDEO_SESSION_BY_ID.insert(session_id.to_string(), Arc::clone(&record));
    VIDEO_SESSION_BY_KEY.insert(key.to_string(), Arc::clone(&record));
    spawn_video_hls_transcode(record, storage, logical_path.to_string(), cfg);
    Ok(VideoTranscodeSessionSnapshot {
        session_id: session_id.to_string(),
        state: VideoTranscodeSessionState::Pending,
        error_message: None,
    })
}

pub async fn resolve_web_video_hls_asset(
    session_id: &str,
    asset: &str,
    cleanup_ttl_secs: u64,
) -> Result<Option<VideoTranscodeAsset>> {
    cleanup_expired_video_sessions().await;
    let asset = normalize_asset_name(asset)?;
    let Some(record_ref) = VIDEO_SESSION_BY_ID.get(session_id) else {
        return Ok(None);
    };
    let record = Arc::clone(record_ref.value());
    drop(record_ref);
    record.touch(cleanup_ttl_secs);
    if !matches!(
        &*record.state.read().await,
        VideoTranscodeSessionWorkerState::Ready
    ) {
        return Ok(None);
    }
    let path = record.session_dir.join(asset);
    if tokio::fs::metadata(&path).await.is_err() {
        return Ok(None);
    }
    let content_type = match path.extension().and_then(|value| value.to_str()) {
        Some("m3u8") => "application/vnd.apple.mpegurl",
        Some("ts") => "video/mp2t",
        Some("m4s") => "video/iso.segment",
        _ => "application/octet-stream",
    };
    Ok(Some(VideoTranscodeAsset { path, content_type }))
}

async fn count_pending_video_jobs() -> usize {
    let records: Vec<Arc<VideoTranscodeSessionRecord>> = VIDEO_SESSION_BY_ID
        .iter()
        .map(|entry| Arc::clone(entry.value()))
        .collect();
    let mut count = 0usize;
    for record in records {
        if matches!(
            &*record.state.read().await,
            VideoTranscodeSessionWorkerState::Pending
        ) {
            count += 1;
        }
    }
    count
}

async fn cleanup_expired_video_sessions() {
    let records: Vec<Arc<VideoTranscodeSessionRecord>> = VIDEO_SESSION_BY_ID
        .iter()
        .map(|entry| Arc::clone(entry.value()))
        .collect();
    for record in records {
        if !record.is_expired() {
            continue;
        }
        if matches!(
            &*record.state.read().await,
            VideoTranscodeSessionWorkerState::Pending
        ) {
            continue;
        }
        remove_video_session_record(&record).await;
    }
}

async fn remove_video_session_record(record: &Arc<VideoTranscodeSessionRecord>) {
    VIDEO_SESSION_BY_ID.remove(record.session_id.as_ref());
    VIDEO_SESSION_BY_KEY.remove(record.key.as_ref());
    let _ = tokio::fs::remove_dir_all(&record.session_dir).await;
}

fn spawn_video_hls_transcode(
    record: Arc<VideoTranscodeSessionRecord>,
    storage: Arc<dyn VfsStorage>,
    logical_path: String,
    cfg: MediaTranscodingRuntimeConfig,
) {
    tokio::spawn(async move {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(cfg.get_timeout_secs()),
            run_video_hls_transcode_job(Arc::clone(&record), storage, &logical_path, &cfg),
        )
        .await;

        let outcome = match result {
            Ok(inner) => inner,
            Err(_) => Err(anyhow!("Video transcoding timeout")),
        };

        let mut state = record.state.write().await;
        match outcome {
            Ok(()) => {
                record.touch(cfg.get_cleanup_ttl_secs());
                *state = VideoTranscodeSessionWorkerState::Ready;
            }
            Err(error) => {
                yhlog(
                    "warn",
                    &format!(
                        "Video transcoding failed for session '{}' path '{}': {}",
                        record.session_id, logical_path, error
                    ),
                );
                *state = VideoTranscodeSessionWorkerState::Failed(Arc::from(error.to_string()));
            }
        }
    });
}

async fn run_video_hls_transcode_job(
    record: Arc<VideoTranscodeSessionRecord>,
    storage: Arc<dyn VfsStorage>,
    logical_path: &str,
    cfg: &MediaTranscodingRuntimeConfig,
) -> Result<()> {
    if tokio::fs::metadata(&record.session_dir).await.is_ok() {
        let _ = tokio::fs::remove_dir_all(&record.session_dir).await;
    }
    tokio::fs::create_dir_all(&record.session_dir).await?;

    let input_ext = Path::new(logical_path)
        .extension()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("bin");
    let input_path = record.session_dir.join(format!("input.{}", input_ext));
    download_to_local(&storage, logical_path, &input_path).await?;

    let manager = yh_config_infra::config_require_manager!(
        yh_external_process_manager::get_global_manager(),
        "external_process_manager"
    );
    manager
        .run_with_permit(yh_external_process_manager::TaskPriority::High, || async {
            transcode_video_to_hls(&input_path, &record.manifest_path, cfg).await
        })
        .await?;

    if tokio::fs::metadata(&record.manifest_path).await.is_err() {
        return Err(anyhow!("Generated HLS manifest is missing"));
    }
    Ok(())
}

async fn transcode_video_to_hls(
    input_path: &Path,
    manifest_path: &Path,
    cfg: &MediaTranscodingRuntimeConfig,
) -> Result<()> {
    let mut first_error = None;
    if cfg.get_hardware().is_enabled() {
        match run_ffmpeg_hls_command(input_path, manifest_path, cfg, true).await {
            Ok(()) => return Ok(()),
            Err(error) => first_error = Some(error),
        }
        if !cfg.is_allow_software_fallback() {
            return Err(first_error.unwrap_or_else(|| anyhow!("Video transcoding failed")));
        }
    }
    match run_ffmpeg_hls_command(input_path, manifest_path, cfg, false).await {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(first_error) = first_error {
                Err(anyhow!(
                    "Hardware transcoding failed: {}; software fallback failed: {}",
                    first_error,
                    error
                ))
            } else {
                Err(error)
            }
        }
    }
}

async fn run_ffmpeg_hls_command(
    input_path: &Path,
    manifest_path: &Path,
    cfg: &MediaTranscodingRuntimeConfig,
    use_hardware: bool,
) -> Result<()> {
    let ffmpeg_path = cfg.get_ffmpeg_path();
    if ffmpeg_path.trim().is_empty() {
        return Err(anyhow!("FFmpeg path is empty"));
    }
    let segment_pattern = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("Invalid manifest parent directory"))?
        .join("segment_%05d.ts");
    let mut cmd = Command::new(ffmpeg_path);
    cmd.arg("-y").arg("-v").arg("error");

    let hardware = cfg.get_hardware();
    if use_hardware {
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
            "none" => return Err(anyhow!("Hardware backend is disabled")),
            _ => {}
        }
    }

    let video = cfg.get_video();
    cmd.arg("-i")
        .arg(input_path)
        .arg("-map")
        .arg("0:v:0")
        .arg("-map")
        .arg("0:a:0?")
        .arg("-sn")
        .arg("-vf")
        .arg(build_video_filter(
            video,
            use_hardware,
            hardware.get_backend(),
        ))
        .arg("-c:a")
        .arg(video.get_audio_codec())
        .arg("-b:a")
        .arg(format!("{}k", video.get_audio_bitrate_kbps()))
        .arg("-ac")
        .arg("2");

    if use_hardware {
        apply_hardware_video_encoder_args(&mut cmd, video, hardware.get_backend());
    } else {
        cmd.arg("-c:v")
            .arg("libx264")
            .arg("-preset")
            .arg(video.get_preset())
            .arg("-crf")
            .arg(video.get_crf().to_string());
    }

    cmd.arg("-f")
        .arg(video.get_delivery())
        .arg("-hls_time")
        .arg(video.get_segment_duration_secs().to_string())
        .arg("-hls_playlist_type")
        .arg("vod")
        .arg("-hls_list_size")
        .arg("0")
        .arg("-hls_flags")
        .arg("independent_segments")
        .arg("-hls_segment_filename")
        .arg(segment_pattern)
        .arg(manifest_path);

    run_command_with_timeout(cmd, cfg.get_timeout_secs()).await
}

fn build_video_filter(
    video: &MediaTranscodingVideoRuntimeConfig,
    use_hardware: bool,
    backend: &str,
) -> String {
    let base = format!(
        "fps={},scale=w='min(iw,{})':h='min(ih,{})':force_original_aspect_ratio=decrease:force_divisible_by=2",
        video.get_max_fps(),
        video.get_max_width(),
        video.get_max_height(),
    );
    if use_hardware && backend == "vaapi" {
        format!("{base},format=nv12,hwupload")
    } else {
        format!("{base},format=yuv420p")
    }
}

fn apply_hardware_video_encoder_args(
    cmd: &mut Command,
    video: &MediaTranscodingVideoRuntimeConfig,
    backend: &str,
) {
    match backend {
        "vaapi" => {
            cmd.arg("-c:v")
                .arg("h264_vaapi")
                .arg("-qp")
                .arg(video.get_crf().to_string());
        }
        "qsv" => {
            cmd.arg("-c:v")
                .arg("h264_qsv")
                .arg("-global_quality")
                .arg(video.get_crf().to_string());
        }
        "nvenc" => {
            cmd.arg("-c:v")
                .arg("h264_nvenc")
                .arg("-cq")
                .arg(video.get_crf().to_string());
        }
        "videotoolbox" => {
            cmd.arg("-c:v").arg("h264_videotoolbox");
        }
        "amf" => {
            cmd.arg("-c:v").arg("h264_amf");
        }
        _ => {
            cmd.arg("-c:v")
                .arg("libx264")
                .arg("-preset")
                .arg(video.get_preset())
                .arg("-crf")
                .arg(video.get_crf().to_string());
        }
    }
}

async fn download_to_local(
    storage: &Arc<dyn VfsStorage>,
    logical_path: &str,
    local_path: &PathBuf,
) -> Result<()> {
    if let Some(parent) = local_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let (stream, _info) = storage.read_stream(logical_path).await?;
    let mut file = tokio::fs::File::create(local_path).await?;
    let mut stream = stream;
    while let Some(chunk) = stream.next().await {
        let data = chunk?;
        file.write_all(&data).await?;
    }
    Ok(())
}

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
        Err(_) => Err(anyhow!("Command timeout")),
    }
}

fn normalize_asset_name(asset: &str) -> Result<&str> {
    let normalized = asset.trim();
    if normalized.is_empty()
        || normalized.contains("..")
        || normalized.contains('/')
        || normalized.contains('\\')
    {
        return Err(anyhow!("Invalid media asset path"));
    }
    Ok(normalized)
}

fn build_video_session_key(
    user_id: &str,
    logical_path: &str,
    cfg: &MediaTranscodingRuntimeConfig,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(user_id.as_bytes());
    hasher.update([0]);
    hasher.update(logical_path.as_bytes());
    hasher.update([0]);
    hasher.update(cfg.get_ffmpeg_path().as_bytes());
    hasher.update([0]);
    hasher.update(cfg.get_video().get_delivery().as_bytes());
    hasher.update([0]);
    hasher.update(cfg.get_video().get_video_codec().as_bytes());
    hasher.update([0]);
    hasher.update(cfg.get_video().get_audio_codec().as_bytes());
    hasher.update([0]);
    hasher.update(cfg.get_hardware().get_backend().as_bytes());
    hasher.update([0]);
    hasher.update(cfg.get_hardware().get_device().as_bytes());
    hasher.update([0]);
    hasher.update(cfg.get_video().get_segment_duration_secs().to_le_bytes());
    hasher.update(cfg.get_video().get_max_width().to_le_bytes());
    hasher.update(cfg.get_video().get_max_height().to_le_bytes());
    hasher.update(cfg.get_video().get_max_fps().to_le_bytes());
    hasher.update(cfg.get_video().get_audio_bitrate_kbps().to_le_bytes());
    hasher.update([cfg.get_video().get_crf()]);
    hasher.update([u8::from(cfg.is_allow_software_fallback())]);
    hex::encode(hasher.finalize())
}

fn now_epoch_secs() -> i64 {
    chrono::Utc::now().timestamp()
}
