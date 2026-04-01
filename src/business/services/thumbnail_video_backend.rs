use anyhow::{Result, anyhow};
#[cfg(any(target_os = "android", target_os = "ios"))]
use std::path::Path;
use std::path::PathBuf;
use tokio::process::Command;
use yh_console_log::yhlog;

#[cfg(any(target_os = "android", target_os = "ios"))]
use super::thumbnail_image_backend::transcode_image_bytes;
use super::thumbnail_runtime::{ThumbnailRuntimeConfig, ThumbnailRuntimeTypeConfig};

pub async fn render_video_thumbnail(
    video_cfg: &ThumbnailRuntimeTypeConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &PathBuf,
    output: &PathBuf,
) -> Result<bool> {
    #[cfg(any(target_os = "android", target_os = "ios"))]
    {
        return render_video_thumbnail_mobile(video_cfg, cfg, input, output).await;
    }

    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        render_video_thumbnail_ffmpeg(video_cfg, cfg, input, output).await
    }
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn render_video_thumbnail_ffmpeg(
    video_cfg: &ThumbnailRuntimeTypeConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &PathBuf,
    output: &PathBuf,
) -> Result<bool> {
    let ffmpeg_path = cfg.get_tools().get_ffmpeg_path();
    let size = cfg.get_thumb_size_px().to_string();
    let seek = match video_cfg.get_seek_mode() {
        "ratio" => {
            let duration = detect_video_duration_ffmpeg(ffmpeg_path, input).await.ok();
            resolve_seek_seconds_from_config(duration, video_cfg)
        }
        "seconds" => video_cfg.get_seek_seconds(),
        "auto" => match (video_cfg.seek_ratio, video_cfg.seek_seconds) {
            (Some(_), _) => {
                let duration = detect_video_duration_ffmpeg(ffmpeg_path, input).await.ok();
                resolve_seek_seconds_from_config(duration, video_cfg)
            }
            (None, Some(_)) => video_cfg.get_seek_seconds(),
            (None, None) => return Err(anyhow!("Thumbnail video seek config missing")),
        },
        _ => return Err(anyhow!("Invalid seek_mode: {}", video_cfg.get_seek_mode())),
    };
    let quality = cfg.get_thumb_quality() as i32;
    let qscale = ((100 - quality) / 4).clamp(2, 31);
    let timeout = video_cfg.get_timeout_secs();
    if let Some(hardware) = resolve_thumbnail_hardware_mode(cfg) {
        match run_thumbnail_command(ThumbnailCommandSpec {
            ffmpeg_path,
            input,
            output,
            seek,
            size: &size,
            qscale,
            hardware_mode: Some(hardware),
            timeout,
        })
        .await
        {
            Ok(value) => return Ok(value),
            Err(err) => {
                yhlog(
                    "warn",
                    &format!(
                        "Thumbnail hardware acceleration fallback triggered (ffmpeg, path='{}'): {}",
                        ffmpeg_path, err
                    ),
                );
            }
        }
    }

    match run_thumbnail_command(ThumbnailCommandSpec {
        ffmpeg_path,
        input,
        output,
        seek,
        size: &size,
        qscale,
        hardware_mode: None,
        timeout,
    })
    .await
    {
        Ok(value) => Ok(value),
        Err(err) => {
            yhlog(
                "warn",
                &format!(
                    "Thumbnail tool failed (ffmpeg, path='{}'): {}",
                    ffmpeg_path, err
                ),
            );
            Err(err)
        }
    }
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
#[derive(Clone)]
enum ThumbnailHardwareMode {
    Vaapi { device: String },
    Qsv { device: Option<String> },
    VideoToolbox,
    Auto,
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn resolve_thumbnail_hardware_mode(cfg: &ThumbnailRuntimeConfig) -> Option<ThumbnailHardwareMode> {
    let hardware = cfg.get_media_hardware();
    if !hardware.is_enabled() {
        return None;
    }
    match hardware.get_backend() {
        "vaapi" => {
            let device = hardware.get_device().trim();
            if device.is_empty() {
                None
            } else {
                Some(ThumbnailHardwareMode::Vaapi {
                    device: device.to_string(),
                })
            }
        }
        "qsv" => Some(ThumbnailHardwareMode::Qsv {
            device: (!hardware.get_device().trim().is_empty())
                .then(|| hardware.get_device().trim().to_string()),
        }),
        "videotoolbox" => Some(ThumbnailHardwareMode::VideoToolbox),
        "nvenc" | "amf" => Some(ThumbnailHardwareMode::Auto),
        _ => None,
    }
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn run_thumbnail_command(spec: ThumbnailCommandSpec<'_>) -> Result<bool> {
    let mut cmd = Command::new(spec.ffmpeg_path);
    cmd.arg("-y");

    if let Some(mode) = spec.hardware_mode {
        match mode {
            ThumbnailHardwareMode::Vaapi { device } => {
                cmd.arg("-hwaccel")
                    .arg("vaapi")
                    .arg("-hwaccel_device")
                    .arg(device);
            }
            ThumbnailHardwareMode::Qsv { device } => {
                cmd.arg("-hwaccel").arg("qsv");
                if let Some(device) = device {
                    cmd.arg("-qsv_device").arg(device);
                }
            }
            ThumbnailHardwareMode::VideoToolbox => {
                cmd.arg("-hwaccel").arg("videotoolbox");
            }
            ThumbnailHardwareMode::Auto => {
                cmd.arg("-hwaccel").arg("auto");
            }
        }
    }

    cmd.arg("-ss")
        .arg(spec.seek.to_string())
        .arg("-i")
        .arg(spec.input)
        .arg("-frames:v")
        .arg("1")
        .arg("-vf")
        .arg(format!("scale={}:-1", spec.size))
        .arg("-q:v")
        .arg(spec.qscale.to_string())
        .arg(spec.output);
    run_command_with_timeout(cmd, spec.timeout).await
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
struct ThumbnailCommandSpec<'a> {
    ffmpeg_path: &'a str,
    input: &'a PathBuf,
    output: &'a PathBuf,
    seek: u64,
    size: &'a str,
    qscale: i32,
    hardware_mode: Option<ThumbnailHardwareMode>,
    timeout: u64,
}

#[cfg(any(target_os = "android", target_os = "ios"))]
async fn render_video_thumbnail_mobile(
    video_cfg: &ThumbnailRuntimeTypeConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &PathBuf,
    output: &PathBuf,
) -> Result<bool> {
    let input = input.clone();
    let output = output.clone();
    let seek_ratio = video_cfg.seek_ratio;
    let seek_seconds = video_cfg.seek_seconds;
    let seek_mode = video_cfg.seek_mode.clone();
    let thumb_size = cfg.get_thumb_size_px();
    let thumb_quality = cfg.get_thumb_quality();
    let thumb_format = normalize_output_format(cfg.get_thumb_format()).to_string();

    tokio::task::spawn_blocking(move || {
        #[cfg(target_os = "android")]
        {
            render_video_thumbnail_android(
                &input,
                &output,
                seek_ratio,
                seek_seconds,
                &seek_mode,
                thumb_size,
                thumb_quality,
                thumb_format.as_str(),
            )
        }

        #[cfg(target_os = "ios")]
        {
            render_video_thumbnail_ios(
                &input,
                &output,
                seek_ratio,
                seek_seconds,
                &seek_mode,
                thumb_size,
                thumb_quality,
                thumb_format.as_str(),
            )
        }
    })
    .await
    .map_err(|err| anyhow!("Mobile video thumbnail task join error: {}", err))??;

    Ok(true)
}

#[cfg(target_os = "android")]
fn render_video_thumbnail_android(
    input: &Path,
    output: &Path,
    seek_ratio: Option<f64>,
    seek_seconds: Option<u64>,
    seek_mode: &Option<std::sync::Arc<str>>,
    thumb_size: u32,
    thumb_quality: u8,
    thumb_format: &str,
) -> Result<()> {
    use jni::objects::{JByteArray, JString, JValue};

    fn java_error_from_jni(
        env: &mut jni::JNIEnv<'_>,
        action: &str,
        err: jni::errors::Error,
    ) -> anyhow::Error {
        let mut message = format!("android media jni error during {}", action);
        if matches!(err, jni::errors::Error::JavaException)
            && let Some(details) = take_java_exception_string(env)
        {
            message.push_str(": ");
            message.push_str(&details);
        }
        anyhow!(message).context(err)
    }

    fn take_java_exception_string(env: &mut jni::JNIEnv<'_>) -> Option<String> {
        let has_exception = env.exception_check().ok()?;
        if !has_exception {
            return None;
        }
        let throwable = env.exception_occurred().ok()?;
        let _ = env.exception_clear();
        let obj = env
            .call_method(&throwable, "toString", "()Ljava/lang/String;", &[])
            .ok()?
            .l()
            .ok()?;
        if obj.is_null() {
            return Some("JavaException".to_string());
        }
        let string = JString::from(obj);
        env.get_string(&string)
            .ok()
            .map(|value| value.to_string_lossy().into_owned())
    }

    let context = ndk_context::android_context();
    let vm = unsafe { jni::JavaVM::from_raw(context.vm().cast()) }
        .map_err(|err| anyhow!("Failed to access Android JavaVM: {}", err))?;
    let mut env = vm
        .attach_current_thread()
        .map_err(|err| anyhow!("Failed to attach Android thread: {}", err))?;
    let retriever_class = env
        .find_class("android/media/MediaMetadataRetriever")
        .map_err(|err| java_error_from_jni(&mut env, "find MediaMetadataRetriever", err))?;
    let retriever = env
        .new_object("android/media/MediaMetadataRetriever", "()V", &[])
        .map_err(|err| java_error_from_jni(&mut env, "construct MediaMetadataRetriever", err))?;
    let jpath = env
        .new_string(input.to_string_lossy().as_ref())
        .map_err(|err| java_error_from_jni(&mut env, "new_string(path)", err))?;
    env.call_method(
        &retriever,
        "setDataSource",
        "(Ljava/lang/String;)V",
        &[JValue::Object(&jpath)],
    )
    .map_err(|err| java_error_from_jni(&mut env, "setDataSource", err))?;

    let duration_ms = get_android_video_duration_ms(&mut env, &retriever_class, &retriever)?;
    let seek = match seek_mode.as_deref().unwrap_or("auto") {
        "ratio" => {
            resolve_seek_seconds(duration_ms.map(|value| value as f64 / 1000.0), seek_ratio, seek_seconds)
        }
        "seconds" => seek_seconds.unwrap_or(0),
        "auto" => {
            resolve_seek_seconds(duration_ms.map(|value| value as f64 / 1000.0), seek_ratio, seek_seconds)
        }
        _ => 0,
    };
    let seek_us = (seek as i64).saturating_mul(1_000_000);

    let option_closest_sync = env
        .get_static_field(retriever_class, "OPTION_CLOSEST_SYNC", "I")
        .map_err(|err| java_error_from_jni(&mut env, "OPTION_CLOSEST_SYNC", err))?
        .i()
        .map_err(|err| java_error_from_jni(&mut env, "OPTION_CLOSEST_SYNC", err))?;
    let bitmap = env
        .call_method(
            &retriever,
            "getFrameAtTime",
            "(JI)Landroid/graphics/Bitmap;",
            &[JValue::Long(seek_us), JValue::Int(option_closest_sync)],
        )
        .map_err(|err| java_error_from_jni(&mut env, "getFrameAtTime", err))?
        .l()
        .map_err(|err| java_error_from_jni(&mut env, "getFrameAtTime", err))?;
    if bitmap.is_null() {
        let _ = env.call_method(&retriever, "release", "()V", &[]);
        return Err(anyhow!("Android media framework returned no video frame"));
    }

    let width = env
        .call_method(&bitmap, "getWidth", "()I", &[])
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap.getWidth", err))?
        .i()
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap.getWidth", err))?;
    let height = env
        .call_method(&bitmap, "getHeight", "()I", &[])
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap.getHeight", err))?
        .i()
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap.getHeight", err))?;
    let (scaled_width, scaled_height) = scaled_dimensions(width as u32, height as u32, thumb_size);

    let scaled_bitmap = if width as u32 != scaled_width || height as u32 != scaled_height {
        let bitmap_class = env
            .find_class("android/graphics/Bitmap")
            .map_err(|err| java_error_from_jni(&mut env, "find Bitmap", err))?;
        env.call_static_method(
            bitmap_class,
            "createScaledBitmap",
            "(Landroid/graphics/Bitmap;IIZ)Landroid/graphics/Bitmap;",
            &[
                JValue::Object(&bitmap),
                JValue::Int(scaled_width as i32),
                JValue::Int(scaled_height as i32),
                JValue::Bool(1),
            ],
        )
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap.createScaledBitmap", err))?
        .l()
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap.createScaledBitmap", err))?
    } else {
        bitmap
    };

    let stream = env
        .new_object("java/io/ByteArrayOutputStream", "()V", &[])
        .map_err(|err| java_error_from_jni(&mut env, "new ByteArrayOutputStream", err))?;
    let compress_class = env
        .find_class("android/graphics/Bitmap$CompressFormat")
        .map_err(|err| java_error_from_jni(&mut env, "find Bitmap$CompressFormat", err))?;
    let compress_field = if thumb_format == "png" { "PNG" } else { "JPEG" };
    let compress_format = env
        .get_static_field(
            compress_class,
            compress_field,
            "Landroid/graphics/Bitmap$CompressFormat;",
        )
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap$CompressFormat", err))?
        .l()
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap$CompressFormat", err))?;
    let quality = if thumb_format == "png" {
        100
    } else {
        thumb_quality as i32
    };
    let compressed = env
        .call_method(
            &scaled_bitmap,
            "compress",
            "(Landroid/graphics/Bitmap$CompressFormat;ILjava/io/OutputStream;)Z",
            &[
                JValue::Object(&compress_format),
                JValue::Int(quality),
                JValue::Object(&stream),
            ],
        )
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap.compress", err))?
        .z()
        .map_err(|err| java_error_from_jni(&mut env, "Bitmap.compress", err))?;
    if !compressed {
        let _ = env.call_method(&retriever, "release", "()V", &[]);
        return Err(anyhow!(
            "Android media framework failed to encode video thumbnail"
        ));
    }

    let array = env
        .call_method(&stream, "toByteArray", "()[B", &[])
        .map_err(|err| java_error_from_jni(&mut env, "ByteArrayOutputStream.toByteArray", err))?
        .l()
        .map_err(|err| java_error_from_jni(&mut env, "ByteArrayOutputStream.toByteArray", err))?;
    let bytes = env
        .convert_byte_array(JByteArray::from(array))
        .map_err(|err| anyhow!("Failed to copy Android thumbnail bytes: {}", err))?;
    let _ = env.call_method(&retriever, "release", "()V", &[]);
    let output_bytes = if thumb_format == "webp" {
        transcode_image_bytes(
            &bytes,
            image::ImageFormat::Jpeg,
            thumb_format,
            thumb_quality,
        )?
    } else {
        bytes
    };
    std::fs::write(output, output_bytes)
        .map_err(|err| anyhow!("Failed to write Android video thumbnail: {}", err))?;
    Ok(())
}

#[cfg(target_os = "android")]
fn get_android_video_duration_ms(
    env: &mut jni::JNIEnv<'_>,
    retriever_class: &jni::objects::JClass<'_>,
    retriever: &jni::objects::JObject<'_>,
) -> Result<Option<u64>> {
    let key_duration = env
        .get_static_field(retriever_class, "METADATA_KEY_DURATION", "I")
        .map_err(|err| anyhow!("Failed to read Android duration metadata key: {}", err))?
        .i()
        .map_err(|err| anyhow!("Failed to read Android duration metadata key: {}", err))?;
    let value = env
        .call_method(
            retriever,
            "extractMetadata",
            "(I)Ljava/lang/String;",
            &[jni::objects::JValue::Int(key_duration)],
        )
        .map_err(|err| anyhow!("Failed to extract Android video duration: {}", err))?
        .l()
        .map_err(|err| anyhow!("Failed to extract Android video duration: {}", err))?;
    if value.is_null() {
        return Ok(None);
    }
    let value = jni::objects::JString::from(value);
    let text = env
        .get_string(&value)
        .map_err(|err| anyhow!("Failed to read Android duration string: {}", err))?
        .to_string_lossy()
        .into_owned();
    Ok(text.trim().parse::<u64>().ok())
}

#[cfg(target_os = "ios")]
fn render_video_thumbnail_ios(
    input: &Path,
    output: &Path,
    seek_ratio: Option<f64>,
    seek_seconds: Option<u64>,
    seek_mode: &Option<std::sync::Arc<str>>,
    thumb_size: u32,
    thumb_quality: u8,
    thumb_format: &str,
) -> Result<()> {
    use objc2::rc::autoreleasepool;
    use objc2_av_foundation::{AVAssetImageGenerator, AVURLAsset};
    use objc2_core_foundation::{CGFloat, CGSize};
    use objc2_core_media::CMTime;
    use objc2_foundation::{NSString, NSURL};
    use objc2_ui_kit::UIImage;

    let output_bytes = autoreleasepool(|_| -> Result<Vec<u8>> {
        let ns_path = NSString::from_str(input.to_string_lossy().as_ref());
        let url = NSURL::fileURLWithPath(&ns_path);
        let asset = unsafe { AVURLAsset::URLAssetWithURL_options(&url, None) };
        let duration_secs = unsafe { asset.duration().seconds() };
        let duration = if duration_secs.is_finite() && duration_secs > 0.0 {
            Some(duration_secs)
        } else {
            None
        };
        let seek = match seek_mode.as_deref().unwrap_or("auto") {
            "ratio" => resolve_seek_seconds(duration, seek_ratio, seek_seconds),
            "seconds" => seek_seconds.unwrap_or(0),
            "auto" => resolve_seek_seconds(duration, seek_ratio, seek_seconds),
            _ => 0,
        };
        let requested_time = unsafe { CMTime::with_seconds(seek as f64, 600) };
        let generator = unsafe { AVAssetImageGenerator::assetImageGeneratorWithAsset(&asset) };
        unsafe {
            generator.setAppliesPreferredTrackTransform(true);
            generator.setMaximumSize(CGSize::new(thumb_size as CGFloat, thumb_size as CGFloat));
        }
        let cg_image = unsafe {
            generator.copyCGImageAtTime_actualTime_error(requested_time, std::ptr::null_mut())
        }
        .map_err(|err| anyhow!("iOS media framework failed to extract video frame: {}", err))?;
        let image = UIImage::imageWithCGImage(&cg_image);
        let prepared = image
            .imageByPreparingThumbnailOfSize(CGSize::new(
                thumb_size as CGFloat,
                thumb_size as CGFloat,
            ))
            .unwrap_or(image);
        let source_format = if thumb_format == "png" {
            image::ImageFormat::Png
        } else {
            image::ImageFormat::Jpeg
        };
        let bytes = if thumb_format == "png" {
            prepared
                .png_representation()
                .ok_or_else(|| anyhow!("iOS media framework failed to encode PNG thumbnail"))?
                .to_vec()
        } else {
            prepared
                .jpeg_representation(thumb_quality as CGFloat / 100.0)
                .ok_or_else(|| anyhow!("iOS media framework failed to encode JPEG thumbnail"))?
                .to_vec()
        };
        if thumb_format == "webp" {
            transcode_image_bytes(&bytes, source_format, thumb_format, thumb_quality)
        } else {
            Ok(bytes)
        }
    })?;

    std::fs::write(output, output_bytes)
        .map_err(|err| anyhow!("Failed to write iOS video thumbnail: {}", err))?;
    Ok(())
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn detect_video_duration_ffmpeg(ffmpeg_path: &str, input: &PathBuf) -> Result<f64> {
    let mut cmd = Command::new(ffmpeg_path);
    cmd.arg("-i").arg(input);
    let output = cmd.output().await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr.lines() {
        if let Some((_, right)) = line.split_once("Duration:")
            && let Some((time_str, _)) = right.split_once(',')
            && let Some(duration) = parse_duration(time_str.trim())
        {
            return Ok(duration);
        }
    }
    Err(anyhow!("Duration not found"))
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn parse_duration(value: &str) -> Option<f64> {
    let mut parts = value.splitn(4, ':');
    let hours: f64 = parts.next()?.trim().parse().ok()?;
    let minutes: f64 = parts.next()?.trim().parse().ok()?;
    let seconds: f64 = parts.next()?.trim().parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(hours * 3600.0 + minutes * 60.0 + seconds)
}

fn resolve_seek_seconds(
    duration_secs: Option<f64>,
    seek_ratio: Option<f64>,
    seek_seconds: Option<u64>,
) -> u64 {
    if let Some(ratio) = seek_ratio
        && ratio.is_finite()
        && ratio > 0.0
        && ratio <= 1.0
        && let Some(duration) = duration_secs
        && duration > 0.0
    {
        return (duration * ratio) as u64;
    }

    seek_seconds.unwrap_or(3)
}

fn resolve_seek_seconds_from_config(
    duration_secs: Option<f64>,
    video_cfg: &ThumbnailRuntimeTypeConfig,
) -> u64 {
    resolve_seek_seconds(duration_secs, video_cfg.seek_ratio, video_cfg.seek_seconds)
}

#[cfg(any(target_os = "android", target_os = "ios"))]
fn scaled_dimensions(width: u32, height: u32, max_edge: u32) -> (u32, u32) {
    if width == 0 || height == 0 {
        return (max_edge.max(1), max_edge.max(1));
    }
    if width <= max_edge && height <= max_edge {
        return (width, height);
    }

    if width >= height {
        let scaled_height = ((height as u64 * max_edge as u64) / width as u64) as u32;
        (max_edge.max(1), scaled_height.max(1))
    } else {
        let scaled_width = ((width as u64 * max_edge as u64) / height as u64) as u32;
        (scaled_width.max(1), max_edge.max(1))
    }
}

#[cfg(any(target_os = "android", target_os = "ios"))]
fn normalize_output_format(format: &str) -> &str {
    if format.eq_ignore_ascii_case("jpeg") {
        "jpg"
    } else if format.eq_ignore_ascii_case("png") {
        "png"
    } else if format.eq_ignore_ascii_case("webp") {
        "webp"
    } else {
        "jpg"
    }
}

async fn run_command_with_timeout(mut cmd: Command, timeout_secs: u64) -> Result<bool> {
    let output =
        tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output()).await;
    match output {
        Ok(Ok(result)) => Ok(result.status.success()),
        Ok(Err(err)) => Err(anyhow!(err)),
        Err(_) => Err(anyhow!("Command timeout")),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    use super::parse_duration;

    #[test]
    fn resolve_seek_seconds_prefers_ratio_when_duration_is_known() {
        assert_eq!(
            super::resolve_seek_seconds(Some(20.0), Some(0.25), Some(3)),
            5
        );
        assert_eq!(super::resolve_seek_seconds(None, Some(0.25), Some(7)), 7);
        assert_eq!(super::resolve_seek_seconds(Some(20.0), None, Some(4)), 4);
    }

    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    #[test]
    fn parse_duration_valid_and_invalid_cases() {
        assert_eq!(parse_duration("01:02:03.5"), Some(3723.5));
        assert_eq!(parse_duration("00:00:00"), Some(0.0));
        assert_eq!(parse_duration("bad"), None);
        assert_eq!(parse_duration("01:02"), None);
    }
}
