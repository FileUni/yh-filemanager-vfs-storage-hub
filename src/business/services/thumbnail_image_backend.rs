use anyhow::{Result, anyhow};
use image::codecs::jpeg::JpegEncoder;
use image::codecs::webp::WebPEncoder;
use image::{DynamicImage, ExtendedColorType, ImageFormat, imageops};
use resvg::{tiny_skia, usvg};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use tokio::process::Command;
use yh_console_log::yhlog;

use super::thumbnail_runtime::{ThumbnailRuntimeConfig, ThumbnailRuntimeImageConfig};

pub async fn render_image_thumbnail(
    cfg: &ThumbnailRuntimeConfig,
    input: &PathBuf,
    output: &PathBuf,
) -> Result<bool> {
    let image_cfg = cfg.get_image();
    if image_cfg.is_builtin_backend() {
        let input = input.clone();
        let output = output.clone();
        let thumb_size = cfg.get_thumb_size_px();
        let thumb_quality = cfg.get_thumb_quality();
        let thumb_format = normalize_output_format(cfg.get_thumb_format()).to_string();
        let result = tokio::task::spawn_blocking(move || {
            render_builtin_image_thumbnail_sync(
                &input,
                &output,
                thumb_size,
                thumb_quality,
                thumb_format.as_str(),
            )
        })
        .await
        .map_err(|err| anyhow!("Builtin image thumbnail task join error: {}", err))?;

        if let Err(err) = &result {
            yhlog(
                "warn",
                &format!("Thumbnail tool failed (builtin image backend): {}", err),
            );
        }
        return result.map(|_| true);
    }

    render_external_image_thumbnail(image_cfg, cfg, input, output).await
}

pub(crate) fn encode_dynamic_image_bytes(
    image: &DynamicImage,
    target_format: &str,
    quality: u8,
) -> Result<Vec<u8>> {
    let format = normalize_output_format(target_format);
    let mut cursor = Cursor::new(Vec::new());
    match format {
        "png" => {
            image
                .write_to(&mut cursor, ImageFormat::Png)
                .map_err(|err| anyhow!("Failed to encode PNG thumbnail: {}", err))?;
        }
        "webp" => {
            let rgba = image.to_rgba8();
            WebPEncoder::new_lossless(&mut cursor)
                .encode(
                    rgba.as_raw(),
                    rgba.width(),
                    rgba.height(),
                    ExtendedColorType::Rgba8,
                )
                .map_err(|err| anyhow!("Failed to encode WebP thumbnail: {}", err))?;
        }
        _ => {
            let rgb = image.to_rgb8();
            let mut encoder = JpegEncoder::new_with_quality(&mut cursor, quality);
            encoder
                .encode(
                    rgb.as_raw(),
                    rgb.width(),
                    rgb.height(),
                    ExtendedColorType::Rgb8,
                )
                .map_err(|err| anyhow!("Failed to encode JPEG thumbnail: {}", err))?;
        }
    }
    Ok(cursor.into_inner())
}

#[cfg(any(target_os = "android", target_os = "ios"))]
pub(crate) fn transcode_image_bytes(
    bytes: &[u8],
    source_format: ImageFormat,
    target_format: &str,
    quality: u8,
) -> Result<Vec<u8>> {
    let image = image::load_from_memory_with_format(bytes, source_format)
        .map_err(|err| anyhow!("Failed to decode platform thumbnail bytes: {}", err))?;
    encode_dynamic_image_bytes(&image, target_format, quality)
}

fn render_builtin_image_thumbnail_sync(
    input: &Path,
    output: &Path,
    thumb_size: u32,
    thumb_quality: u8,
    thumb_format: &str,
) -> Result<()> {
    let source_bytes = std::fs::read(input)
        .map_err(|err| anyhow!("Failed to read image source for builtin thumbnail: {}", err))?;
    let ext = input
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();

    let image = if ext == "svg" {
        render_svg_image(&source_bytes, thumb_size)?
    } else {
        image::load_from_memory(&source_bytes)
            .map_err(|err| anyhow!("Failed to decode image for builtin thumbnail: {}", err))?
    };

    let image = apply_exif_orientation(image, &source_bytes);
    let resized = image.thumbnail(thumb_size, thumb_size);
    let encoded = encode_dynamic_image_bytes(&resized, thumb_format, thumb_quality)?;
    std::fs::write(output, encoded)
        .map_err(|err| anyhow!("Failed to write builtin image thumbnail: {}", err))?;
    Ok(())
}

fn render_svg_image(source_bytes: &[u8], thumb_size: u32) -> Result<DynamicImage> {
    let tree = usvg::Tree::from_data(source_bytes, &usvg::Options::default())
        .map_err(|err| anyhow!("Failed to parse SVG for thumbnail: {}", err))?;
    let svg_size = tree.size();
    let (target_width, target_height) = scaled_dimensions(
        svg_size.width().round() as u32,
        svg_size.height().round() as u32,
        thumb_size,
    );
    let mut pixmap = tiny_skia::Pixmap::new(target_width, target_height)
        .ok_or_else(|| anyhow!("Failed to allocate SVG thumbnail pixmap"))?;
    let sx = target_width as f32 / svg_size.width();
    let sy = target_height as f32 / svg_size.height();
    let mut pixmap_mut = pixmap.as_mut();
    resvg::render(
        &tree,
        tiny_skia::Transform::from_scale(sx, sy),
        &mut pixmap_mut,
    );
    let image = image::RgbaImage::from_raw(target_width, target_height, pixmap.take())
        .ok_or_else(|| anyhow!("Failed to materialize SVG thumbnail pixels"))?;
    Ok(DynamicImage::ImageRgba8(image))
}

fn apply_exif_orientation(image: DynamicImage, source_bytes: &[u8]) -> DynamicImage {
    let orientation = exif::Reader::new()
        .read_from_container(&mut Cursor::new(source_bytes))
        .ok()
        .and_then(|value| {
            value
                .get_field(exif::Tag::Orientation, exif::In::PRIMARY)
                .and_then(|field| field.value.get_uint(0))
        });

    match orientation {
        Some(2) => DynamicImage::ImageRgba8(imageops::flip_horizontal(&image.to_rgba8())),
        Some(3) => DynamicImage::ImageRgba8(imageops::rotate180(&image.to_rgba8())),
        Some(4) => DynamicImage::ImageRgba8(imageops::flip_vertical(&image.to_rgba8())),
        Some(5) => {
            let flipped = imageops::flip_horizontal(&image.to_rgba8());
            DynamicImage::ImageRgba8(imageops::rotate90(&flipped))
        }
        Some(6) => DynamicImage::ImageRgba8(imageops::rotate90(&image.to_rgba8())),
        Some(7) => {
            let flipped = imageops::flip_horizontal(&image.to_rgba8());
            DynamicImage::ImageRgba8(imageops::rotate270(&flipped))
        }
        Some(8) => DynamicImage::ImageRgba8(imageops::rotate270(&image.to_rgba8())),
        _ => image,
    }
}

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

async fn render_external_image_thumbnail(
    image_cfg: &ThumbnailRuntimeImageConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &PathBuf,
    output: &PathBuf,
) -> Result<bool> {
    let tools = cfg.get_tools();
    let vips_path = (!tools.vips_path.trim().is_empty()).then(|| tools.vips_path.clone());
    let magick_path =
        (!tools.imagemagick_path.trim().is_empty()).then(|| tools.imagemagick_path.clone());
    let size = cfg.get_thumb_size_px().to_string();
    let quality = cfg.get_thumb_quality().to_string();
    let max_magick_bytes = image_cfg.get_imagemagick_max_mb() * 1024 * 1024;

    if let Some(vips_path) = vips_path {
        let mut cmd = Command::new(&vips_path);
        let mut output_arg = output.to_string_lossy().to_string();
        if output_arg.ends_with(".jpg")
            || output_arg.ends_with(".jpeg")
            || output_arg.ends_with(".webp")
        {
            output_arg = format!("{}[Q={}]", output_arg, quality);
        }
        cmd.arg(input)
            .arg("-s")
            .arg(&size)
            .arg("-o")
            .arg(output_arg);
        let timeout = image_cfg.get_timeout_secs();
        match run_command_with_timeout(cmd, timeout).await {
            Ok(true) => {
                if tokio::fs::metadata(output).await.is_ok() {
                    return Ok(true);
                }
            }
            Ok(false) => {}
            Err(err) => {
                yhlog(
                    "warn",
                    &format!(
                        "Thumbnail tool failed (vips, path='{}'): {}",
                        vips_path, err
                    ),
                );
            }
        }
    }

    if let Some(magick_path) = magick_path {
        let meta = tokio::fs::metadata(input).await?;
        if meta.len() > max_magick_bytes {
            return Ok(false);
        }
        let mut cmd = Command::new(&magick_path);
        cmd.arg(input)
            .arg("-thumbnail")
            .arg(format!("{}x{}", size, size))
            .arg("-quality")
            .arg(&quality)
            .arg(output);
        let timeout = image_cfg.get_timeout_secs();
        return match run_command_with_timeout(cmd, timeout).await {
            Ok(value) => Ok(value),
            Err(err) => {
                yhlog(
                    "warn",
                    &format!(
                        "Thumbnail tool failed (imagemagick, path='{}'): {}",
                        magick_path, err
                    ),
                );
                Err(err)
            }
        };
    }

    Ok(false)
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
