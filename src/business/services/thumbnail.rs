// Thumbnail service
use crate::VfsStorage;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use yh_console_log::yhlog;

use super::UserSettingsSnapshot;
use super::thumbnail_image_backend::render_image_thumbnail;
use super::thumbnail_model3d_backend::render_model3d_thumbnail;
use super::thumbnail_runtime::{
    LatexPreviewRuntimeConfig, ThumbnailRuntimeConfig, ThumbnailRuntimeTypeConfig,
    ThumbnailServiceContext, guess_mime_type, normalize_logical_path,
};
use super::thumbnail_video_backend::render_video_thumbnail;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThumbnailKind {
    Image,
    Video,
    Pdf,
    Office,
    Text,
    Latex,
    Model3d,
}
struct ThumbnailPaths {
    dir: String,
    file: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThumbnailCacheMode {
    Dir,
    Global,
    None,
}
const IMAGE_EXTS: &[&str] = &[
    "jpg", "jpeg", "png", "webp", "gif", "bmp", "tiff", "tif", "svg", "psd", "ai",
];
const VIDEO_EXTS: &[&str] = &["mp4", "mov", "mkv", "avi", "webm", "m4v", "mpg", "mpeg"];
const PDF_EXTS: &[&str] = &["pdf"];
const LATEX_EXTS: &[&str] = &["tex", "latex"];
const MODEL3D_EXTS: &[&str] = &["obj", "stl", "gltf", "glb"];
const OFFICE_EXTS: &[&str] = &[
    "doc", "docx", "dot", "dotx", "docm", "dotm", "xls", "xlsx", "xltx", "xlsm", "xltm", "ppt",
    "pptx", "potx", "pptm", "pps", "ppsx",
];
const TEXT_EXTS: &[&str] = &[
    "txt", "md", "markdown", "log", "json", "yaml", "yml", "toml", "ini", "csv", "tsv", "xml",
    "html", "htm", "css", "js", "ts", "rs", "py", "go", "java", "c", "cpp", "h", "hpp", "sh",
];
impl ThumbnailServiceContext {
    pub async fn get_thumbnail_bytes(
        &self,
        storage: std::sync::Arc<dyn VfsStorage>,
        logical_path: &str,
        user_id: &str,
        role_id: &str,
    ) -> Result<(Bytes, String)> {
        self.get_thumbnail_bytes_internal(storage, logical_path, user_id, role_id, false)
            .await
    }

    pub async fn get_thumbnail_bytes_for_nextcloud_preview(
        &self,
        storage: std::sync::Arc<dyn VfsStorage>,
        logical_path: &str,
        user_id: &str,
        role_id: &str,
    ) -> Result<(Bytes, String)> {
        self.get_thumbnail_bytes_internal(storage, logical_path, user_id, role_id, true)
            .await
    }

    async fn get_thumbnail_bytes_internal(
        &self,
        storage: std::sync::Arc<dyn VfsStorage>,
        logical_path: &str,
        user_id: &str,
        role_id: &str,
        ignore_max_size_limit: bool,
    ) -> Result<(Bytes, String)> {
        let thumb_cfg = &self.thumbnail;
        let latex_cfg = &self.latex;
        let logical_path = normalize_logical_path(logical_path);
        let info = storage.stat(&logical_path).await?;
        if info.is_dir {
            return Err(anyhow!("Thumbnail not supported for directory"));
        }
        if info.size == 0 {
            return Err(anyhow!("Thumbnail not supported for empty file"));
        }
        if !thumb_cfg.is_enabled() {
            return Err(anyhow!("Thumbnail disabled"));
        }
        let ext = get_extension(&logical_path);
        let kind =
            detect_kind(&ext).ok_or_else(|| anyhow!("Thumbnail not supported for extension"))?;
        let user_settings = self
            .storage_hub
            .ensure_user_settings(&self.db, user_id, role_id)
            .await?;
        let user_settings = UserSettingsSnapshot::from(user_settings.as_ref());
        if is_user_thumbnail_disabled(&user_settings, kind, &ext) {
            return Err(anyhow!("Thumbnail disabled for user"));
        }
        if !is_kind_enabled(thumb_cfg, latex_cfg, kind)? {
            return Err(anyhow!("Thumbnail disabled for file type"));
        }
        let parent_dir = parent_dir(&logical_path);
        if is_directory_thumbnail_disabled(&storage, &parent_dir).await? {
            return Err(anyhow!("Thumbnail disabled for directory"));
        }
        let max_bytes = max_size_bytes(thumb_cfg, latex_cfg, kind)?;
        if !ignore_max_size_limit && info.size > max_bytes {
            return Err(anyhow!("Thumbnail exceeds size limit"));
        }
        if kind == ThumbnailKind::Image {
            let small_skip_bytes = thumb_cfg.get_image().get_small_skip_mb() * 1024 * 1024;
            if info.size <= small_skip_bytes {
                let (data, _meta) = storage.read(&logical_path).await?;
                let mime = guess_mime_type(Path::new(&logical_path));
                return Ok((data, mime));
            }
        }
        let format = thumb_cfg.get_thumb_format();
        let format_norm = normalize_format(format);
        let paths = build_thumbnail_paths(thumb_cfg, &logical_path)?;
        if let Some(paths) = &paths
            && storage.exists(&paths.file).await?
        {
            let (data, _meta) = storage.read(&paths.file).await?;
            let content_type = thumbnail_content_type(format);
            return Ok((data, content_type));
        }
        let manager = yh_config_infra::config_require_manager!(
            yh_external_process_manager::get_global_manager(),
            "external_process_manager"
        );
        if manager.available_permits() == 0 {
            return Err(anyhow!("System busy: no process slots available"));
        }
        let temp_manager = yh_config_infra::config_require_manager!(
            crate::get_global_temp_manager().await,
            "vfs_storage_hub"
        );
        let (temp_dir, _guard) = temp_manager
            .create_user_temp_dir(user_id, "thumbnail")
            .await?;
        let input_path = match kind {
            ThumbnailKind::Latex => temp_dir.join("main.tex"),
            _ => {
                let suffix = if ext.is_empty() {
                    "input".to_string()
                } else {
                    format!("input.{}", ext)
                };
                temp_dir.join(suffix)
            }
        };
        download_to_local(&storage, &logical_path, &input_path).await?;
        let output_path = temp_dir.join(format!("output.{}", format_norm));
        let result = manager
            .run_with_permit(yh_external_process_manager::TaskPriority::High, || async {
                match kind {
                    ThumbnailKind::Image => {
                        render_image_thumbnail(thumb_cfg, &input_path, &output_path).await
                    }
                    ThumbnailKind::Video => {
                        render_video_thumbnail(
                            thumb_cfg.get_video(),
                            thumb_cfg,
                            &input_path,
                            &output_path,
                        )
                        .await
                    }
                    ThumbnailKind::Pdf => {
                        render_pdf_thumbnail(
                            thumb_cfg.get_pdf(),
                            thumb_cfg,
                            &input_path,
                            &output_path,
                        )
                        .await
                    }
                    ThumbnailKind::Office => {
                        render_office_thumbnail(
                            thumb_cfg.get_office(),
                            thumb_cfg,
                            &input_path,
                            &output_path,
                        )
                        .await
                    }
                    ThumbnailKind::Text => {
                        render_text_thumbnail(
                            thumb_cfg.get_text(),
                            thumb_cfg,
                            &input_path,
                            &output_path,
                            &storage,
                            &logical_path,
                        )
                        .await
                    }
                    ThumbnailKind::Latex => {
                        render_latex_thumbnail(
                            latex_cfg,
                            thumb_cfg.get_pdf(),
                            thumb_cfg,
                            &input_path,
                            &output_path,
                        )
                        .await
                    }
                    ThumbnailKind::Model3d => {
                        render_model3d_thumbnail(
                            thumb_cfg.get_model3d(),
                            thumb_cfg,
                            &input_path,
                            &output_path,
                        )
                        .await
                    }
                }
            })
            .await?;
        if !result {
            return Err(anyhow!("Thumbnail generation failed"));
        }
        let output_bytes = Bytes::from(tokio::fs::read(&output_path).await?);
        if output_bytes.is_empty() {
            return Err(anyhow!("Thumbnail output is empty"));
        }
        if let Some(paths) = &paths {
            ensure_dir(&storage, &paths.dir).await?;
            let write_bytes = output_bytes.slice(..);
            let _ = storage.write(&paths.file, write_bytes).await?;
        }
        let content_type = thumbnail_content_type(format);
        Ok((output_bytes, content_type))
    }
    pub async fn remove_thumbnail_by_path(
        &self,
        storage: std::sync::Arc<dyn VfsStorage>,
        logical_path: &str,
    ) -> Result<()> {
        let logical_path = normalize_logical_path(logical_path);
        let Some(paths) = build_thumbnail_paths(&self.thumbnail, &logical_path)? else {
            return Ok(());
        };
        if storage.exists(&paths.file).await? {
            let _ = storage.delete(&paths.file).await;
        }
        Ok(())
    }
    pub async fn move_thumbnail_by_path(
        &self,
        storage: std::sync::Arc<dyn VfsStorage>,
        src_path: &str,
        dst_path: &str,
    ) -> Result<()> {
        let src_path = normalize_logical_path(src_path);
        let dst_path = normalize_logical_path(dst_path);
        let Some(src_paths) = build_thumbnail_paths(&self.thumbnail, &src_path)? else {
            return Ok(());
        };
        let Some(dst_paths) = build_thumbnail_paths(&self.thumbnail, &dst_path)? else {
            return Ok(());
        };
        if src_paths.file == dst_paths.file {
            return Ok(());
        }
        if !storage.exists(&src_paths.file).await? {
            return Ok(());
        }
        ensure_dir(&storage, &dst_paths.dir).await?;
        let move_result = storage.move_file(&src_paths.file, &dst_paths.file).await;
        if move_result.is_err() {
            let (data, _meta) = storage.read(&src_paths.file).await?;
            let _ = storage.write(&dst_paths.file, data).await?;
            let _ = storage.delete(&src_paths.file).await;
        }
        Ok(())
    }
    pub async fn clear_thumbnail_cache(
        &self,
        storage: std::sync::Arc<dyn VfsStorage>,
        scope: ThumbnailClearScope,
    ) -> Result<u64> {
        match scope {
            ThumbnailClearScope::All => clear_all_thumbnails(&storage, &self.thumbnail).await,
            ThumbnailClearScope::Directory(path) => {
                clear_directory_thumbnails(&storage, &self.thumbnail, &path).await
            }
        }
    }
    pub async fn set_thumbnail_disabled(
        &self,
        storage: std::sync::Arc<dyn VfsStorage>,
        dir_path: &str,
        disabled: bool,
    ) -> Result<()> {
        let dir_path = normalize_logical_path(dir_path);
        let thumb_dir = build_thumb_dir(&dir_path);
        let marker = build_disable_marker(&thumb_dir);
        if disabled {
            ensure_dir(&storage, &thumb_dir).await?;
            let _ = storage
                .write(&marker, Bytes::from("disabled".as_bytes().to_vec()))
                .await?;
        } else if storage.exists(&marker).await? {
            let _ = storage.delete(&marker).await;
        }
        Ok(())
    }
    pub async fn is_thumbnail_disabled(
        &self,
        storage: std::sync::Arc<dyn VfsStorage>,
        dir_path: &str,
    ) -> Result<bool> {
        let dir_path = normalize_logical_path(dir_path);
        let thumb_dir = build_thumb_dir(&dir_path);
        let marker = build_disable_marker(&thumb_dir);
        Ok(storage.exists(&marker).await?)
    }
}
fn is_user_thumbnail_disabled(
    settings: &UserSettingsSnapshot,
    kind: ThumbnailKind,
    ext: &str,
) -> bool {
    match kind {
        ThumbnailKind::Image => settings.thumbnail_disable_image,
        ThumbnailKind::Video => settings.thumbnail_disable_video,
        ThumbnailKind::Pdf => settings.thumbnail_disable_pdf,
        ThumbnailKind::Office => settings.thumbnail_disable_office,
        ThumbnailKind::Text => {
            if ext == "md" || ext == "markdown" {
                settings.thumbnail_disable_markdown
            } else {
                settings.thumbnail_disable_text
            }
        }
        ThumbnailKind::Latex => settings.thumbnail_disable_tex,
        ThumbnailKind::Model3d => settings.thumbnail_disable_image,
    }
}
pub enum ThumbnailClearScope {
    All,
    Directory(String),
}
fn detect_kind(ext: &str) -> Option<ThumbnailKind> {
    let ext = ext.to_lowercase();
    if IMAGE_EXTS.contains(&ext.as_str()) {
        Some(ThumbnailKind::Image)
    } else if VIDEO_EXTS.contains(&ext.as_str()) {
        Some(ThumbnailKind::Video)
    } else if PDF_EXTS.contains(&ext.as_str()) {
        Some(ThumbnailKind::Pdf)
    } else if OFFICE_EXTS.contains(&ext.as_str()) {
        Some(ThumbnailKind::Office)
    } else if LATEX_EXTS.contains(&ext.as_str()) {
        Some(ThumbnailKind::Latex)
    } else if MODEL3D_EXTS.contains(&ext.as_str()) {
        Some(ThumbnailKind::Model3d)
    } else if TEXT_EXTS.contains(&ext.as_str()) {
        Some(ThumbnailKind::Text)
    } else {
        None
    }
}
fn is_kind_enabled(
    cfg: &ThumbnailRuntimeConfig,
    latex_cfg: &LatexPreviewRuntimeConfig,
    kind: ThumbnailKind,
) -> Result<bool> {
    let enabled = match kind {
        ThumbnailKind::Image => cfg.get_image().is_enabled(),
        ThumbnailKind::Video => cfg.get_video().is_enabled(),
        ThumbnailKind::Pdf => cfg.get_pdf().is_enabled(),
        ThumbnailKind::Office => cfg.get_office().is_enabled(),
        ThumbnailKind::Text => cfg.get_text().is_enabled(),
        ThumbnailKind::Latex => cfg.get_text().is_enabled() && latex_cfg.is_enable_latexmk(),
        ThumbnailKind::Model3d => cfg.get_model3d().is_enabled(),
    };
    Ok(enabled)
}
fn get_extension(path: &str) -> String {
    Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map_or("", |v| v)
        .to_lowercase()
}
fn parent_dir(path: &str) -> String {
    Path::new(path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "/".to_string())
}
fn normalize_format(format: &str) -> String {
    if format == "jpeg" {
        "jpg".to_string()
    } else {
        format.to_string()
    }
}
fn hash_path(path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    hex::encode(hasher.finalize())
}
fn build_thumb_dir(dir_path: &str) -> String {
    if dir_path == "/" {
        "/.thumbs".to_string()
    } else {
        format!("{}/.thumbs", dir_path.trim_end_matches('/'))
    }
}
fn build_disable_marker(thumb_dir: &str) -> String {
    if thumb_dir == "/" {
        "/.thumbs/.disabled".to_string()
    } else {
        format!("{}/.disabled", thumb_dir.trim_end_matches('/'))
    }
}
fn parse_thumbnail_cache_mode(cfg: &ThumbnailRuntimeConfig) -> Result<ThumbnailCacheMode> {
    match cfg.get_cache_mode().trim().to_ascii_lowercase().as_str() {
        "dir" => Ok(ThumbnailCacheMode::Dir),
        "global" | "db" => Ok(ThumbnailCacheMode::Global),
        "none" => Ok(ThumbnailCacheMode::None),
        other => Err(anyhow!("Unsupported thumbnail.cache_mode: {}", other)),
    }
}
fn build_thumbnail_paths(
    cfg: &ThumbnailRuntimeConfig,
    logical_path: &str,
) -> Result<Option<ThumbnailPaths>> {
    let cache_mode = parse_thumbnail_cache_mode(cfg)?;
    let cache_dir = cfg.get_cache_dir();
    let format = normalize_format(cfg.get_thumb_format());
    let hash = hash_path(logical_path);
    let file_name = format!("{}.{}", hash, format);
    if cache_mode == ThumbnailCacheMode::Global {
        let (prefix1, rest) = hash.split_at(2);
        let (prefix2, _) = rest.split_at(2);
        let dir = format!(
            "{}/{}/{}",
            cache_dir.trim_end_matches('/'),
            prefix1,
            prefix2
        );
        let file = format!("{}/{}", dir, file_name);
        Ok(Some(ThumbnailPaths { dir, file }))
    } else if cache_mode == ThumbnailCacheMode::Dir {
        let parent = parent_dir(logical_path);
        let dir = build_thumb_dir(&parent);
        let file = format!("{}/{}", dir.trim_end_matches('/'), file_name);
        Ok(Some(ThumbnailPaths { dir, file }))
    } else {
        Ok(None)
    }
}
fn thumbnail_content_type(format: &str) -> String {
    match normalize_format(format).as_str() {
        "png" => "image/png".to_string(),
        "webp" => "image/webp".to_string(),
        _ => "image/jpeg".to_string(),
    }
}
fn max_size_bytes(
    cfg: &ThumbnailRuntimeConfig,
    latex_cfg: &LatexPreviewRuntimeConfig,
    kind: ThumbnailKind,
) -> Result<u64> {
    let mb = match kind {
        ThumbnailKind::Image => cfg.get_image().get_max_size_mb(),
        ThumbnailKind::Video => cfg.get_video().get_max_size_mb(),
        ThumbnailKind::Pdf => cfg.get_pdf().get_max_size_mb(),
        ThumbnailKind::Office => cfg.get_office().get_max_size_mb(),
        ThumbnailKind::Text => cfg.get_text().get_max_size_mb(),
        ThumbnailKind::Latex => latex_cfg.get_max_input_size_mb(),
        ThumbnailKind::Model3d => cfg.get_model3d().get_max_size_mb(),
    };
    Ok(mb * 1024 * 1024)
}
async fn ensure_dir(storage: &std::sync::Arc<dyn VfsStorage>, dir: &str) -> Result<()> {
    if !storage.exists(dir).await? {
        let _ = storage.create_dir_all(dir).await?;
    }
    Ok(())
}
async fn is_directory_thumbnail_disabled(
    storage: &std::sync::Arc<dyn VfsStorage>,
    dir_path: &str,
) -> Result<bool> {
    let thumb_dir = build_thumb_dir(dir_path);
    let marker = build_disable_marker(&thumb_dir);
    Ok(storage.exists(&marker).await?)
}
async fn download_to_local(
    storage: &std::sync::Arc<dyn VfsStorage>,
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
async fn run_command_with_timeout(mut cmd: Command, timeout_secs: u64) -> Result<bool> {
    let output =
        tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output()).await;
    match output {
        Ok(Ok(res)) => Ok(res.status.success()),
        Ok(Err(err)) => Err(anyhow!(err)),
        Err(_) => Err(anyhow!("Command timeout")),
    }
}
async fn render_pdf_thumbnail(
    pdf_cfg: &ThumbnailRuntimeTypeConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &Path,
    output: &Path,
) -> Result<bool> {
    let tools = cfg.get_tools();
    let vips_path = tools.get_vips_path();
    let magick_path = tools.get_imagemagick_path();
    let size = cfg.get_thumb_size_px().to_string();
    let quality = cfg.get_thumb_quality().to_string();
    if !vips_path.is_empty() {
        let mut cmd = Command::new(vips_path);
        let mut output_arg = output.to_string_lossy().to_string();
        if output_arg.ends_with(".jpg")
            || output_arg.ends_with(".jpeg")
            || output_arg.ends_with(".webp")
        {
            output_arg = format!("{}[Q={}]", output_arg, quality);
        }
        cmd.arg(format!("{}[0]", input.to_string_lossy()))
            .arg("-s")
            .arg(&size)
            .arg("-o")
            .arg(output_arg);
        let timeout = pdf_cfg.get_timeout_secs();
        match run_command_with_timeout(cmd, timeout).await {
            Ok(true) => {
                if tokio::fs::metadata(output).await.is_ok() {
                    return Ok(true);
                }
            }
            Ok(false) => {}
            Err(e) => {
                yhlog(
                    "warn",
                    &format!("Thumbnail tool failed (vips, path='{}'): {}", vips_path, e),
                );
            }
        }
    }
    if !magick_path.is_empty() {
        let mut cmd = Command::new(magick_path);
        cmd.arg(format!("{}[0]", input.to_string_lossy()))
            .arg("-thumbnail")
            .arg(format!("{}x{}", size, size))
            .arg("-quality")
            .arg(&quality)
            .arg(output);
        let timeout = pdf_cfg.get_timeout_secs();
        return match run_command_with_timeout(cmd, timeout).await {
            Ok(v) => Ok(v),
            Err(e) => {
                yhlog(
                    "warn",
                    &format!(
                        "Thumbnail tool failed (imagemagick, path='{}'): {}",
                        magick_path, e
                    ),
                );
                Err(e)
            }
        };
    }
    Ok(false)
}
async fn render_latex_thumbnail(
    latex_cfg: &LatexPreviewRuntimeConfig,
    pdf_cfg: &ThumbnailRuntimeTypeConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &Path,
    output: &Path,
) -> Result<bool> {
    let latexmk_path = latex_cfg.get_latexmk_path();
    let timeout = latex_cfg.get_latexmk_timeout_secs();
    let max_output_mb = latex_cfg.get_max_output_size_mb();
    let allow_shell_escape = latex_cfg.is_allow_shell_escape();
    let temp_dir = input.parent().ok_or_else(|| anyhow!("Invalid temp dir"))?;
    let input_name = input
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("Latex input filename missing"))?;
    let compile_input_name = prepare_latex_compile_input(temp_dir, input, input_name).await?;
    let job_name = "rs_thumb";
    let output_pdf = temp_dir.join(format!("{}.pdf", job_name));
    let mut cmd = Command::new(latexmk_path);
    cmd.current_dir(temp_dir)
        .arg("-pdf")
        .arg("-xelatex")
        .arg("-interaction=nonstopmode")
        .arg("-halt-on-error")
        .arg("-file-line-error")
        .arg(format!("-jobname={}", job_name))
        .arg(format!("-outdir={}", temp_dir.display()))
        .arg(format!("-auxdir={}", temp_dir.display()))
        .arg(compile_input_name);
    if allow_shell_escape {
        cmd.arg("-shell-escape");
    } else {
        cmd.arg("-no-shell-escape");
    }
    let ok = run_command_with_timeout(cmd, timeout).await?;
    if !ok {
        return Ok(false);
    }
    let meta = tokio::fs::metadata(&output_pdf).await?;
    if meta.len() > max_output_mb * 1024 * 1024 {
        return Ok(false);
    }
    render_pdf_thumbnail(pdf_cfg, cfg, &output_pdf, output).await
}
fn contains_cjk(text: &str) -> bool {
    text.chars()
        .any(|ch| ('\u{4E00}'..='\u{9FFF}').contains(&ch))
}
fn normalize_latex_source(content: &str) -> String {
    let mut packages: Vec<&str> = Vec::new();
    let has_ctex = content
        .lines()
        .any(|line| line.trim_start().starts_with("\\documentclass") && line.contains("ctex"))
        || content.contains("\\usepackage{ctex");
    if !has_ctex && contains_cjk(content) {
        packages.push("ctex");
    }
    if !content.contains("\\usepackage{amsmath}") && content.contains("\\text") {
        packages.push("amsmath");
    }
    if packages.is_empty() {
        return content.to_string();
    }
    if let Some(idx) = content
        .lines()
        .position(|line| line.trim_start().starts_with("\\documentclass"))
    {
        let mut output = String::new();
        for (line_idx, line) in content.lines().enumerate() {
            output.push_str(line);
            output.push('\n');
            if line_idx == idx {
                for package in &packages {
                    output.push_str("\\usepackage{");
                    output.push_str(package);
                    output.push_str("}\n");
                }
            }
        }
        return output;
    }
    let mut output = String::from("\\documentclass{article}\n");
    for package in &packages {
        output.push_str("\\usepackage{");
        output.push_str(package);
        output.push_str("}\n");
    }
    output.push_str("\\begin{document}\n");
    output.push_str(content);
    if !content.ends_with('\n') {
        output.push('\n');
    }
    output.push_str("\\end{document}\n");
    output
}
async fn prepare_latex_compile_input(
    temp_dir: &Path,
    input: &Path,
    fallback_input_name: &str,
) -> Result<String> {
    let source = match tokio::fs::read_to_string(input).await {
        Ok(content) => content,
        Err(_) => return Ok(fallback_input_name.to_string()),
    };
    let normalized = normalize_latex_source(&source);
    let generated_name = "thumb_main.tex";
    let generated_path = temp_dir.join(generated_name);
    tokio::fs::write(generated_path, normalized).await?;
    Ok(generated_name.to_string())
}
async fn render_office_thumbnail(
    office_cfg: &ThumbnailRuntimeTypeConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &Path,
    output: &Path,
) -> Result<bool> {
    let libreoffice_path = cfg.get_tools().get_libreoffice_path();
    let temp_dir = input.parent().ok_or_else(|| anyhow!("Invalid temp dir"))?;
    let mut cmd = Command::new(libreoffice_path);
    cmd.arg("--headless")
        .arg("--convert-to")
        .arg("pdf")
        .arg("--outdir")
        .arg(temp_dir)
        .arg(input);
    let timeout = office_cfg.get_timeout_secs();
    let ok = match run_command_with_timeout(cmd, timeout).await {
        Ok(v) => v,
        Err(e) => {
            yhlog(
                "warn",
                &format!(
                    "Thumbnail tool failed (libreoffice, path='{}'): {}",
                    libreoffice_path, e
                ),
            );
            return Err(e);
        }
    };
    if !ok {
        return Ok(false);
    }
    let pdf_path = temp_dir.join(format!(
        "{}.pdf",
        input
            .file_stem()
            .and_then(|s| s.to_str())
            .map_or("output", |v| v)
    ));
    if tokio::fs::metadata(&pdf_path).await.is_err() {
        return Ok(false);
    }
    render_pdf_thumbnail(cfg.get_pdf(), cfg, &pdf_path, output).await
}
async fn render_text_thumbnail(
    text_cfg: &ThumbnailRuntimeTypeConfig,
    cfg: &ThumbnailRuntimeConfig,
    input: &Path,
    output: &Path,
    storage: &std::sync::Arc<dyn VfsStorage>,
    logical_path: &str,
) -> Result<bool> {
    let magick_path = cfg.get_tools().get_imagemagick_path();
    if magick_path.is_empty() {
        return Err(anyhow!(
            "ImageMagick path not configured for text thumbnails"
        ));
    }
    let max_chars = text_cfg.get_max_chars() as usize;
    let max_bytes = (max_chars as u64).saturating_mul(4);
    let info = storage.stat(logical_path).await?;
    let read_bytes = std::cmp::min(info.size, max_bytes);
    if read_bytes == 0 {
        return Ok(false);
    }
    let (data, _meta) = storage.read_range(logical_path, 0, read_bytes).await?;
    let mut text = String::from_utf8_lossy(&data).to_string();
    text.retain(|c| c != '\0');
    if text.chars().count() > max_chars {
        text = text.chars().take(max_chars).collect();
    }
    let text_path = input
        .parent()
        .unwrap_or_else(|| Path::new("/tmp"))
        .join("preview.txt");
    tokio::fs::write(&text_path, text).await?;
    let size = cfg.get_thumb_size_px().to_string();
    let quality = cfg.get_thumb_quality().to_string();
    let mut cmd = Command::new(magick_path);
    cmd.arg("-background")
        .arg("white")
        .arg("-fill")
        .arg("#111111")
        .arg("-pointsize")
        .arg("14")
        .arg("-size")
        .arg(format!("{}x{}", size, size))
        .arg(format!("caption:@{}", text_path.to_string_lossy()))
        .arg("-quality")
        .arg(&quality)
        .arg(output);
    let timeout = text_cfg.get_timeout_secs();
    match run_command_with_timeout(cmd, timeout).await {
        Ok(v) => Ok(v),
        Err(e) => {
            yhlog(
                "warn",
                &format!(
                    "Thumbnail tool failed (imagemagick, path='{}'): {}",
                    magick_path, e
                ),
            );
            Err(e)
        }
    }
}
async fn clear_directory_thumbnails(
    storage: &std::sync::Arc<dyn VfsStorage>,
    cfg: &ThumbnailRuntimeConfig,
    dir_path: &str,
) -> Result<u64> {
    let dir_path = normalize_logical_path(dir_path);
    let cache_mode = parse_thumbnail_cache_mode(cfg)?;
    if cache_mode == ThumbnailCacheMode::None {
        return Ok(0);
    }
    if cache_mode == ThumbnailCacheMode::Global {
        return clear_global_thumbnails(storage, cfg, Some(&dir_path)).await;
    }
    let thumb_dir = build_thumb_dir(&dir_path);
    if !storage.exists(&thumb_dir).await? {
        return Ok(0);
    }
    let entries = storage.list(&thumb_dir).await?;
    let mut count = 0u64;
    for entry in entries {
        if entry.name.as_ref() == ".disabled" {
            continue;
        }
        let _ = storage.delete(&entry.path).await;
        count += 1;
    }
    Ok(count)
}
async fn clear_all_thumbnails(
    storage: &std::sync::Arc<dyn VfsStorage>,
    cfg: &ThumbnailRuntimeConfig,
) -> Result<u64> {
    let cache_mode = parse_thumbnail_cache_mode(cfg)?;
    if cache_mode == ThumbnailCacheMode::None {
        return Ok(0);
    }
    if cache_mode == ThumbnailCacheMode::Global {
        return clear_global_thumbnails(storage, cfg, None).await;
    }
    let mut count = 0u64;
    let mut dirs = Vec::new();
    dirs.push("/".to_string());
    let entries = storage.list_recursive("/").await?;
    for entry in entries {
        if entry.is_dir {
            dirs.push(entry.path.to_string());
        }
    }
    for dir in dirs {
        count += clear_directory_thumbnails(storage, cfg, &dir).await?;
    }
    Ok(count)
}
async fn clear_global_thumbnails(
    storage: &std::sync::Arc<dyn VfsStorage>,
    cfg: &ThumbnailRuntimeConfig,
    dir: Option<&str>,
) -> Result<u64> {
    if let Some(target_dir) = dir {
        let entries = storage.list(target_dir).await?;
        let mut count = 0u64;
        for entry in entries {
            if entry.is_dir {
                continue;
            }
            let Some(paths) = build_thumbnail_paths(cfg, &entry.path)? else {
                continue;
            };
            if storage.exists(&paths.file).await? {
                let _ = storage.delete(&paths.file).await;
                count += 1;
            }
        }
        return Ok(count);
    }
    let cache_dir = cfg.get_cache_dir();
    if !storage.exists(cache_dir).await? {
        return Ok(0);
    }
    let mut paths: Vec<String> = storage
        .list_recursive(cache_dir)
        .await?
        .into_iter()
        .map(|entry| entry.path.to_string())
        .collect();
    paths.sort_by_key(|path| std::cmp::Reverse(path.len()));
    let mut count = 0u64;
    for path in paths {
        if storage.exists(&path).await? {
            let _ = storage.delete(&path).await;
            count += 1;
        }
    }
    if storage.exists(cache_dir).await? {
        let _ = storage.delete(cache_dir).await;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_latex_source_adds_ctex_when_cjk_present() {
        let src = "\\documentclass{article}\n\\begin{document}\n你好\n\\end{document}\n";
        let normalized = normalize_latex_source(src);
        assert!(normalized.contains("\\usepackage{ctex}"));
    }

    #[test]
    fn normalize_latex_source_adds_amsmath_when_text_command_present() {
        let src = "\\documentclass{article}\n\\begin{document}\n\\text{demo}\n\\end{document}\n";
        let normalized = normalize_latex_source(src);
        assert!(normalized.contains("\\usepackage{amsmath}"));
    }

    #[test]
    fn build_thumb_dir_and_disable_marker_paths_are_stable() {
        assert_eq!(build_thumb_dir("/"), "/.thumbs");
        assert_eq!(build_thumb_dir("/docs/sub/"), "/docs/sub/.thumbs");
        assert_eq!(build_disable_marker("/.thumbs"), "/.thumbs/.disabled");
    }

    #[test]
    fn thumbnail_content_type_maps_formats() {
        assert_eq!(thumbnail_content_type("png"), "image/png");
        assert_eq!(thumbnail_content_type("webp"), "image/webp");
        assert_eq!(thumbnail_content_type("jpeg"), "image/jpeg");
        assert_eq!(thumbnail_content_type("jpg"), "image/jpeg");
    }
}
