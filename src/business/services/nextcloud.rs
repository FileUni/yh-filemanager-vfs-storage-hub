use std::path::Path;

pub fn derive_nextcloud_file_numeric_id(user_id: &str, path: &str) -> u64 {
    stable_hash_u64(format!("{}:{}", user_id, path).as_bytes()) & 0x7fff_ffff_ffff_ffff
}

pub fn derive_nextcloud_share_numeric_id(share_token: &str) -> u64 {
    stable_hash_u64(share_token.as_bytes()) & 0x7fff_ffff
}

pub fn derive_nextcloud_remote_id(user_id: &str, path: &str) -> String {
    format!(
        "{:08}fileuni",
        derive_nextcloud_file_numeric_id(user_id, path)
    )
}

pub fn nextcloud_guess_content_type(path: &str) -> &'static str {
    match nextcloud_extension(path).as_deref() {
        Some("md") | Some("markdown") => "text/markdown",
        Some("txt") | Some("org") | Some("note") => "text/plain",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("bmp") => "image/bmp",
        Some("heic") => "image/heic",
        Some("heif") => "image/heif",
        Some("avif") => "image/avif",
        Some("pdf") => "application/pdf",
        Some("mp4") | Some("m4v") => "video/mp4",
        Some("webm") => "video/webm",
        Some("mov") => "video/quicktime",
        Some("mkv") => "video/x-matroska",
        Some("avi") => "video/x-msvideo",
        Some("3gp") => "video/3gpp",
        Some("mpg") | Some("mpeg") => "video/mpeg",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("m4a") => "audio/mp4",
        Some("aac") => "audio/aac",
        Some("flac") => "audio/flac",
        Some("ogg") | Some("oga") => "audio/ogg",
        _ => "application/octet-stream",
    }
}

pub fn nextcloud_guess_content_type_with_charset(path: &str) -> &'static str {
    match nextcloud_guess_content_type(path) {
        "text/markdown" => "text/markdown; charset=utf-8",
        "text/plain" => "text/plain; charset=utf-8",
        other => other,
    }
}

pub fn nextcloud_is_image(path: &str) -> bool {
    matches!(
        nextcloud_extension(path).as_deref(),
        Some("jpg")
            | Some("jpeg")
            | Some("png")
            | Some("gif")
            | Some("webp")
            | Some("bmp")
            | Some("svg")
            | Some("psd")
            | Some("ai")
            | Some("heic")
            | Some("heif")
            | Some("avif")
    )
}

pub fn nextcloud_supports_generated_preview(path: &str) -> bool {
    matches!(
        nextcloud_extension(path).as_deref(),
        Some("pdf")
            | Some("psd")
            | Some("ai")
            | Some("obj")
            | Some("stl")
            | Some("gltf")
            | Some("glb")
    )
}

pub fn nextcloud_is_video(path: &str) -> bool {
    matches!(
        nextcloud_extension(path).as_deref(),
        Some("mp4")
            | Some("mov")
            | Some("mkv")
            | Some("avi")
            | Some("webm")
            | Some("m4v")
            | Some("3gp")
            | Some("mpg")
            | Some("mpeg")
    )
}

pub fn nextcloud_is_media(path: &str) -> bool {
    nextcloud_is_image(path) || nextcloud_is_video(path)
}

pub fn nextcloud_supports_preview(path: &str) -> bool {
    nextcloud_is_media(path) || nextcloud_supports_generated_preview(path)
}

pub fn nextcloud_supports_image_preview_fallback(path: &str) -> bool {
    nextcloud_is_image(path)
}

fn nextcloud_extension(path: &str) -> Option<String> {
    Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
}

fn stable_hash_u64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    if hash == 0 { 1 } else { hash }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nextcloud_content_type_covers_text_and_media() {
        assert_eq!(
            nextcloud_guess_content_type("/Notes/demo.md"),
            "text/markdown"
        );
        assert_eq!(
            nextcloud_guess_content_type("/Photos/frame.webp"),
            "image/webp"
        );
        assert_eq!(
            nextcloud_guess_content_type("/Videos/clip.mkv"),
            "video/x-matroska"
        );
    }

    #[test]
    fn nextcloud_content_type_with_charset_only_changes_text() {
        assert_eq!(
            nextcloud_guess_content_type_with_charset("/Notes/demo.note"),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            nextcloud_guess_content_type_with_charset("/Docs/file.pdf"),
            "application/pdf"
        );
    }

    #[test]
    fn nextcloud_preview_and_media_sets_stay_consistent() {
        assert!(nextcloud_is_image("/Photos/a.HEIC"));
        assert!(nextcloud_is_video("/Videos/b.mpeg"));
        assert!(nextcloud_is_media("/Videos/b.mpeg"));
        assert!(nextcloud_supports_preview("/Docs/file.pdf"));
        assert!(nextcloud_supports_preview("/Design/model.stl"));
        assert!(nextcloud_supports_preview("/Design/mockup.psd"));
        assert!(nextcloud_supports_image_preview_fallback(
            "/Photos/icon.svg"
        ));
        assert!(!nextcloud_supports_preview("/Docs/archive.zip"));
    }
}
