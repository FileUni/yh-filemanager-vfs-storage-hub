use percent_encoding::percent_decode_str;

fn normalize_absolute_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return "/".to_string();
    }
    let replaced = trimmed.replace('\\', "/");
    let mut segments = Vec::new();
    for segment in replaced.split('/') {
        let segment = segment.trim();
        if segment.is_empty() || segment == "." {
            continue;
        }
        segments.push(segment);
    }
    if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", segments.join("/"))
    }
}

fn decode_share_path(raw: &str) -> Result<String, &'static str> {
    let normalized = raw.replace('\\', "/");
    if normalized.contains('%') {
        return percent_decode_str(&normalized)
            .decode_utf8()
            .map(|value| value.into_owned())
            .map_err(|_| "INVALID_PATH");
    }
    Ok(normalized)
}

pub fn normalize_share_relative_path(requested_path: &str) -> Result<String, &'static str> {
    let decoded = decode_share_path(requested_path.trim())?;
    let mut segments = Vec::new();
    for segment in decoded.split('/') {
        let segment = segment.trim();
        if segment.is_empty() || segment == "." {
            continue;
        }
        if segment == ".." || segment.chars().any(|ch| ch.is_control()) {
            return Err("INVALID_PATH");
        }
        segments.push(segment.to_string());
    }
    if segments.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", segments.join("/")))
    }
}

pub fn is_path_within_root(root: &str, candidate: &str) -> bool {
    let root = normalize_absolute_path(root);
    let candidate = normalize_absolute_path(candidate);
    root == "/"
        || candidate == root
        || candidate
            .strip_prefix(root.as_str())
            .is_some_and(|rest| rest.starts_with('/'))
}

pub fn resolve_share_descendant_path(
    base_path: &str,
    requested_path: &str,
    base_is_dir: bool,
) -> Result<String, &'static str> {
    let base_path = normalize_absolute_path(base_path);
    let relative_path = normalize_share_relative_path(requested_path)?;
    if relative_path == "/" {
        return Ok(base_path);
    }
    if !base_is_dir {
        return Err("NOT_A_DIRECTORY");
    }
    let candidate = if base_path == "/" {
        relative_path
    } else {
        format!(
            "{}/{}",
            base_path.trim_end_matches('/'),
            relative_path.trim_start_matches('/')
        )
    };
    if is_path_within_root(base_path.as_str(), candidate.as_str()) {
        Ok(candidate)
    } else {
        Err("INVALID_PATH")
    }
}

pub fn is_direct_share_name_allowed(name: &str) -> bool {
    let trimmed = name.trim();
    !trimmed.is_empty()
        && trimmed.chars().all(|ch| {
            ch.is_alphanumeric() || matches!(ch, ' ' | '.' | '_' | '-' | '(' | ')' | '[' | ']')
        })
}

pub fn is_direct_share_path_allowed(requested_path: &str) -> bool {
    match normalize_share_relative_path(requested_path) {
        Ok(path) if path == "/" => true,
        Ok(path) => path
            .trim_matches('/')
            .split('/')
            .all(is_direct_share_name_allowed),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_share_relative_path_rejects_parent_segments() {
        assert!(matches!(
            normalize_share_relative_path("../secret.txt"),
            Err("INVALID_PATH")
        ));
        assert!(matches!(
            normalize_share_relative_path("/%2e%2e/secret.txt"),
            Err("INVALID_PATH")
        ));
    }

    #[test]
    fn resolve_share_descendant_path_keeps_root_boundary() {
        assert_eq!(
            resolve_share_descendant_path("/Docs", "nested/file.txt", true),
            Ok("/Docs/nested/file.txt".to_string())
        );
        assert!(matches!(
            resolve_share_descendant_path("/Docs", "../file.txt", true),
            Err("INVALID_PATH")
        ));
    }

    #[test]
    fn path_with_common_prefix_is_not_treated_as_descendant() {
        assert!(!is_path_within_root("/data", "/database/secret.txt"));
        assert!(is_path_within_root("/data", "/data/file.txt"));
    }

    #[test]
    fn direct_share_name_allowlist_blocks_special_chars() {
        assert!(is_direct_share_name_allowed("report 01.txt"));
        assert!(is_direct_share_name_allowed("中文文档.md"));
        assert!(!is_direct_share_name_allowed("evil<script>.txt"));
        assert!(!is_direct_share_name_allowed("bad#name.txt"));
    }

    #[test]
    fn direct_share_path_allowlist_checks_each_segment() {
        assert!(is_direct_share_path_allowed("/safe/中文文档.txt"));
        assert!(!is_direct_share_path_allowed("/safe/bad#name.txt"));
    }
}
