// Path sanitization and validation tests
// These tests verify the security checks for file paths

#[cfg(test)]
mod path_tests {
    /// Test that validates the path security check logic
    /// This replicates the logic from internal.rs validate_file_operation
    fn validate_path_security(path: &str) -> Result<String, String> {
        let normalized = if path.starts_with('/') { path.to_string() } else { format!("/{}", path) };

        // Unified security line: intercept all dangerous path patterns
        //1. (..), 2. (//), 3. (./)
        if normalized.contains("..") || normalized.contains("//") || normalized.contains("./") {
            return Err(format!("Security violation: dangerous path pattern detected in '{}'", normalized));
        }

        // Prohibit control characters
        if normalized.chars().any(|c| c.is_control()) {
            return Err("Security violation: control characters in path".to_string());
        }

        Ok(normalized)
    }

    #[test]
    fn test_valid_simple_path() {
        let result = validate_path_security("/documents/file.txt");
        assert!(matches!(result, Ok(path) if path == "/documents/file.txt"));
    }

    #[test]
    fn test_valid_path_without_leading_slash() {
        let result = validate_path_security("documents/file.txt");
        assert!(matches!(result, Ok(path) if path == "/documents/file.txt"));
    }

    #[test]
    fn test_valid_nested_path() {
        let result = validate_path_security("/a/b/c/d/e.txt");
        assert!(matches!(result, Ok(path) if path == "/a/b/c/d/e.txt"));
    }

    #[test]
    fn test_path_with_dot_in_filename() {
        // File names with dots are valid
        let result = validate_path_security("/documents/my.file.name.txt");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_with_dot_prefix() {
        // .gitignore is a valid file name
        let result = validate_path_security("/.gitignore");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_with_dot_folder() {
        // .hidden is a valid folder name
        let result = validate_path_security("/.hidden/file.txt");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_double_dot_in_filename() {
        // File names like "foo..bar" are valid
        let result = validate_path_security("/foo..bar.txt");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_traversal_parent_directory() {
        let result = validate_path_security("/documents/../etc/passwd");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.contains("Security violation"));
        }
    }
    fn test_path_traversal_parent_directory() {
        let result = validate_path_security("/documents/../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Security violation"));
    }

    #[test]
    fn test_path_traversal_at_start() {
        let result = validate_path_security("/../etc/passwd");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.contains("Security violation"));
        }
    }
    fn test_path_traversal_at_start() {
        let result = validate_path_security("/../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Security violation"));
    }

    #[test]
    fn test_path_traversal_mid_path() {
        let result = validate_path_security("/a/b/../c/d");
        assert!(result.is_err());
    }

    #[test]
    fn test_path_double_slash() {
        let result = validate_path_security("/documents//file.txt");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.contains("Security violation"));
        }
    }
    fn test_path_double_slash() {
        let result = validate_path_security("/documents//file.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Security violation"));
    }

    #[test]
    fn test_path_triple_slash() {
        let result = validate_path_security("/a///b/c");
        assert!(result.is_err());
    }

    #[test]
    fn test_path_single_dot_current_directory() {
        let result = validate_path_security("/documents/./file.txt");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.contains("Security violation"));
        }
    }
    fn test_path_single_dot_current_directory() {
        let result = validate_path_security("/documents/./file.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Security violation"));
    }

    #[test]
    fn test_path_single_dot_mid_path() {
        let result = validate_path_security("/a/./b/./c");
        assert!(result.is_err());
    }

    #[test]
    fn test_path_control_character() {
        // Null character
        let result = validate_path_security("/documents/fil\x00e.txt");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.contains("control characters"));
        }
    }
    fn test_path_control_character() {
        // Null character
        let result = validate_path_security("/documents/fil\x00e.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("control characters"));
    }

    #[test]
    fn test_path_newline_character() {
        let result = validate_path_security("/documents/file\n.txt");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.contains("control characters"));
        }
    }
    fn test_path_newline_character() {
        let result = validate_path_security("/documents/file\n.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("control characters"));
    }

    #[test]
    fn test_path_tab_character() {
        let result = validate_path_security("/documents/file\t.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_path_carriage_return() {
        let result = validate_path_security("/documents/file\r.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_path_special_vfs_prefix_temp() {
        // Temp path should be valid
        let result = validate_path_security("/.virtual/tmp/upload.tmp");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_special_vfs_prefix_thumbs() {
        // Thumbs cache path should be valid
        let result = validate_path_security("/.thumbs_cache/large/image.jpg");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_recycle_bin() {
        // Recycle bin path should be valid
        let result = validate_path_security("/.recycle_bin/trash");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_empty_after_normalization() {
        // "/" normalizes to "/"
        let result = validate_path_security("/");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_root_with_trailing_slash() {
        let result = validate_path_security("////");
        // After normalization this becomes "////" which contains //
        assert!(result.is_err());
    }

    #[test]
    fn test_path_unicode_valid() {
        // Unicode characters should be valid in paths
        let result = validate_path_security("/文档/文件.txt");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_emoji_valid() {
        // Emoji in paths should be valid
        let result = validate_path_security("/📁/file.txt");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_space_in_filename() {
        let result = validate_path_security("/my documents/file.txt");
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_special_chars_valid() {
        // Various special characters that should be valid
        let result = validate_path_security("/file-name_123.txt");
        assert!(result.is_ok());
    }
}
