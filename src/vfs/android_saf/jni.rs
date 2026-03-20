#![cfg(target_os = "android")]

use super::ANDROID_SAF_MIME_DIR;
use jni::objects::{JObject, JString, JValue};
use jni::sys::{jint, jobject};
use opendal::raw::{Timestamp, oio};
use opendal::{EntryMode, Error, ErrorKind, Metadata, Result};
use std::io::Write;

const MIME_OCTET_STREAM: &str = "application/octet-stream";

fn opendal_error_from_jni(
    env: &mut jni::JNIEnv<'_>,
    action: &str,
    err: jni::errors::Error,
) -> Error {
    let mut kind = ErrorKind::Unexpected;
    let mut message = format!("android_saf jni error during {}", action);

    if matches!(err, jni::errors::Error::JavaException) {
        if let Some(ex) = take_java_exception_string(env) {
            // Rough classification.
            if ex.contains("SecurityException") || ex.to_ascii_lowercase().contains("permission") {
                kind = ErrorKind::PermissionDenied;
            } else if ex.contains("FileNotFound") {
                kind = ErrorKind::NotFound;
            } else if ex.contains("UnsupportedOperationException") {
                kind = ErrorKind::Unsupported;
            }
            message.push_str(": ");
            message.push_str(&ex);
        }
    }

    Error::new(kind, message).set_source(err)
}

fn take_java_exception_string(env: &mut jni::JNIEnv<'_>) -> Option<String> {
    let has = env.exception_check().ok()?;
    if !has {
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
    let s: JString = JString::from(obj);
    env.get_string(&s)
        .ok()
        .map(|v| v.to_string_lossy().into_owned())
}

fn java_vm() -> Result<jni::JavaVM> {
    let ctx = ndk_context::android_context();
    // SAFETY: Provided by Android runtime.
    unsafe { jni::JavaVM::from_raw(ctx.vm().cast()) }
        .map_err(|e| Error::new(ErrorKind::Unexpected, "Failed to obtain JavaVM").set_source(e))
}

fn app_context_obj<'a>(_env: &mut jni::JNIEnv<'a>) -> Result<JObject<'a>> {
    let ctx = ndk_context::android_context();
    let raw = ctx.context() as jobject;
    // SAFETY: The context object is a valid JNI object reference.
    Ok(unsafe { JObject::from_raw(raw) })
}

fn get_content_resolver<'a>(
    env: &mut jni::JNIEnv<'a>,
    context: &JObject<'a>,
) -> Result<JObject<'a>> {
    env.call_method(
        context,
        "getContentResolver",
        "()Landroid/content/ContentResolver;",
        &[],
    )
    .map_err(|e| opendal_error_from_jni(env, "getContentResolver", e))?
    .l()
    .map_err(|e| opendal_error_from_jni(env, "getContentResolver", e))
}

fn parse_uri<'a>(env: &mut jni::JNIEnv<'a>, uri: &str) -> Result<JObject<'a>> {
    let uri_class = env
        .find_class("android/net/Uri")
        .map_err(|e| opendal_error_from_jni(env, "find Uri", e))?;
    let juri = env
        .new_string(uri)
        .map_err(|e| opendal_error_from_jni(env, "new_string(uri)", e))?;
    env.call_static_method(
        uri_class,
        "parse",
        "(Ljava/lang/String;)Landroid/net/Uri;",
        &[JValue::Object(&juri)],
    )
    .map_err(|e| opendal_error_from_jni(env, "Uri.parse", e))?
    .l()
    .map_err(|e| opendal_error_from_jni(env, "Uri.parse", e))
}

fn documents_contract_class<'a>(env: &mut jni::JNIEnv<'a>) -> Result<jni::objects::JClass<'a>> {
    env.find_class("android/provider/DocumentsContract")
        .map_err(|e| opendal_error_from_jni(env, "find DocumentsContract", e))
}

fn documents_contract_document_class<'a>(
    env: &mut jni::JNIEnv<'a>,
) -> Result<jni::objects::JClass<'a>> {
    env.find_class("android/provider/DocumentsContract$Document")
        .map_err(|e| opendal_error_from_jni(env, "find DocumentsContract$Document", e))
}

fn get_doc_column<'a>(env: &mut jni::JNIEnv<'a>, field: &str) -> Result<JObject<'a>> {
    let doc_class = documents_contract_document_class(env)?;
    env.get_static_field(doc_class, field, "Ljava/lang/String;")
        .map_err(|e| opendal_error_from_jni(env, "get Document column", e))?
        .l()
        .map_err(|e| opendal_error_from_jni(env, "get Document column", e))
}

fn build_document_uri_using_tree<'a>(
    env: &mut jni::JNIEnv<'a>,
    tree_uri: &JObject<'a>,
    doc_id: &str,
) -> Result<JObject<'a>> {
    let dc = documents_contract_class(env)?;
    let jdoc = env
        .new_string(doc_id)
        .map_err(|e| opendal_error_from_jni(env, "new_string(doc_id)", e))?;
    env.call_static_method(
        dc,
        "buildDocumentUriUsingTree",
        "(Landroid/net/Uri;Ljava/lang/String;)Landroid/net/Uri;",
        &[JValue::Object(tree_uri), JValue::Object(&jdoc)],
    )
    .map_err(|e| opendal_error_from_jni(env, "buildDocumentUriUsingTree", e))?
    .l()
    .map_err(|e| opendal_error_from_jni(env, "buildDocumentUriUsingTree", e))
}

fn build_child_documents_uri_using_tree<'a>(
    env: &mut jni::JNIEnv<'a>,
    tree_uri: &JObject<'a>,
    parent_doc_id: &str,
) -> Result<JObject<'a>> {
    let dc = documents_contract_class(env)?;
    let jdoc = env
        .new_string(parent_doc_id)
        .map_err(|e| opendal_error_from_jni(env, "new_string(parent_doc_id)", e))?;
    env.call_static_method(
        dc,
        "buildChildDocumentsUriUsingTree",
        "(Landroid/net/Uri;Ljava/lang/String;)Landroid/net/Uri;",
        &[JValue::Object(tree_uri), JValue::Object(&jdoc)],
    )
    .map_err(|e| opendal_error_from_jni(env, "buildChildDocumentsUriUsingTree", e))?
    .l()
    .map_err(|e| opendal_error_from_jni(env, "buildChildDocumentsUriUsingTree", e))
}

fn get_tree_document_id_inner(
    env: &mut jni::JNIEnv<'_>,
    tree_uri_obj: &JObject<'_>,
) -> Result<String> {
    let dc = documents_contract_class(env)?;
    let obj = env
        .call_static_method(
            dc,
            "getTreeDocumentId",
            "(Landroid/net/Uri;)Ljava/lang/String;",
            &[JValue::Object(tree_uri_obj)],
        )
        .map_err(|e| opendal_error_from_jni(env, "getTreeDocumentId", e))?
        .l()
        .map_err(|e| opendal_error_from_jni(env, "getTreeDocumentId", e))?;
    if obj.is_null() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "DocumentsContract.getTreeDocumentId returned null",
        ));
    }
    let s: JString = JString::from(obj);
    env.get_string(&s)
        .map_err(|e| opendal_error_from_jni(env, "getTreeDocumentId/get_string", e))
        .map(|v| v.to_string_lossy().into_owned())
}

fn get_document_id_from_uri(env: &mut jni::JNIEnv<'_>, uri_obj: &JObject<'_>) -> Result<String> {
    let dc = documents_contract_class(env)?;
    let obj = env
        .call_static_method(
            dc,
            "getDocumentId",
            "(Landroid/net/Uri;)Ljava/lang/String;",
            &[JValue::Object(uri_obj)],
        )
        .map_err(|e| opendal_error_from_jni(env, "getDocumentId", e))?
        .l()
        .map_err(|e| opendal_error_from_jni(env, "getDocumentId", e))?;
    if obj.is_null() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "DocumentsContract.getDocumentId returned null",
        ));
    }
    let s: JString = JString::from(obj);
    env.get_string(&s)
        .map_err(|e| opendal_error_from_jni(env, "getDocumentId/get_string", e))
        .map(|v| v.to_string_lossy().into_owned())
}

fn cursor_close(env: &mut jni::JNIEnv<'_>, cursor: &JObject<'_>) {
    let _ = env.call_method(cursor, "close", "()V", &[]);
}

fn query_cursor<'a>(
    env: &mut jni::JNIEnv<'a>,
    resolver: &JObject<'a>,
    uri: &JObject<'a>,
    projection: &[&JObject<'a>],
) -> Result<Option<JObject<'a>>> {
    let array = env
        .new_object_array(
            projection.len() as jint,
            "java/lang/String",
            JObject::null(),
        )
        .map_err(|e| opendal_error_from_jni(env, "new_object_array", e))?;
    for (idx, col) in projection.iter().enumerate() {
        env.set_object_array_element(&array, idx as jint, *col)
            .map_err(|e| opendal_error_from_jni(env, "set_object_array_element", e))?;
    }

    let cursor = env
        .call_method(
            resolver,
            "query",
            "(Landroid/net/Uri;[Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;)Landroid/database/Cursor;",
            &[
                JValue::Object(uri),
                JValue::Object(&array),
                JValue::Object(&JObject::null()),
                JValue::Object(&JObject::null()),
                JValue::Object(&JObject::null()),
            ],
        )
        .map_err(|e| opendal_error_from_jni(env, "ContentResolver.query", e))?
        .l()
        .map_err(|e| opendal_error_from_jni(env, "ContentResolver.query", e))?;
    if cursor.is_null() {
        return Ok(None);
    }
    Ok(Some(cursor))
}

fn cursor_get_string(env: &mut jni::JNIEnv<'_>, cursor: &JObject<'_>, idx: jint) -> Result<String> {
    let obj = env
        .call_method(
            cursor,
            "getString",
            "(I)Ljava/lang/String;",
            &[JValue::Int(idx)],
        )
        .map_err(|e| opendal_error_from_jni(env, "Cursor.getString", e))?
        .l()
        .map_err(|e| opendal_error_from_jni(env, "Cursor.getString", e))?;
    if obj.is_null() {
        return Ok(String::new());
    }
    let s: JString = JString::from(obj);
    env.get_string(&s)
        .map_err(|e| opendal_error_from_jni(env, "Cursor.getString/get_string", e))
        .map(|v| v.to_string_lossy().into_owned())
}

fn cursor_get_long(env: &mut jni::JNIEnv<'_>, cursor: &JObject<'_>, idx: jint) -> Result<i64> {
    env.call_method(cursor, "getLong", "(I)J", &[JValue::Int(idx)])
        .map_err(|e| opendal_error_from_jni(env, "Cursor.getLong", e))?
        .j()
        .map_err(|e| opendal_error_from_jni(env, "Cursor.getLong", e))
}

fn cursor_is_null(env: &mut jni::JNIEnv<'_>, cursor: &JObject<'_>, idx: jint) -> Result<bool> {
    let v = env
        .call_method(cursor, "isNull", "(I)Z", &[JValue::Int(idx)])
        .map_err(|e| opendal_error_from_jni(env, "Cursor.isNull", e))?
        .z()
        .map_err(|e| opendal_error_from_jni(env, "Cursor.isNull", e))?;
    Ok(v)
}

fn cursor_move_to_first(env: &mut jni::JNIEnv<'_>, cursor: &JObject<'_>) -> Result<bool> {
    let v = env
        .call_method(cursor, "moveToFirst", "()Z", &[])
        .map_err(|e| opendal_error_from_jni(env, "Cursor.moveToFirst", e))?
        .z()
        .map_err(|e| opendal_error_from_jni(env, "Cursor.moveToFirst", e))?;
    Ok(v)
}

fn cursor_move_to_next(env: &mut jni::JNIEnv<'_>, cursor: &JObject<'_>) -> Result<bool> {
    let v = env
        .call_method(cursor, "moveToNext", "()Z", &[])
        .map_err(|e| opendal_error_from_jni(env, "Cursor.moveToNext", e))?
        .z()
        .map_err(|e| opendal_error_from_jni(env, "Cursor.moveToNext", e))?;
    Ok(v)
}

fn cursor_col_index_or_throw(
    env: &mut jni::JNIEnv<'_>,
    cursor: &JObject<'_>,
    col: &JObject<'_>,
) -> Result<jint> {
    env.call_method(
        cursor,
        "getColumnIndexOrThrow",
        "(Ljava/lang/String;)I",
        &[JValue::Object(col)],
    )
    .map_err(|e| opendal_error_from_jni(env, "Cursor.getColumnIndexOrThrow", e))?
    .i()
    .map_err(|e| opendal_error_from_jni(env, "Cursor.getColumnIndexOrThrow", e))
}

fn normalize_path(path: &str) -> (String, bool) {
    let trimmed = path.trim();
    let is_dir = trimmed.ends_with('/');
    let p = trimmed.trim_start_matches('/');
    let p = if p == "/" { "" } else { p };
    let p = p.trim_end_matches('/');
    (p.to_string(), is_dir)
}

fn split_parent_child(path: &str) -> (String, String) {
    let (norm, _is_dir) = normalize_path(path);
    let mut it = norm.rsplitn(2, '/');
    let name = it.next().unwrap_or("").to_string();
    let parent = it.next().unwrap_or("").to_string();
    (parent, name)
}

fn resolve_existing_doc_id<'a>(
    env: &mut jni::JNIEnv<'a>,
    resolver: &JObject<'a>,
    tree_uri_obj: &JObject<'a>,
    root_doc_id: &str,
    path: &str,
) -> Result<String> {
    let (norm, _is_dir) = normalize_path(path);
    if norm.is_empty() {
        return Ok(root_doc_id.to_string());
    }
    let doc_col_id = get_doc_column(env, "COLUMN_DOCUMENT_ID")?;
    let doc_col_name = get_doc_column(env, "COLUMN_DISPLAY_NAME")?;

    let mut current = root_doc_id.to_string();
    for part in norm.split('/').filter(|p| !p.is_empty()) {
        let child_uri = build_child_documents_uri_using_tree(env, tree_uri_obj, &current)?;
        let cursor = query_cursor(env, resolver, &child_uri, &[&doc_col_id, &doc_col_name])?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ContentResolver.query returned null cursor",
                )
            })?;

        let idx_id = cursor_col_index_or_throw(env, &cursor, &doc_col_id)?;
        let idx_name = cursor_col_index_or_throw(env, &cursor, &doc_col_name)?;

        let mut found: Option<String> = None;
        if cursor_move_to_first(env, &cursor)? {
            loop {
                let name = cursor_get_string(env, &cursor, idx_name)?;
                if name == part {
                    let id = cursor_get_string(env, &cursor, idx_id)?;
                    found = Some(id);
                    break;
                }
                if !cursor_move_to_next(env, &cursor)? {
                    break;
                }
            }
        }
        cursor_close(env, &cursor);

        let Some(next) = found else {
            return Err(Error::new(ErrorKind::NotFound, "Path component not found")
                .with_context("component", part.to_string())
                .with_context("path", path.to_string()));
        };
        current = next;
    }
    Ok(current)
}

fn resolve_dir_doc_id_create<'a>(
    env: &mut jni::JNIEnv<'a>,
    resolver: &JObject<'a>,
    tree_uri_obj: &JObject<'a>,
    root_doc_id: &str,
    dir_path: &str,
    create_intermediate: bool,
) -> Result<String> {
    let (norm, _is_dir) = normalize_path(dir_path);
    if norm.is_empty() {
        return Ok(root_doc_id.to_string());
    }

    let doc_col_id = get_doc_column(env, "COLUMN_DOCUMENT_ID")?;
    let doc_col_name = get_doc_column(env, "COLUMN_DISPLAY_NAME")?;
    let doc_col_mime = get_doc_column(env, "COLUMN_MIME_TYPE")?;

    let mut current = root_doc_id.to_string();
    for part in norm.split('/').filter(|p| !p.is_empty()) {
        let child_uri = build_child_documents_uri_using_tree(env, tree_uri_obj, &current)?;
        let cursor = query_cursor(
            env,
            resolver,
            &child_uri,
            &[&doc_col_id, &doc_col_name, &doc_col_mime],
        )?
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "ContentResolver.query returned null cursor",
            )
        })?;

        let idx_id = cursor_col_index_or_throw(env, &cursor, &doc_col_id)?;
        let idx_name = cursor_col_index_or_throw(env, &cursor, &doc_col_name)?;
        let idx_mime = cursor_col_index_or_throw(env, &cursor, &doc_col_mime)?;

        let mut found: Option<(String, String)> = None;
        if cursor_move_to_first(env, &cursor)? {
            loop {
                let name = cursor_get_string(env, &cursor, idx_name)?;
                if name == part {
                    let id = cursor_get_string(env, &cursor, idx_id)?;
                    let mime = cursor_get_string(env, &cursor, idx_mime)?;
                    found = Some((id, mime));
                    break;
                }
                if !cursor_move_to_next(env, &cursor)? {
                    break;
                }
            }
        }
        cursor_close(env, &cursor);

        if let Some((child_id, child_mime)) = found {
            if child_mime != ANDROID_SAF_MIME_DIR {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    "Path component is not a directory",
                )
                .with_context("component", part.to_string())
                .with_context("path", dir_path.to_string()));
            }
            current = child_id;
            continue;
        }

        if !create_intermediate {
            return Err(Error::new(ErrorKind::NotFound, "Directory not found")
                .with_context("component", part.to_string())
                .with_context("path", dir_path.to_string()));
        }

        // Create missing directory.
        let parent_uri = build_document_uri_using_tree(env, tree_uri_obj, &current)?;
        let dc = documents_contract_class(env)?;
        let jmime = env
            .new_string(ANDROID_SAF_MIME_DIR)
            .map_err(|e| opendal_error_from_jni(env, "new_string(mime_dir)", e))?;
        let jname = env
            .new_string(part)
            .map_err(|e| opendal_error_from_jni(env, "new_string(dir_name)", e))?;
        let created_uri = env
            .call_static_method(
                dc,
                "createDocument",
                "(Landroid/content/ContentResolver;Landroid/net/Uri;Ljava/lang/String;Ljava/lang/String;)Landroid/net/Uri;",
                &[
                    JValue::Object(resolver),
                    JValue::Object(&parent_uri),
                    JValue::Object(&jmime),
                    JValue::Object(&jname),
                ],
            )
            .map_err(|e| opendal_error_from_jni(env, "createDocument(dir)", e))?
            .l()
            .map_err(|e| opendal_error_from_jni(env, "createDocument(dir)", e))?;
        if created_uri.is_null() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "DocumentsContract.createDocument returned null",
            ));
        }
        let new_id = get_document_id_from_uri(env, &created_uri)?;
        current = new_id;
    }
    Ok(current)
}

fn stat_by_doc_id<'a>(
    env: &mut jni::JNIEnv<'a>,
    resolver: &JObject<'a>,
    tree_uri_obj: &JObject<'a>,
    doc_id: &str,
) -> Result<Metadata> {
    let doc_col_mime = get_doc_column(env, "COLUMN_MIME_TYPE")?;
    let doc_col_size = get_doc_column(env, "COLUMN_SIZE")?;
    let doc_col_last = get_doc_column(env, "COLUMN_LAST_MODIFIED")?;

    let doc_uri = build_document_uri_using_tree(env, tree_uri_obj, doc_id)?;
    let cursor = query_cursor(
        env,
        resolver,
        &doc_uri,
        &[&doc_col_mime, &doc_col_size, &doc_col_last],
    )?
    .ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            "ContentResolver.query returned null cursor",
        )
    })?;

    if !cursor_move_to_first(env, &cursor)? {
        cursor_close(env, &cursor);
        return Err(Error::new(ErrorKind::NotFound, "Document not found"));
    }

    let idx_mime = cursor_col_index_or_throw(env, &cursor, &doc_col_mime)?;
    let idx_size = cursor_col_index_or_throw(env, &cursor, &doc_col_size)?;
    let idx_last = cursor_col_index_or_throw(env, &cursor, &doc_col_last)?;

    let mime = cursor_get_string(env, &cursor, idx_mime)?;
    let is_dir = mime == ANDROID_SAF_MIME_DIR;
    let size = if cursor_is_null(env, &cursor, idx_size)? {
        0
    } else {
        cursor_get_long(env, &cursor, idx_size)?
    };
    let last_modified = if cursor_is_null(env, &cursor, idx_last)? {
        None
    } else {
        let ms = cursor_get_long(env, &cursor, idx_last)?;
        if ms <= 0 {
            None
        } else {
            Some(Timestamp::from_millisecond(ms)?)
        }
    };
    cursor_close(env, &cursor);

    let mut meta = Metadata::new(if is_dir {
        EntryMode::DIR
    } else {
        EntryMode::FILE
    })
    .with_content_length(size as u64);
    if let Some(ts) = last_modified {
        meta = meta.with_last_modified(ts);
    }
    Ok(meta)
}

pub fn take_persistable_permission(tree_uri: &str) -> Result<()> {
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let context_obj = app_context_obj(&mut env)?;
    let resolver = get_content_resolver(&mut env, &context_obj)?;
    let uri_obj = parse_uri(&mut env, tree_uri)?;

    let intent_class = env
        .find_class("android/content/Intent")
        .map_err(|e| opendal_error_from_jni(&mut env, "find Intent", e))?;
    let read_flag = env
        .get_static_field(&intent_class, "FLAG_GRANT_READ_URI_PERMISSION", "I")
        .map_err(|e| opendal_error_from_jni(&mut env, "Intent.FLAG_GRANT_READ_URI_PERMISSION", e))?
        .i()
        .map_err(|e| {
            opendal_error_from_jni(&mut env, "Intent.FLAG_GRANT_READ_URI_PERMISSION", e)
        })?;
    let write_flag = env
        .get_static_field(&intent_class, "FLAG_GRANT_WRITE_URI_PERMISSION", "I")
        .map_err(|e| opendal_error_from_jni(&mut env, "Intent.FLAG_GRANT_WRITE_URI_PERMISSION", e))?
        .i()
        .map_err(|e| {
            opendal_error_from_jni(&mut env, "Intent.FLAG_GRANT_WRITE_URI_PERMISSION", e)
        })?;

    let flags = read_flag | write_flag;
    env.call_method(
        &resolver,
        "takePersistableUriPermission",
        "(Landroid/net/Uri;I)V",
        &[JValue::Object(&uri_obj), JValue::Int(flags)],
    )
    .map_err(|e| opendal_error_from_jni(&mut env, "takePersistableUriPermission", e))?;
    Ok(())
}

pub fn get_tree_document_id(tree_uri: &str) -> Result<String> {
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let uri_obj = parse_uri(&mut env, tree_uri)?;
    get_tree_document_id_inner(&mut env, &uri_obj)
}

pub fn stat(tree_uri: &str, root_doc_id: &str, path: &str) -> Result<Metadata> {
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let context_obj = app_context_obj(&mut env)?;
    let resolver = get_content_resolver(&mut env, &context_obj)?;
    let tree_uri_obj = parse_uri(&mut env, tree_uri)?;
    let doc_id = resolve_existing_doc_id(&mut env, &resolver, &tree_uri_obj, root_doc_id, path)?;
    stat_by_doc_id(&mut env, &resolver, &tree_uri_obj, &doc_id)
}

pub fn ensure_dir(
    tree_uri: &str,
    root_doc_id: &str,
    path: &str,
    create_intermediate: bool,
) -> Result<()> {
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let context_obj = app_context_obj(&mut env)?;
    let resolver = get_content_resolver(&mut env, &context_obj)?;
    let tree_uri_obj = parse_uri(&mut env, tree_uri)?;
    let (norm, _is_dir) = normalize_path(path);
    if norm.is_empty() {
        return Ok(());
    }
    let _ = resolve_dir_doc_id_create(
        &mut env,
        &resolver,
        &tree_uri_obj,
        root_doc_id,
        &format!("{}/", norm),
        create_intermediate,
    )?;
    Ok(())
}

pub fn list(tree_uri: &str, root_doc_id: &str, path: &str) -> Result<Vec<oio::Entry>> {
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let context_obj = app_context_obj(&mut env)?;
    let resolver = get_content_resolver(&mut env, &context_obj)?;
    let tree_uri_obj = parse_uri(&mut env, tree_uri)?;

    let (norm, _is_dir) = normalize_path(path);
    let dir_id = if norm.is_empty() {
        root_doc_id.to_string()
    } else {
        resolve_existing_doc_id(
            &mut env,
            &resolver,
            &tree_uri_obj,
            root_doc_id,
            &format!("{}/", norm),
        )?
    };
    let child_uri = build_child_documents_uri_using_tree(&mut env, &tree_uri_obj, &dir_id)?;

    let doc_col_name = get_doc_column(&mut env, "COLUMN_DISPLAY_NAME")?;
    let doc_col_mime = get_doc_column(&mut env, "COLUMN_MIME_TYPE")?;
    let doc_col_size = get_doc_column(&mut env, "COLUMN_SIZE")?;
    let doc_col_last = get_doc_column(&mut env, "COLUMN_LAST_MODIFIED")?;

    let cursor = match query_cursor(
        &mut env,
        &resolver,
        &child_uri,
        &[&doc_col_name, &doc_col_mime, &doc_col_size, &doc_col_last],
    )? {
        Some(c) => c,
        None => return Ok(Vec::new()),
    };

    let idx_name = cursor_col_index_or_throw(&mut env, &cursor, &doc_col_name)?;
    let idx_mime = cursor_col_index_or_throw(&mut env, &cursor, &doc_col_mime)?;
    let idx_size = cursor_col_index_or_throw(&mut env, &cursor, &doc_col_size)?;
    let idx_last = cursor_col_index_or_throw(&mut env, &cursor, &doc_col_last)?;

    let mut out: Vec<oio::Entry> = Vec::new();

    let parent_prefix = if norm.is_empty() {
        "".to_string()
    } else {
        format!("{}/", norm.trim_end_matches('/'))
    };

    if cursor_move_to_first(&mut env, &cursor)? {
        loop {
            let name = cursor_get_string(&mut env, &cursor, idx_name)?;
            let mime = cursor_get_string(&mut env, &cursor, idx_mime)?;
            let is_dir = mime == ANDROID_SAF_MIME_DIR;
            let size = if cursor_is_null(&mut env, &cursor, idx_size)? {
                0
            } else {
                cursor_get_long(&mut env, &cursor, idx_size)?
            };
            let last_modified = if cursor_is_null(&mut env, &cursor, idx_last)? {
                None
            } else {
                let ms = cursor_get_long(&mut env, &cursor, idx_last)?;
                if ms <= 0 {
                    None
                } else {
                    Some(Timestamp::from_millisecond(ms)?)
                }
            };

            let mut meta = Metadata::new(if is_dir {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            })
            .with_content_length(size as u64);
            if let Some(ts) = last_modified {
                meta = meta.with_last_modified(ts);
            }

            let mut entry_path = format!("{}{}", parent_prefix, name);
            if is_dir {
                entry_path.push('/');
            }
            out.push(oio::Entry::new(&entry_path, meta));

            if !cursor_move_to_next(&mut env, &cursor)? {
                break;
            }
        }
    }

    cursor_close(&mut env, &cursor);
    Ok(out)
}

pub fn open_read_fd(tree_uri: &str, root_doc_id: &str, path: &str) -> Result<i32> {
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let context_obj = app_context_obj(&mut env)?;
    let resolver = get_content_resolver(&mut env, &context_obj)?;
    let tree_uri_obj = parse_uri(&mut env, tree_uri)?;
    let doc_id = resolve_existing_doc_id(&mut env, &resolver, &tree_uri_obj, root_doc_id, path)?;
    let doc_uri = build_document_uri_using_tree(&mut env, &tree_uri_obj, &doc_id)?;

    let mode = env
        .new_string("r")
        .map_err(|e| opendal_error_from_jni(&mut env, "new_string(mode)", e))?;
    let pfd = env
        .call_method(
            &resolver,
            "openFileDescriptor",
            "(Landroid/net/Uri;Ljava/lang/String;)Landroid/os/ParcelFileDescriptor;",
            &[JValue::Object(&doc_uri), JValue::Object(&mode)],
        )
        .map_err(|e| opendal_error_from_jni(&mut env, "openFileDescriptor(r)", e))?
        .l()
        .map_err(|e| opendal_error_from_jni(&mut env, "openFileDescriptor(r)", e))?;
    if pfd.is_null() {
        return Err(Error::new(
            ErrorKind::NotFound,
            "openFileDescriptor returned null",
        ));
    }
    let fd = env
        .call_method(&pfd, "detachFd", "()I", &[])
        .map_err(|e| opendal_error_from_jni(&mut env, "ParcelFileDescriptor.detachFd", e))?
        .i()
        .map_err(|e| opendal_error_from_jni(&mut env, "ParcelFileDescriptor.detachFd", e))?;
    Ok(fd)
}

pub fn open_write_fd(
    tree_uri: &str,
    root_doc_id: &str,
    path: &str,
    if_not_exists: bool,
) -> Result<i32> {
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let context_obj = app_context_obj(&mut env)?;
    let resolver = get_content_resolver(&mut env, &context_obj)?;
    let tree_uri_obj = parse_uri(&mut env, tree_uri)?;

    let (parent, name) = split_parent_child(path);
    if name.is_empty() {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            "Invalid path for write",
        ));
    }

    let parent_id = resolve_dir_doc_id_create(
        &mut env,
        &resolver,
        &tree_uri_obj,
        root_doc_id,
        &format!("{}/", parent),
        true,
    )?;

    // Check existing.
    let existing = resolve_existing_doc_id(&mut env, &resolver, &tree_uri_obj, &parent_id, &name);

    let file_doc_id = match existing {
        Ok(id) => {
            if if_not_exists {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "file already exists (if_not_exists)",
                ));
            }
            id
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {
            // Create file.
            let parent_uri = build_document_uri_using_tree(&mut env, &tree_uri_obj, &parent_id)?;
            let dc = documents_contract_class(&mut env)?;
            let jmime = env
                .new_string(MIME_OCTET_STREAM)
                .map_err(|e| opendal_error_from_jni(&mut env, "new_string(mime)", e))?;
            let jname = env
                .new_string(&name)
                .map_err(|e| opendal_error_from_jni(&mut env, "new_string(file_name)", e))?;
            let created_uri = env
                .call_static_method(
                    dc,
                    "createDocument",
                    "(Landroid/content/ContentResolver;Landroid/net/Uri;Ljava/lang/String;Ljava/lang/String;)Landroid/net/Uri;",
                    &[
                        JValue::Object(&resolver),
                        JValue::Object(&parent_uri),
                        JValue::Object(&jmime),
                        JValue::Object(&jname),
                    ],
                )
                .map_err(|e| opendal_error_from_jni(&mut env, "createDocument(file)", e))?
                .l()
                .map_err(|e| opendal_error_from_jni(&mut env, "createDocument(file)", e))?;
            if created_uri.is_null() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "DocumentsContract.createDocument returned null",
                ));
            }
            get_document_id_from_uri(&mut env, &created_uri)?
        }
        Err(other) => return Err(other),
    };

    let doc_uri = build_document_uri_using_tree(&mut env, &tree_uri_obj, &file_doc_id)?;
    let mode = env
        .new_string("w")
        .map_err(|e| opendal_error_from_jni(&mut env, "new_string(mode)", e))?;
    let pfd = env
        .call_method(
            &resolver,
            "openFileDescriptor",
            "(Landroid/net/Uri;Ljava/lang/String;)Landroid/os/ParcelFileDescriptor;",
            &[JValue::Object(&doc_uri), JValue::Object(&mode)],
        )
        .map_err(|e| opendal_error_from_jni(&mut env, "openFileDescriptor(w)", e))?
        .l()
        .map_err(|e| opendal_error_from_jni(&mut env, "openFileDescriptor(w)", e))?;
    if pfd.is_null() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "openFileDescriptor returned null",
        ));
    }
    let fd = env
        .call_method(&pfd, "detachFd", "()I", &[])
        .map_err(|e| opendal_error_from_jni(&mut env, "ParcelFileDescriptor.detachFd", e))?
        .i()
        .map_err(|e| opendal_error_from_jni(&mut env, "ParcelFileDescriptor.detachFd", e))?;
    Ok(fd)
}

pub fn delete(tree_uri: &str, root_doc_id: &str, path: &str) -> Result<()> {
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let context_obj = app_context_obj(&mut env)?;
    let resolver = get_content_resolver(&mut env, &context_obj)?;
    let tree_uri_obj = parse_uri(&mut env, tree_uri)?;

    let doc_id =
        match resolve_existing_doc_id(&mut env, &resolver, &tree_uri_obj, root_doc_id, path) {
            Ok(id) => id,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };

    if doc_id == root_doc_id {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "Refusing to delete SAF tree root",
        ));
    }

    let doc_uri = build_document_uri_using_tree(&mut env, &tree_uri_obj, &doc_id)?;
    let dc = documents_contract_class(&mut env)?;
    let ok = env
        .call_static_method(
            dc,
            "deleteDocument",
            "(Landroid/content/ContentResolver;Landroid/net/Uri;)Z",
            &[JValue::Object(&resolver), JValue::Object(&doc_uri)],
        )
        .map_err(|e| opendal_error_from_jni(&mut env, "deleteDocument", e))?
        .z()
        .map_err(|e| opendal_error_from_jni(&mut env, "deleteDocument", e))?;
    if ok {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::Unexpected,
            "DocumentsContract.deleteDocument returned false",
        ))
    }
}

pub fn rename(tree_uri: &str, root_doc_id: &str, from: &str, to: &str) -> Result<()> {
    // Best effort: try move/rename via DocumentsContract. If it fails, fall back to copy+delete.
    let vm = java_vm()?;
    let mut env = vm.attach_current_thread().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to attach JNI thread").set_source(e)
    })?;
    let context_obj = app_context_obj(&mut env)?;
    let resolver = get_content_resolver(&mut env, &context_obj)?;
    let tree_uri_obj = parse_uri(&mut env, tree_uri)?;

    let (from_parent, from_name) = split_parent_child(from);
    let (to_parent, to_name) = split_parent_child(to);
    if from_name.is_empty() || to_name.is_empty() {
        return Err(Error::new(ErrorKind::ConfigInvalid, "Invalid rename path"));
    }

    let from_parent_id = resolve_dir_doc_id_create(
        &mut env,
        &resolver,
        &tree_uri_obj,
        root_doc_id,
        &format!("{}/", from_parent),
        false,
    )?;
    let from_doc_id = resolve_existing_doc_id(
        &mut env,
        &resolver,
        &tree_uri_obj,
        &from_parent_id,
        &from_name,
    )?;
    let from_doc_uri = build_document_uri_using_tree(&mut env, &tree_uri_obj, &from_doc_id)?;
    let from_parent_uri = build_document_uri_using_tree(&mut env, &tree_uri_obj, &from_parent_id)?;

    let to_parent_id = resolve_dir_doc_id_create(
        &mut env,
        &resolver,
        &tree_uri_obj,
        root_doc_id,
        &format!("{}/", to_parent),
        true,
    )?;
    let to_parent_uri = build_document_uri_using_tree(&mut env, &tree_uri_obj, &to_parent_id)?;

    // Overwrite if destination exists.
    if let Ok(dest_id) =
        resolve_existing_doc_id(&mut env, &resolver, &tree_uri_obj, &to_parent_id, &to_name)
    {
        let dest_uri = build_document_uri_using_tree(&mut env, &tree_uri_obj, &dest_id)?;
        let dc = documents_contract_class(&mut env)?;
        let _ = env.call_static_method(
            dc,
            "deleteDocument",
            "(Landroid/content/ContentResolver;Landroid/net/Uri;)Z",
            &[JValue::Object(&resolver), JValue::Object(&dest_uri)],
        );
        let _ = take_java_exception_string(&mut env);
    }

    let dc = documents_contract_class(&mut env)?;
    let mut current_uri = from_doc_uri;

    if from_parent_id != to_parent_id {
        let moved = env
            .call_static_method(
                &dc,
                "moveDocument",
                "(Landroid/content/ContentResolver;Landroid/net/Uri;Landroid/net/Uri;Landroid/net/Uri;)Landroid/net/Uri;",
                &[
                    JValue::Object(&resolver),
                    JValue::Object(&current_uri),
                    JValue::Object(&from_parent_uri),
                    JValue::Object(&to_parent_uri),
                ],
            )
            .map_err(|e| opendal_error_from_jni(&mut env, "moveDocument", e));
        match moved {
            Ok(v) => {
                let uri = v
                    .l()
                    .map_err(|e| opendal_error_from_jni(&mut env, "moveDocument", e))?;
                if !uri.is_null() {
                    current_uri = uri;
                }
            }
            Err(_) => {
                // Clear Java exception if any and fall back.
                let _ = take_java_exception_string(&mut env);
                drop(env);
                return copy(tree_uri, root_doc_id, from, to)
                    .and_then(|_| delete(tree_uri, root_doc_id, from));
            }
        }
    }

    if from_name != to_name {
        let jname = env
            .new_string(&to_name)
            .map_err(|e| opendal_error_from_jni(&mut env, "new_string(rename)", e))?;
        let renamed = env
            .call_static_method(
                &dc,
                "renameDocument",
                "(Landroid/content/ContentResolver;Landroid/net/Uri;Ljava/lang/String;)Landroid/net/Uri;",
                &[JValue::Object(&resolver), JValue::Object(&current_uri), JValue::Object(&jname)],
            )
            .map_err(|e| opendal_error_from_jni(&mut env, "renameDocument", e));
        if renamed.is_err() {
            let _ = take_java_exception_string(&mut env);
            drop(env);
            return copy(tree_uri, root_doc_id, from, to)
                .and_then(|_| delete(tree_uri, root_doc_id, from));
        }
    }

    Ok(())
}

pub fn copy(tree_uri: &str, root_doc_id: &str, from: &str, to: &str) -> Result<()> {
    // Portable implementation: stream copy via file descriptors.
    let src_fd = open_read_fd(tree_uri, root_doc_id, from)?;
    let dst_fd = open_write_fd(tree_uri, root_doc_id, to, false)?;

    // SAF file descriptors are unix fds.
    let mut src = unsafe { std::fs::File::from_raw_fd(src_fd) };
    let mut dst = unsafe { std::fs::File::from_raw_fd(dst_fd) };
    std::io::copy(&mut src, &mut dst).map_err(|e| opendal::raw::new_std_io_error(e))?;
    dst.flush().map_err(opendal::raw::new_std_io_error)?;
    dst.sync_all().map_err(opendal::raw::new_std_io_error)?;
    Ok(())
}

// Unix-only imports.
use std::os::unix::io::FromRawFd;
