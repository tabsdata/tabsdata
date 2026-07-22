//
// Copyright 2026 Tabsdata Inc.
//

//! `GET /api/v1/resources/{*path}` — serve a file from the apiserver's
//! configured `resources_folder`.
//!
//! Authenticated (standard Bearer token, via the shared authorization layer)
//! but **not** authorized: any logged-in user may read any file under the
//! directory and its subdirs. The response is always `application/octet-stream`.
//! A request for `/resources/conns/foo.svg` serves `<resources_folder>/conns/foo.svg`.
//!
//! Returns `404` when the route is unconfigured, the requested path escapes the
//! root, or the target is missing / not a regular file.

use std::path::{Component, Path as StdPath, PathBuf};
use std::sync::Arc;
use td_apiforge::router_ext;

/// The configured resources root, injected into each request as an extension.
/// `None` disables the route (every request `404`s).
#[derive(Clone)]
pub struct ResourcesDir(pub Arc<Option<PathBuf>>);

/// The greedy `{*path}` segment of `/resources/{*path}` — the file path
/// (with subdirectories) relative to the resources directory.
#[derive(serde::Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Path)]
pub struct ResourcePathParam {
    /// File path relative to the resources directory, e.g. `conns/foo.svg`.
    pub path: String,
}

/// Join `rel` under `root`, rejecting anything that is not a plain relative
/// path strictly under `root`: absolute paths, `..`, `.`, and (via
/// canonicalization) symlinks that escape the directory.
fn safe_join(root: &StdPath, rel: &str) -> Option<PathBuf> {
    let mut joined = root.to_path_buf();
    for component in StdPath::new(rel).components() {
        match component {
            Component::Normal(c) => joined.push(c),
            // RootDir / Prefix (absolute), ParentDir (`..`), CurDir (`.`).
            _ => return None,
        }
    }
    // Canonicalize and confirm the result is still under the canonical root.
    let root = root.canonicalize().ok()?;
    let joined = joined.canonicalize().ok()?;
    joined.starts_with(&root).then_some(joined)
}

#[router_ext(ResourcesRouter)]
mod routes {
    use super::{ResourcePathParam, ResourcesDir, safe_join};
    use axum::Extension;
    use axum::extract::Path;
    use bytes::Bytes;
    use ta_apiserver::status::error_status::ErrorStatus;
    use td_apiforge::apiserver_path;
    use td_error::{ApiError, TdError, api_error};
    use td_objects::dxo::file::DownloadResponse;
    use td_objects::rest_urls::RESOURCES;
    use td_objects::types::stream::BoxedSyncStream;

    const SYSTEM_TAG: &str = "System";

    fn not_found() -> ErrorStatus {
        api_error!(ApiError::NotFound, "resource not found").into()
    }

    #[apiserver_path(
        method = get,
        path = RESOURCES,
        tag = SYSTEM_TAG,
        content_type = "application/octet-stream",
    )]
    #[doc = "Serve a file from the apiserver's configured resources directory"]
    pub async fn get_resource(
        Extension(dir): Extension<ResourcesDir>,
        Path(params): Path<ResourcePathParam>,
    ) -> Result<DownloadResponse, ErrorStatus> {
        let root = dir.0.as_ref().as_ref().ok_or_else(not_found)?;
        let path = safe_join(root, &params.path).ok_or_else(not_found)?;
        // `read` errors for both missing paths and directories -> 404.
        let bytes = tokio::fs::read(&path).await.map_err(|_| not_found())?;
        let stream = BoxedSyncStream::new(futures::stream::once(async move {
            Ok::<_, TdError>(Bytes::from(bytes))
        }));
        Ok(DownloadResponse::octet_stream(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::extract::Request;
    use axum::{Extension, Router};
    use http::header::CONTENT_TYPE;
    use http::{Method, StatusCode};
    use ta_apiserver::router::RouterExtension;
    use tower::ServiceExt;

    fn temp_root(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("td-resources-{}-{}", std::process::id(), tag));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn app(root: Option<PathBuf>) -> Router {
        let router: Router = ResourcesRouter::router(()).into();
        router.layer(Extension(ResourcesDir(Arc::new(root))))
    }

    async fn get(app: Router, rel: &str) -> (StatusCode, Option<String>, Vec<u8>) {
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/resources/{rel}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let ct = resp
            .headers()
            .get(CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_string());
        let body = to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, ct, body)
    }

    #[tokio::test]
    async fn serves_a_file_as_octet_stream() {
        let root = temp_root("serve");
        std::fs::create_dir_all(root.join("conns")).unwrap();
        std::fs::write(root.join("conns/foo.svg"), b"<svg/>").unwrap();

        let (status, ct, body) = get(app(Some(root.clone())), "conns/foo.svg").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ct.as_deref(), Some("application/octet-stream"));
        assert_eq!(body, b"<svg/>");
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_file_is_404() {
        let root = temp_root("missing");
        let (status, ..) = get(app(Some(root.clone())), "nope.txt").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn unconfigured_is_404() {
        let (status, ..) = get(app(None), "anything.txt").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn safe_join_rejects_traversal_and_absolute() {
        let root = temp_root("guard");
        std::fs::write(root.join("ok.txt"), b"x").unwrap();
        // A normal nested path resolves under the root.
        assert!(safe_join(&root, "ok.txt").is_some());
        // Traversal / current-dir / absolute escapes are rejected.
        assert!(safe_join(&root, "../etc/passwd").is_none());
        assert!(safe_join(&root, "a/../../b").is_none());
        assert!(safe_join(&root, "/etc/passwd").is_none());
        std::fs::remove_dir_all(&root).ok();
    }
}
