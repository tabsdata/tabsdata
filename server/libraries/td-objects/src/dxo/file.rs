//
// Copyright 2025 Tabsdata Inc.
//

//! [`DownloadResponse`] — the standard streaming response for download
//! endpoints. Carries the byte stream plus optional `Content-Disposition`
//! filename, `ETag`, `Cache-Control`, and the request's `If-None-Match` for
//! a `304 Not Modified` short-circuit. RFC 7232 / 6266 / 8246 details come
//! from the `headers` crate.

use crate::types::stream::BoxedSyncStream;
use axum::response::{IntoResponse, Response};
use headers::{CacheControl, ETag, HeaderMapExt, IfNoneMatch};
use http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use http::{HeaderValue, StatusCode};
use std::borrow::Cow;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use utoipa::openapi::Schema;
use utoipa::{PartialSchema, ToSchema};

pub const CONTENT_TYPE_OCTET_STREAM: &str = "application/octet-stream";
pub const CONTENT_TYPE_PARQUET: &str = "application/vnd.apache.parquet";
pub const CONTENT_TYPE_CSV: &str = "text/csv";
pub const CONTENT_TYPE_PEM: &str = "application/x-pem-file";

const IMMUTABLE_MAX_AGE: Duration = Duration::from_secs(31_536_000); // 1 year

pub struct DownloadResponse {
    stream: BoxedSyncStream,
    content_type: &'static str,
    disposition: Option<HeaderValue>,
    etag: Option<ETag>,
    cache_control: Option<CacheControl>,
    if_none_match: Option<IfNoneMatch>,
}

impl DownloadResponse {
    pub fn new(stream: BoxedSyncStream, content_type: &'static str) -> Self {
        Self {
            stream,
            content_type,
            disposition: None,
            etag: None,
            cache_control: None,
            if_none_match: None,
        }
    }

    pub fn octet_stream(stream: BoxedSyncStream) -> Self {
        Self::new(stream, CONTENT_TYPE_OCTET_STREAM)
    }

    pub fn parquet(stream: BoxedSyncStream) -> Self {
        Self::new(stream, CONTENT_TYPE_PARQUET)
    }

    pub fn csv(stream: BoxedSyncStream) -> Self {
        Self::new(stream, CONTENT_TYPE_CSV)
    }

    pub fn pem(stream: BoxedSyncStream) -> Self {
        Self::new(stream, CONTENT_TYPE_PEM)
    }

    /// Set `Content-Disposition: attachment; filename="..."`. Filename must
    /// be safe ASCII (no quotes, backslashes, control chars); panics
    /// otherwise. All current callers pass typed identifiers
    /// (function/table names, ids) that satisfy this.
    pub fn attachment(mut self, filename: impl Display) -> Self {
        let value = format!("attachment; filename=\"{filename}\"");
        let header = HeaderValue::from_str(&value)
            .expect("filename must be safe ASCII for Content-Disposition");
        self.disposition = Some(header);
        self
    }

    /// Strong ETag from the opaque-tag content (raw hash/identifier). The
    /// value is quoted internally — pass `BundleHash`, a UUID, etc., not
    /// `"\"...\""`. Panics if `value` contains characters outside the
    /// opaque-tag grammar.
    pub fn etag(mut self, value: impl Display) -> Self {
        let etag = ETag::from_str(&format!("\"{value}\""))
            .expect("ETag opaque value must be ASCII without quotes/backslashes/control chars");
        self.etag = Some(etag);
        self
    }

    pub fn cache_control(mut self, cc: CacheControl) -> Self {
        self.cache_control = Some(cc);
        self
    }

    /// `Cache-Control: immutable, max-age=31536000` — for content-addressed
    /// payloads.
    pub fn immutable(self) -> Self {
        self.cache_control(
            CacheControl::new()
                .with_immutable()
                .with_max_age(IMMUTABLE_MAX_AGE),
        )
    }

    pub fn with_if_none_match(mut self, if_none_match: Option<IfNoneMatch>) -> Self {
        self.if_none_match = if_none_match;
        self
    }
}

// utoipa-axum walks each handler's OK type to collect schemas; the inline
// `responses(..., body = Vec<u8>)` on the route attribute documents the
// actual body type, but the type itself still needs a schema for the walker.
impl PartialSchema for DownloadResponse {
    fn schema() -> utoipa::openapi::RefOr<Schema> {
        <Vec<u8> as PartialSchema>::schema()
    }
}

impl ToSchema for DownloadResponse {
    fn name() -> Cow<'static, str> {
        "Data".into()
    }
}

impl IntoResponse for DownloadResponse {
    fn into_response(self) -> Response {
        if let (Some(inm), Some(etag)) = (self.if_none_match.as_ref(), self.etag.as_ref())
            && !inm.precondition_passes(etag)
        {
            let mut resp = StatusCode::NOT_MODIFIED.into_response();
            resp.headers_mut().typed_insert(etag.clone());
            return resp;
        }

        let mut resp = self.stream.into_response();
        let h = resp.headers_mut();
        h.insert(CONTENT_TYPE, HeaderValue::from_static(self.content_type));
        if let Some(d) = self.disposition {
            h.insert(CONTENT_DISPOSITION, d);
        }
        if let Some(e) = self.etag {
            h.typed_insert(e);
        }
        if let Some(c) = self.cache_control {
            h.typed_insert(c);
        }
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use bytes::Bytes;
    use futures::stream;
    use http::header::{
        CACHE_CONTROL, CONTENT_DISPOSITION, ETAG as ETAG_NAME, IF_NONE_MATCH as IF_NONE_MATCH_NAME,
    };

    fn payload_stream(payload: &'static [u8]) -> BoxedSyncStream {
        BoxedSyncStream::new(stream::once(async move { Ok(Bytes::from_static(payload)) }))
    }

    async fn body_bytes(resp: Response) -> Vec<u8> {
        to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn body_only_when_no_extras() {
        let resp = DownloadResponse::octet_stream(payload_stream(b"hello")).into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );
        assert!(resp.headers().get(CONTENT_DISPOSITION).is_none());
        assert!(resp.headers().get(ETAG_NAME).is_none());
        assert!(resp.headers().get(CACHE_CONTROL).is_none());
        assert_eq!(body_bytes(resp).await, b"hello");
    }

    #[tokio::test]
    async fn attachment_sets_content_disposition() {
        let resp = DownloadResponse::octet_stream(payload_stream(b""))
            .attachment("foo-bar.tar.zst")
            .into_response();
        let cd = resp.headers().get(CONTENT_DISPOSITION).unwrap();
        assert!(cd.to_str().unwrap().contains("foo-bar.tar.zst"));
    }

    #[tokio::test]
    async fn attachment_accepts_format_args() {
        let resp = DownloadResponse::octet_stream(payload_stream(b""))
            .attachment(format_args!("{}-{}.tar.zst", "foo", 42))
            .into_response();
        let cd = resp.headers().get(CONTENT_DISPOSITION).unwrap();
        assert_eq!(cd, "attachment; filename=\"foo-42.tar.zst\"");
    }

    #[tokio::test]
    async fn etag_quotes_opaque_value() {
        let resp = DownloadResponse::octet_stream(payload_stream(b"x"))
            .etag("abc")
            .into_response();
        assert_eq!(resp.headers().get(ETAG_NAME).unwrap(), "\"abc\"");
    }

    fn build_inm(value: &str) -> IfNoneMatch {
        let mut m = http::HeaderMap::new();
        m.insert(IF_NONE_MATCH_NAME, HeaderValue::from_str(value).unwrap());
        m.typed_get::<IfNoneMatch>().unwrap()
    }

    #[tokio::test]
    async fn matching_if_none_match_returns_304() {
        let resp = DownloadResponse::octet_stream(payload_stream(b"x"))
            .etag("abc")
            .with_if_none_match(Some(build_inm("\"abc\"")))
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(resp.headers().get(ETAG_NAME).unwrap(), "\"abc\"");
        assert!(body_bytes(resp).await.is_empty());
    }

    #[tokio::test]
    async fn weak_if_none_match_still_matches() {
        let resp = DownloadResponse::octet_stream(payload_stream(b"x"))
            .etag("abc")
            .with_if_none_match(Some(build_inm("W/\"abc\"")))
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn star_if_none_match_returns_304() {
        let resp = DownloadResponse::octet_stream(payload_stream(b"x"))
            .etag("abc")
            .with_if_none_match(Some(build_inm("*")))
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn comma_list_with_one_match_returns_304() {
        let resp = DownloadResponse::octet_stream(payload_stream(b"x"))
            .etag("abc")
            .with_if_none_match(Some(build_inm("\"other\", \"abc\", \"more\"")))
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn non_matching_if_none_match_returns_200() {
        let resp = DownloadResponse::octet_stream(payload_stream(b"hello"))
            .etag("abc")
            .with_if_none_match(Some(build_inm("\"different\"")))
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_bytes(resp).await, b"hello");
    }

    #[tokio::test]
    async fn if_none_match_without_etag_is_noop() {
        let resp = DownloadResponse::octet_stream(payload_stream(b"hello"))
            .with_if_none_match(Some(build_inm("*")))
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_bytes(resp).await, b"hello");
    }

    #[tokio::test]
    async fn immutable_sets_cache_control() {
        let resp = DownloadResponse::octet_stream(payload_stream(b""))
            .immutable()
            .into_response();
        let cc = resp.headers().get(CACHE_CONTROL).unwrap().to_str().unwrap();
        assert!(cc.contains("immutable"));
        assert!(cc.contains("max-age=31536000"));
    }

    #[tokio::test]
    async fn parquet_constructor_sets_correct_content_type() {
        let resp = DownloadResponse::parquet(payload_stream(b"")).into_response();
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).unwrap(),
            "application/vnd.apache.parquet"
        );
    }
}
