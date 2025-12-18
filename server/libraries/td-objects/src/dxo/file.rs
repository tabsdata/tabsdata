//
// Copyright 2025 Tabs Data Inc.
//

use crate::stream::BoxedSyncStream;
use axum::response::IntoResponse;
#[allow(unused_imports)]
use serde_json::json;
use utoipa::IntoResponses;

/// This struct is just used to document ParquetFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[allow(dead_code)]
#[derive(utoipa::ToSchema, IntoResponses)]
#[response(
    status = 200,
    description = "OK",
    example = json!([]),
    content_type = "application/vnd.apache.parquet"
)]
pub struct ParquetFile(pub BoxedSyncStream);

impl IntoResponse for ParquetFile {
    fn into_response(self) -> axum::response::Response {
        self.0.into_response()
    }
}

/// This struct is just used to document CsvFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[allow(dead_code)]
#[derive(utoipa::ToSchema, IntoResponses)]
#[response(status = 200, description = "OK", content_type = "text/csv")]
pub struct CsvFile(pub BoxedSyncStream);

impl IntoResponse for CsvFile {
    fn into_response(self) -> axum::response::Response {
        self.0.into_response()
    }
}

/// This struct is just used to document PemFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[derive(utoipa::ToSchema, IntoResponses)]
#[response(
    status = 200,
    description = "OK",
    example = json!([]),
    content_type = "application/x-pem-file"
)]
pub struct PemFile(pub BoxedSyncStream);

impl IntoResponse for PemFile {
    fn into_response(self) -> axum::response::Response {
        self.0.into_response()
    }
}

/// This struct is just used to document LogsFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[allow(dead_code)]
#[derive(utoipa::ToSchema, IntoResponses)]
#[response(
    status = 200,
    description = "OK",
    example = json!([]),
    content_type = "application/octet-stream"
)]
pub struct LogsFile(pub BoxedSyncStream);

impl IntoResponse for LogsFile {
    fn into_response(self) -> axum::response::Response {
        self.0.into_response()
    }
}
