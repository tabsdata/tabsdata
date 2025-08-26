//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Workers;
use crate::status::error_status::ErrorStatus;
use axum::Extension;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum_extra::extract::Query;
#[allow(unused_imports)]
use serde_json::json;
use td_apiforge::{apiserver_path, apiserver_schema};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{WORKER_LOGS, WorkerLogsParams, WorkerLogsQueryParams, WorkerParam};
use td_tower::ctx_service::RawOneshot;
use utoipa::IntoResponses;

router! {
    state => { Workers },
    routes => { worker_logs }
}

/// This struct is just used to document ParquetFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[allow(dead_code)]
#[apiserver_schema]
#[derive(IntoResponses)]
#[response(
    status = 200,
    description = "OK",
    example = json!([]),
    content_type = "application/octet-stream"
)]
pub struct LogsFile(Vec<u8>);

#[apiserver_path(method = get, path = WORKER_LOGS, tag = EXECUTION_TAG, override_response = LogsFile)]
#[doc = "Read worker message logs"]
pub async fn worker_logs(
    State(messages): State<Workers>,
    Extension(context): Extension<RequestContext>,
    Path(path_params): Path<WorkerParam>,
    Query(query_params): Query<WorkerLogsQueryParams>,
) -> Result<impl IntoResponse, ErrorStatus> {
    let params = WorkerLogsParams::new(path_params, query_params);
    let request = context.read(params);
    let response = messages.logs().await.raw_oneshot(request).await?;
    Ok(Body::from_stream(response.into_inner()))
}
