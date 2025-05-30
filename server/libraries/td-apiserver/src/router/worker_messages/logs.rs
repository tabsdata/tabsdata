//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::WorkerMessages;
use crate::status::error_status::ListErrorStatus;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Extension;
#[allow(unused_imports)]
use serde_json::json;
use td_apiforge::{apiserver_path, apiserver_schema};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{WorkerMessageParam, WORKER_LOGS};
use td_tower::ctx_service::IntoData;
use tower::ServiceExt;
use utoipa::IntoResponses;

router! {
    state => { WorkerMessages },
    routes => { worker_message_logs }
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
pub async fn worker_message_logs(
    State(messages): State<WorkerMessages>,
    Extension(context): Extension<RequestContext>,
    Path(path_params): Path<WorkerMessageParam>,
) -> Result<impl IntoResponse, ListErrorStatus> {
    let request = context.read(path_params);
    let response = messages.logs().await.oneshot(request).await?;
    let stream = response.into_data();
    Ok(Body::from_stream(stream.into_inner()))
}
