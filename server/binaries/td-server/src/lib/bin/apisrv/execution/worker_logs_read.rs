//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::execution::EXECUTION_TAG;
use crate::logic::apisrv::status::error_status::CreateErrorStatus;
use crate::router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Extension;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{WorkerMessageParam, WORKER_LOGS};
use td_utoipa::{api_server_path, api_server_schema};
use tower::ServiceExt;
use utoipa::IntoResponses;

#[allow(unused_imports)]
use serde_json::json;

router! {
    state => { DatasetsState },
    paths => {{
        WORKER_LOGS => get(read_worker_logs),
    }}
}

/// This struct is just used to document ParquetFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[allow(dead_code)]
#[api_server_schema]
#[derive(IntoResponses)]
#[response(
    status = 200,
    description = "OK",
    example = json!([]),
    content_type = "application/octet-stream"
)]
pub struct LogsFile(Vec<u8>);

#[api_server_path(method = get, path = WORKER_LOGS, tag = EXECUTION_TAG, override_response = LogsFile)]
#[doc = "Read worker logs"]
pub async fn read_worker_logs(
    State(dataset_state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<WorkerMessageParam>,
) -> Result<impl IntoResponse, CreateErrorStatus> {
    let request = context.read(param);
    let response = dataset_state.read_worker().await.oneshot(request).await?;
    Ok(Body::from_stream(response.into_inner()))
}
