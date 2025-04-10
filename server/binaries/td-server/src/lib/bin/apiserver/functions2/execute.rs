//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apiserver::execution::EXECUTION_TAG;
use crate::bin::apiserver::ExecutionState;
use crate::logic::apiserver::status::error_status::CreateErrorStatus;
use crate::logic::apiserver::status::extractors::Json;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::{apiserver_path, create_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FunctionParam, FUNCTION_EXECUTE};
use td_objects::types::execution::{ExecutionRequest, ExecutionResponse};
use tower::ServiceExt;

router! {
    state => { ExecutionState },
    routes => { execute }
}

create_status!(ExecutionResponse);

#[apiserver_path(method = post, path = FUNCTION_EXECUTE, tag = EXECUTION_TAG)]
#[doc = "Executes a function"]
pub async fn execute(
    State(function_state): State<ExecutionState>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
    Json(request): Json<ExecutionRequest>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(function_param, request);
    let response = function_state.execute().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}
