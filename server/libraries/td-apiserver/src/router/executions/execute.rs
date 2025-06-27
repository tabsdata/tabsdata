//
//   Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::CreateErrorStatus;
use crate::status::extractors::Json;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, create_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FunctionParam, FUNCTION_EXECUTE};
use td_objects::types::execution::{ExecutionRequest, ExecutionResponse};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { execute }
}

create_status!(ExecutionResponse);

#[apiserver_path(method = post, path = FUNCTION_EXECUTE, tag = EXECUTION_TAG)]
#[doc = "Executes a function"]
pub async fn execute(
    State(executions): State<Executions>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
    Json(request): Json<ExecutionRequest>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(function_param, request);
    let response = executions.execute().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}
