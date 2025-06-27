//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::GetErrorStatus;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{ExecutionParam, EXECUTION_READ};
use td_objects::types::execution::ExecutionResponse;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { read_execution }
}

get_status!(ExecutionResponse);

#[apiserver_path(method = get, path = EXECUTION_READ, tag = EXECUTION_TAG)]
#[doc = "Read an execution"]
pub async fn read_execution(
    State(executions): State<Executions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<ExecutionParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(param);
    let response = executions.read().await.oneshot(request).await?;
    Ok(GetStatus::OK(response.into()))
}
