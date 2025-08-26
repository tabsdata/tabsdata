//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::GetStatus;
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{EXECUTION_READ, ExecutionParam};
use td_objects::types::execution::ExecutionResponse;
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { read_execution }
}

#[apiserver_path(method = get, path = EXECUTION_READ, tag = EXECUTION_TAG)]
#[doc = "Read an execution"]
pub async fn read_execution(
    State(executions): State<Executions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<ExecutionParam>,
) -> Result<GetStatus<ExecutionResponse>, ErrorStatus> {
    let request = context.read(param);
    let response = executions.read().await.oneshot(request).await?;
    Ok(GetStatus::OK(response))
}
