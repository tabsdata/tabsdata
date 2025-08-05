//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::FunctionRuns;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::GetStatus;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FunctionRunParam, FUNCTION_RUN_GET};
use td_objects::types::execution::FunctionRun;
use tower::ServiceExt;

router! {
    state => { FunctionRuns },
    routes => { read_run }
}

#[apiserver_path(method = get, path = FUNCTION_RUN_GET, tag = EXECUTION_TAG)]
#[doc = "Read function run"]
pub async fn read_run(
    State(runs): State<FunctionRuns>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<FunctionRunParam>,
) -> Result<GetStatus<FunctionRun>, ErrorStatus> {
    let request = context.read(param);
    let response = runs.read().await.oneshot(request).await?;
    Ok(GetStatus::OK(response))
}
