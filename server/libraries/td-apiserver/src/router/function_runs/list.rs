//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::FunctionRuns;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::Extension;
use axum::extract::State;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::FUNCTION_RUN_LIST;
use td_objects::types::execution::FunctionRun;
use tower::ServiceExt;

router! {
    state => { FunctionRuns },
    routes => { list_function_runs }
}

#[apiserver_path(method = get, path = FUNCTION_RUN_LIST, tag = EXECUTION_TAG)]
#[doc = "List function runs"]
pub async fn list_function_runs(
    State(function_runs): State<FunctionRuns>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus<FunctionRun>, ErrorStatus> {
    let request = context.list((), query_params);
    let response = function_runs.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
