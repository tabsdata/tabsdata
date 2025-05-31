//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::FunctionRuns;
use crate::status::error_status::ListErrorStatus;
use axum::extract::State;
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{ListParams, ListResponse, ListResponseBuilder, RequestContext};
use td_objects::rest_urls::FUNCTION_RUN_LIST;
use td_objects::types::execution::FunctionRun;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { FunctionRuns },
    routes => { list_function_runs }
}

list_status!(FunctionRun);

#[apiserver_path(method = get, path = FUNCTION_RUN_LIST, tag = EXECUTION_TAG)]
#[doc = "List function runs"]
pub async fn list_function_runs(
    State(function_runs): State<FunctionRuns>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request = context.list((), query_params);
    let response = function_runs.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
