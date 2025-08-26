//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::Extension;
use axum::extract::State;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::EXECUTION_LIST;
use td_objects::types::execution::Execution;
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { list_executions }
}

#[apiserver_path(method = get, path = EXECUTION_LIST, tag = EXECUTION_TAG)]
#[doc = "List executions"]
pub async fn list_executions(
    State(executions): State<Executions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus<Execution>, ErrorStatus> {
    let request = context.list((), query_params);
    let response = executions.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
