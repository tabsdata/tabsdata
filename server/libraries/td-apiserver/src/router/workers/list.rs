//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Workers;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::Extension;
use axum::extract::State;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::WORKERS_LIST;
use td_objects::types::execution::Worker;
use tower::ServiceExt;

router! {
    state => { Workers },
    routes => { list_workers }
}

#[apiserver_path(method = get, path = WORKERS_LIST, tag = EXECUTION_TAG)]
#[doc = "List worker messages"]
pub async fn list_workers(
    State(messages): State<Workers>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus<Worker>, ErrorStatus> {
    let request = context.list((), query_params);
    let response = messages.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
