//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::ListErrorStatus;
use axum::extract::State;
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::crudl::{ListResponse, ListResponseBuilder};
use td_objects::rest_urls::EXECUTION_LIST;
use td_objects::types::execution::Execution;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { list_executions }
}

list_status!(Execution);

#[apiserver_path(method = get, path = EXECUTION_LIST, tag = EXECUTION_TAG)]
#[doc = "List executions"]
pub async fn list_executions(
    State(execution): State<Executions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request = context.list((), query_params);
    let response = execution.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
