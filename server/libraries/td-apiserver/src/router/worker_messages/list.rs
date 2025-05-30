//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::WorkerMessages;
use crate::status::error_status::ListErrorStatus;
use axum::extract::{Path, State};
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::crudl::{ListResponse, ListResponseBuilder};
use td_objects::rest_urls::{TransactionParam, EXECUTION_LIST};
use td_objects::types::execution::WorkerMessage;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { WorkerMessages },
    routes => { list_worker_messages }
}

list_status!(WorkerMessage);

#[apiserver_path(method = delete, path = EXECUTION_LIST, tag = EXECUTION_TAG)]
#[doc = "List executions"]
pub async fn list_worker_messages(
    State(messages): State<WorkerMessages>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Path(path_params): Path<TransactionParam>,
) -> Result<ListStatus, ListErrorStatus> {
    let request = context.list(path_params, query_params);
    let response = messages.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
