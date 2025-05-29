//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Transactions;
use crate::status::error_status::ListErrorStatus;
use axum::extract::State;
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{ListParams, ListResponse, ListResponseBuilder, RequestContext};
use td_objects::rest_urls::SYNCHROTRON_READ;
use td_objects::types::execution::SynchrotronResponse;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Transactions },
    routes => { synchrotron }
}

list_status!(SynchrotronResponse);

#[apiserver_path(method = get, path = SYNCHROTRON_READ, tag = EXECUTION_TAG)]
#[doc = "Synchrotron endpoint to list transactions in the system"]
pub async fn synchrotron(
    State(transaction): State<Transactions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request = context.list((), query_params);
    let response = transaction.synchrotron().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
