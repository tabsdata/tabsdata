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
use td_objects::rest_urls::TRANSACTIONS_LIST;
use td_objects::types::execution::Transaction;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Transactions },
    routes => { list_transactions }
}

list_status!(Transaction);

#[apiserver_path(method = get, path = TRANSACTIONS_LIST, tag = EXECUTION_TAG)]
#[doc = "List transactions"]
pub async fn list_transactions(
    State(transaction): State<Transactions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request = context.list((), query_params);
    let response = transaction.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
