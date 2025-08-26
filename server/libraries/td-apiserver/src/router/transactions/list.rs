//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Transactions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::Extension;
use axum::extract::State;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::TRANSACTIONS_LIST;
use td_objects::types::execution::Transaction;
use tower::ServiceExt;

router! {
    state => { Transactions },
    routes => { list_transactions }
}

#[apiserver_path(method = get, path = TRANSACTIONS_LIST, tag = EXECUTION_TAG)]
#[doc = "List transactions"]
pub async fn list_transactions(
    State(transaction): State<Transactions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus<Transaction>, ErrorStatus> {
    let request = context.list((), query_params);
    let response = transaction.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
