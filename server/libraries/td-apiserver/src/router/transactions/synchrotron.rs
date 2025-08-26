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
use td_objects::rest_urls::SYNCHROTRON_READ;
use td_objects::types::execution::SynchrotronResponse;
use tower::ServiceExt;

router! {
    state => { Transactions },
    routes => { synchrotron }
}

#[apiserver_path(method = get, path = SYNCHROTRON_READ, tag = EXECUTION_TAG)]
#[doc = "Synchrotron endpoint to list transactions in the system"]
pub async fn synchrotron(
    State(transaction): State<Transactions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus<SynchrotronResponse>, ErrorStatus> {
    let request = context.list((), query_params);
    let response = transaction.synchrotron().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
