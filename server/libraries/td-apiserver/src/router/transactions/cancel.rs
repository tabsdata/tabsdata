//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Transactions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::{NoContent, UpdateStatus};
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{TransactionParam, TRANSACTION_CANCEL};
use tower::ServiceExt;

router! {
    state => { Transactions },
    routes => { cancel_transaction }
}

#[apiserver_path(method = post, path = TRANSACTION_CANCEL, tag = EXECUTION_TAG)]
#[doc = "Cancel all function runs in the given transaction"]
pub async fn cancel_transaction(
    State(transaction): State<Transactions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<TransactionParam>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(param, ());
    let response = transaction.cancel().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response))
}
