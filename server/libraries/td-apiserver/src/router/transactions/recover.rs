//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Transactions;
use crate::status::error_status::UpdateErrorStatus;
use crate::status::EmptyUpdateStatus;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{TransactionParam, TRANSACTION_RECOVER};
use tower::ServiceExt;

router! {
    state => { Transactions },
    routes => { recover_transaction }
}

#[apiserver_path(method = post, path = TRANSACTION_RECOVER, tag = EXECUTION_TAG)]
#[doc = "Recover all function runs in the given transaction"]
pub async fn recover_transaction(
    State(transaction): State<Transactions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<TransactionParam>,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let request = context.update(param, ());
    let response = transaction.recover().await.oneshot(request).await?;
    Ok(EmptyUpdateStatus::OK(response.into()))
}
