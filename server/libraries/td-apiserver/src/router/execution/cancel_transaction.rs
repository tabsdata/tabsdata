//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::execution::EXECUTION_TAG;
use crate::router::state::Execution;
use crate::status::error_status::UpdateErrorStatus;
use crate::status::EmptyUpdateStatus;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{TransactionParam, TRANSACTION_CANCEL};
use tower::ServiceExt;

router! {
    state => { Execution },
    routes => { cancel_transaction }
}

#[apiserver_path(method = post, path = TRANSACTION_CANCEL, tag = EXECUTION_TAG)]
#[doc = "Cancel all function runs in the given transaction"]
pub async fn cancel_transaction(
    State(execution): State<Execution>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<TransactionParam>,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let request = context.update(param, ());
    let response = execution
        .cancel_transaction()
        .await
        .oneshot(request)
        .await?;
    Ok(EmptyUpdateStatus::OK(response.into()))
}
