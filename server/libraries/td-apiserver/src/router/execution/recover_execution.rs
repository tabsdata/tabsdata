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
use td_objects::rest_urls::{ExecutionParam, EXECUTION_RECOVER};
use tower::ServiceExt;

router! {
    state => { Execution },
    routes => { recover_execution }
}

#[apiserver_path(method = post, path = EXECUTION_RECOVER, tag = EXECUTION_TAG)]
#[doc = "Recover all transactions in the given execution"]
pub async fn recover_execution(
    State(execution): State<Execution>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<ExecutionParam>,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let request = context.update(param, ());
    let response = execution.recover_execution().await.oneshot(request).await?;
    Ok(EmptyUpdateStatus::OK(response.into()))
}
