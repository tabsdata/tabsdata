//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::{NoContent, UpdateStatus};
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{EXECUTION_CANCEL, ExecutionParam};
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { cancel_execution }
}

#[apiserver_path(method = post, path = EXECUTION_CANCEL, tag = EXECUTION_TAG)]
#[doc = "Cancel all transactions in the given execution"]
pub async fn cancel_execution(
    State(executions): State<Executions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<ExecutionParam>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(param, ());
    let response = executions.cancel().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response))
}
