//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::executions::EXECUTION_TAG;
use crate::router::state::Executions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::{NoContent, UpdateStatus};
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{ExecutionParam, EXECUTION_RECOVER};
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { recover_execution }
}

#[apiserver_path(method = post, path = EXECUTION_RECOVER, tag = EXECUTION_TAG)]
#[doc = "Recover all transactions in the given execution"]
pub async fn recover_execution(
    State(executions): State<Executions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<ExecutionParam>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(param, ());
    let response = executions.recover().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response))
}
