//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::state::Executions;
use crate::status::error_status::ErrorStatus;
use crate::status::extractors::Json;
use crate::status::ok_status::{NoContent, UpdateStatus};
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::{apiserver_path, apiserver_tag};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FunctionRunIdParam, UPDATE_FUNCTION_RUN};
use td_objects::types::execution::CallbackRequest;
use tower::ServiceExt;

router! {
    state => { Executions },
    routes => { callback }
}

apiserver_tag!(name = "Internal", description = "Internal API");

#[apiserver_path(method = post, path = UPDATE_FUNCTION_RUN, tag = INTERNAL_TAG)]
#[doc = "Callback endpoint for function executions"]
pub async fn callback(
    State(execution): State<Executions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<FunctionRunIdParam>,
    Json(request): Json<CallbackRequest>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(param, request);
    let response = execution.callback().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response))
}
