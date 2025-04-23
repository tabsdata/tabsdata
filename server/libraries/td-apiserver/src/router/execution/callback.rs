//
//   Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::state::Execution;
use crate::status::error_status::UpdateErrorStatus;
use crate::status::extractors::Json;
use crate::status::EmptyUpdateStatus;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::{apiserver_path, apiserver_tag};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FunctionRunParam, UPDATE_FUNCTION_RUN};
use td_objects::types::execution::CallbackRequest;
use tower::ServiceExt;

router! {
    state => { Execution },
    routes => { callback }
}

apiserver_tag!(name = "Internal", description = "Internal API");

#[apiserver_path(method = post, path = UPDATE_FUNCTION_RUN, tag = INTERNAL_TAG)]
#[doc = "Callback endpoint for function executions"]
pub async fn callback(
    State(execution): State<Execution>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<FunctionRunParam>,
    Json(request): Json<CallbackRequest>,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let request = context.update(param, request);
    let response = execution.callback().await.oneshot(request).await?;
    Ok(EmptyUpdateStatus::OK(response.into()))
}
