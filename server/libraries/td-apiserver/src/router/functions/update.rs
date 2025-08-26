//
//  Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::ErrorStatus;
use crate::status::extractors::Json;
use crate::status::ok_status::UpdateStatus;
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FUNCTION_UPDATE, FunctionParam};
use td_objects::types::function::{Function, FunctionUpdate};
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { update }
}

#[apiserver_path(method = post, path = FUNCTION_UPDATE, tag = FUNCTIONS_TAG)]
#[doc = "Update a function"]
pub async fn update(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
    Json(request): Json<FunctionUpdate>,
) -> Result<UpdateStatus<Function>, ErrorStatus> {
    let request = context.update(function_param, request);
    let response = state.update().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response))
}
