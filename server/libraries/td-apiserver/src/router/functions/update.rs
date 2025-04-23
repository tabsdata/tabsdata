//
//  Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::UpdateErrorStatus;
use crate::status::extractors::Json;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, update_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FunctionParam, FUNCTION_UPDATE};
use td_objects::types::function::{FunctionUpdate, FunctionVersion};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { update }
}

update_status!(FunctionVersion);

#[apiserver_path(method = post, path = FUNCTION_UPDATE, tag = FUNCTIONS_TAG)]
#[doc = "Update a function"]
pub async fn update(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
    Json(request): Json<FunctionUpdate>,
) -> Result<UpdateStatus, UpdateErrorStatus> {
    let request = context.update(function_param, request);
    let response = state.update().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response.into()))
}
