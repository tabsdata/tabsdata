//
//  Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::GetErrorStatus;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{FunctionParam, FUNCTION_GET};
use td_objects::types::function::FunctionVersionWithAllVersions;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { read }
}

get_status!(FunctionVersionWithAllVersions);

#[apiserver_path(method = get, path = FUNCTION_GET, tag = FUNCTIONS_TAG)]
#[doc = "Show a current function"]
pub async fn read(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<FunctionParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(param);
    let response = state.read().await.oneshot(request).await?;
    Ok(GetStatus::OK(response.into()))
}
