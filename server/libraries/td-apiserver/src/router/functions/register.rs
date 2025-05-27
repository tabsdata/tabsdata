//
//  Copyright 2024 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::CreateErrorStatus;
use crate::status::extractors::Json;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, create_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{CollectionParam, FUNCTION_CREATE};
use td_objects::types::function::{Function, FunctionRegister};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { register }
}

create_status!(Function);

#[apiserver_path(method = post, path = FUNCTION_CREATE, tag = FUNCTIONS_TAG)]
#[doc = "Register a function"]
pub async fn register(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
    Json(request): Json<FunctionRegister>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(collection_param, request);
    let response = state.register().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}
