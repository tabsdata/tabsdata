//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::GetErrorStatus;
use axum::extract::State;
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::ListResponseBuilder;
use td_objects::crudl::{ListParams, ListResponse, RequestContext};
use td_objects::rest_urls::{AtTimeParam, FUNCTION_LIST};
use td_objects::types::function::Function;
use td_tower::ctx_service::CtxMap;
use td_tower::ctx_service::CtxResponse;
use td_tower::ctx_service::CtxResponseBuilder;
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { list_function }
}

list_status!(Function);

#[apiserver_path(method = get, path = FUNCTION_LIST, tag = FUNCTIONS_TAG)]
#[doc = "List functions"]
pub async fn list_function(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<ListStatus, GetErrorStatus> {
    let request = context.list(at_param, query_params);
    let response = state.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
