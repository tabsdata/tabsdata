//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::extract::State;
use axum::Extension;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{AtTimeParam, FUNCTION_LIST};
use td_objects::types::function::Function;
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { list_function }
}

#[apiserver_path(method = get, path = FUNCTION_LIST, tag = FUNCTIONS_TAG)]
#[doc = "List functions"]
pub async fn list_function(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<ListStatus<Function>, ErrorStatus> {
    let request = context.list(at_param, query_params);
    let response = state.list().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
