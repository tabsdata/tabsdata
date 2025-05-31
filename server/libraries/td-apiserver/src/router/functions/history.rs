//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::list::ListStatus;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::GetErrorStatus;
use axum::extract::{Path, State};
use axum::Extension;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{AtTimeParam, FunctionParam, FUNCTION_HISTORY};
use td_objects::types::table::FunctionAtIdName;
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { function_history }
}

#[apiserver_path(method = get, path = FUNCTION_HISTORY, tag = FUNCTIONS_TAG)]
#[doc = "List history of versions for a function"]
pub async fn function_history(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<ListStatus, GetErrorStatus> {
    let name = FunctionAtIdName::new(function_param, at_param);
    let request = context.list(name, query_params);
    let response = state.history().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
