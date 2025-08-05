//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::functions::FUNCTIONS_TAG;
use crate::router::state::Functions;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::extract::{Path, State};
use axum::Extension;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{AtTimeParam, CollectionParam, FUNCTION_LIST_BY_COLL};
use td_objects::types::function::Function;
use td_objects::types::table::CollectionAtName;
use tower::ServiceExt;

router! {
    state => { Functions },
    routes => { list_function_by_collection }
}

#[apiserver_path(method = get, path = FUNCTION_LIST_BY_COLL, tag = FUNCTIONS_TAG)]
#[doc = "List functions for a collection"]
pub async fn list_function_by_collection(
    State(state): State<Functions>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<ListStatus<Function>, ErrorStatus> {
    let name = CollectionAtName::new(collection_param, at_param);
    let request = context.list(name, query_params);
    let response = state.list_by_collection().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
