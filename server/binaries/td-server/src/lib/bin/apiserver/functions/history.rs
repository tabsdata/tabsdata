//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apiserver::functions::FUNCTIONS_TAG;
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::ListErrorStatus;
use crate::router;
use axum::extract::{Path, Query, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::{
    ListParams, ListRequest, ListResponse, ListResponseBuilder, RequestContext,
};
use td_objects::datasets::dto::FunctionList;
use td_objects::rest_urls::{FunctionParam, FUNCTION_HISTORY};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    routes => { list_dataset_functions }
}

list_status!(FunctionList);

#[apiserver_path(method = get, path = FUNCTION_HISTORY, tag = FUNCTIONS_TAG)]
#[doc = "List a function history"]
pub async fn list_dataset_functions(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(collection_dataset): Path<FunctionParam>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request: ListRequest<FunctionParam> = context.list(collection_dataset, query_params);
    let response = state
        .list_dataset_functions()
        .await
        .oneshot(request)
        .await?;
    Ok(ListStatus::OK(response.into()))
}
