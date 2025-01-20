//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::functions::FUNCTIONS_TAG;
use crate::logic::apisrv::status::error_status::ListErrorStatus;
use crate::{list_status, router};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_concrete::concrete;
use td_objects::crudl::{
    ListParams, ListRequest, ListResponse, ListResponseBuilder, RequestContext,
};
use td_objects::datasets::dto::FunctionList;
use td_objects::rest_urls::{FunctionParam, FUNCTION_HISTORY};
use td_utoipa::{api_server_path, api_server_schema};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        FUNCTION_HISTORY => get(list_dataset_functions),
    }}
}

#[concrete]
#[api_server_schema]
pub type ListResponseFunctions = ListResponse<FunctionList>;
list_status!(ListResponseFunctions);

#[api_server_path(method = get, path = FUNCTION_HISTORY, tag = FUNCTIONS_TAG)]
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
