//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::functions::FUNCTIONS_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::router;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{api_server_path, get_status};
use td_objects::crudl::{ListParams, ListRequest, RequestContext};
use td_objects::datasets::dto::FunctionRead;
use td_objects::rest_urls::{FunctionParam, FUNCTION_GET};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

// TODO(TD-281) add Datasets logic, clean unused code serving as example
router! {
    state => { DatasetsState },
    paths => {{
        FUNCTION_GET => get(read_dataset_function),
    }}
}

pub type GetResponseFunction = FunctionRead;
get_status!(GetResponseFunction);

#[api_server_path(method = get, path = FUNCTION_GET, tag = FUNCTIONS_TAG)]
#[doc = "Show a current function"]
pub async fn read_dataset_function(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(collection_dataset): Path<FunctionParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let request: ListRequest<FunctionParam> = context.list(collection_dataset, ListParams::first());
    let response = state
        .list_dataset_functions()
        .await
        .oneshot(request)
        .await?;
    let response = response.transform(|v| v.data()[0].clone());
    Ok(GetStatus::OK(response.into()))
}
