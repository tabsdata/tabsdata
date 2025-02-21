//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::data::DATA_TAG;
use crate::logic::apisrv::status::error_status::ListErrorStatus;
use crate::router;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{api_server_path, list_status};
use td_objects::crudl::{
    ListParams, ListRequest, ListResponse, ListResponseBuilder, RequestContext,
};
use td_objects::datasets::dto::DataVersionList;
use td_objects::rest_urls::FunctionParam;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

// TODO(TD-281) add Datasets logic, clean unused code serving as example
router! {
    state => { DatasetsState },
    paths => {{
        LIST_DATASET_VERSIONS => get(list_dataset_versions),
    }}
}

list_status!(DataVersionList);

pub const LIST_DATASET_VERSIONS: &str = "/collections/{collection}/functions/{function}/versions";
#[api_server_path(method = get, path = LIST_DATASET_VERSIONS, tag = DATA_TAG)]
#[doc = "List the versions of a collection"]
pub async fn list_dataset_versions(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(function_param): Path<FunctionParam>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request: ListRequest<FunctionParam> = context.list(function_param, query_params);
    let response = state.list_dataset_versions().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
