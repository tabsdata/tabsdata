//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::functions::{DATASETS, FUNCTIONS_TAG};
use crate::logic::apisrv::status::error_status::ListErrorStatus;
use crate::{list_status, router};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_concrete::concrete;
use td_objects::crudl::ListResponse;
use td_objects::crudl::ListResponseBuilder;
use td_objects::crudl::{ListParams, ListRequest, RequestContext};
use td_objects::datasets::dto::DatasetList;
use td_objects::dlo::CollectionName;
use td_objects::rest_urls::CollectionParam;
use td_utoipa::{api_server_path, api_server_schema};
use tower::ServiceExt;

// TODO(TD-281) add Datasets logic, clean unused code serving as example
router! {
    state => { DatasetsState },
    paths => {{
        LIST_DATASETS => get(list_datasets),
    }}
}

#[concrete]
#[api_server_schema]
pub type ListResponseDataset = ListResponse<DatasetList>;
list_status!(ListResponseDataset);

const LIST_DATASETS: &str = DATASETS;
#[api_server_path(method = get, path = LIST_DATASETS, tag = FUNCTIONS_TAG)]
#[doc = "Lists functions of a collection"]
pub async fn list_datasets(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(collection_uri_params): Path<CollectionParam>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request: ListRequest<CollectionName> = context.list(collection_uri_params, query_params);
    let response = state.list_datasets().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
