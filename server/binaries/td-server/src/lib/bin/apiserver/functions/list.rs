//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apiserver::functions::{DATASETS, FUNCTIONS_TAG};
use crate::bin::apiserver::DatasetsState;
use crate::logic::apiserver::status::error_status::ListErrorStatus;
use crate::router;
use axum::extract::{Path, Query, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::ListResponse;
use td_objects::crudl::ListResponseBuilder;
use td_objects::crudl::{ListParams, ListRequest, RequestContext};
use td_objects::datasets::dto::DatasetList;
use td_objects::dlo::CollectionName;
use td_objects::rest_urls::CollectionParam;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

// TODO(TD-281) add Datasets logic, clean unused code serving as example
router! {
    state => { DatasetsState },
    routes => { list_datasets }
}

list_status!(DatasetList);

const LIST_DATASETS: &str = DATASETS;
#[apiserver_path(method = get, path = LIST_DATASETS, tag = FUNCTIONS_TAG)]
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
