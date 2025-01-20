//
// Copyright 2025. Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::data::DATA_TAG;
use crate::logic::apisrv::status::error_status::ListErrorStatus;
use crate::{list_status, router};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_concrete::concrete;
use td_objects::crudl::ListResponse;
use td_objects::crudl::ListResponseBuilder;
use td_objects::crudl::{ListParams, ListRequest, RequestContext};
use td_objects::datasets::dto::TableList;
use td_objects::rest_urls::{CollectionParam, TABLES_LIST};
use td_utoipa::{api_server_path, api_server_schema};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        TABLES_LIST => get(tables_list),
    }}
}

#[concrete]
#[api_server_schema]
pub type ListResponseTable = ListResponse<TableList>;
list_status!(ListResponseTable);

#[api_server_path(method = get, path = TABLES_LIST, tag = DATA_TAG)]
#[doc = "List current tables of a collection"]
pub async fn tables_list(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
    Query(query_params): Query<ListParams>,
) -> Result<ListStatus, ListErrorStatus> {
    let request: ListRequest<CollectionParam> = context.list(collection_param, query_params);
    let response = state.list_tables().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
