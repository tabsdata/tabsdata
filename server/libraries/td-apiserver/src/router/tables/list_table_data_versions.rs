//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::state::Tables;
use crate::router::tables::TABLES_TAG;
use crate::status::error_status::GetErrorStatus;
use axum::extract::{Path, State};
use axum::Extension;
use axum_extra::extract::Query;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{apiserver_path, list_status};
use td_objects::crudl::ListResponseBuilder;
use td_objects::crudl::{ListParams, ListResponse, RequestContext};
use td_objects::rest_urls::{AtTimeParam, TableParam, LIST_TABLE_DATA_VERSIONS};
use td_objects::types::table::{TableAtName, TableDataVersion};
use td_tower::ctx_service::CtxMap;
use td_tower::ctx_service::CtxResponse;
use td_tower::ctx_service::CtxResponseBuilder;
use tower::ServiceExt;

router! {
    state => { Tables },
    routes => { list_table_data_versions }
}

list_status!(TableDataVersion);

#[apiserver_path(method = get, path = LIST_TABLE_DATA_VERSIONS, tag = TABLES_TAG)]
#[doc = "List data versions for a table"]
pub async fn list_table_data_versions(
    State(state): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<ListStatus, GetErrorStatus> {
    let name = TableAtName::new(table_param, at_param);
    let request = context.list(name, query_params);
    let response = state
        .list_table_data_versions_service()
        .await
        .oneshot(request)
        .await?;
    Ok(ListStatus::OK(response.into()))
}
