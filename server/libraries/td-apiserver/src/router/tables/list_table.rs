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
use td_objects::rest_urls::{AtMultiParam, CollectionParam, LIST_TABLES};
use td_objects::types::table::{CollectionAtName, Table};
use td_tower::ctx_service::CtxMap;
use td_tower::ctx_service::CtxResponse;
use td_tower::ctx_service::CtxResponseBuilder;
use tower::ServiceExt;

router! {
    state => { Tables },
    routes => { list_table }
}

list_status!(Table);

#[apiserver_path(method = get, path = LIST_TABLES, tag = TABLES_TAG)]
#[doc = "List tables for a collection"]
pub async fn list_table(
    State(state): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtMultiParam>,
) -> Result<ListStatus, GetErrorStatus> {
    let name = CollectionAtName::new(collection_param, at_param);
    let request = context.list(name, query_params);
    let response = state.list_table_service().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
