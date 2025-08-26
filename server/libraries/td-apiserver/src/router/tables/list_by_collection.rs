//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::state::Tables;
use crate::router::tables::TABLES_TAG;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{AtTimeParam, CollectionParam, LIST_TABLES_BY_COLL};
use td_objects::types::table::{CollectionAtName, Table};
use tower::ServiceExt;

router! {
    state => { Tables },
    routes => { list_table_by_collection }
}

#[apiserver_path(method = get, path = LIST_TABLES_BY_COLL, tag = TABLES_TAG)]
#[doc = "List tables for a collection"]
pub async fn list_table_by_collection(
    State(state): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Path(collection_param): Path<CollectionParam>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<ListStatus<Table>, ErrorStatus> {
    let name = CollectionAtName::new(collection_param, at_param);
    let request = context.list(name, query_params);
    let response = state
        .list_table_by_collection_service()
        .await
        .oneshot(request)
        .await?;
    Ok(ListStatus::OK(response))
}
