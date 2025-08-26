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
use td_objects::rest_urls::{AtTimeParam, LIST_TABLE_DATA_VERSIONS, TableParam};
use td_objects::types::execution::TableDataVersion;
use td_objects::types::table::TableAtIdName;
use tower::ServiceExt;

router! {
    state => { Tables },
    routes => { list_table_data_versions }
}

#[apiserver_path(method = get, path = LIST_TABLE_DATA_VERSIONS, tag = TABLES_TAG)]
#[doc = "List data versions for a table"]
pub async fn list_table_data_versions(
    State(state): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<ListStatus<TableDataVersion>, ErrorStatus> {
    let name = TableAtIdName::new(table_param, at_param);
    let request = context.list(name, query_params);
    let response = state
        .list_table_data_versions_service()
        .await
        .oneshot(request)
        .await?;
    Ok(ListStatus::OK(response))
}
