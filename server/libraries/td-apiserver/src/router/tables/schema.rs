//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::state::Tables;
use crate::router::tables::TABLES_TAG;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::GetStatus;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{AtTimeParam, SCHEMA_TABLE, TableParam};
use td_objects::types::table::{TableAtIdName, TableSchema};
use tower::ServiceExt;

router! {
    state => { Tables },
    routes => { schema }
}

#[apiserver_path(method = get, path = SCHEMA_TABLE, tag = TABLES_TAG)]
#[doc = "Get the schema of a table"]
pub async fn schema(
    State(state): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<GetStatus<TableSchema>, ErrorStatus> {
    let name = TableAtIdName::new(table_param, at_param);
    let request = context.read(name);
    let response = state.table_schema_service().await.oneshot(request).await?;
    Ok(GetStatus::OK(response))
}
