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
use serde::Serialize;
use td_apiforge::{apiserver_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{AtMultiParam, TableParam, SCHEMA_TABLE};
use td_objects::types::table::{TableAtName, TableSchema};
use td_tower::ctx_service::CtxMap;
use td_tower::ctx_service::CtxResponse;
use td_tower::ctx_service::CtxResponseBuilder;
use tower::ServiceExt;

router! {
    state => { Tables },
    routes => { schema }
}

get_status!(TableSchema);

#[apiserver_path(method = get, path = SCHEMA_TABLE, tag = TABLES_TAG)]
#[doc = "Get the schema of a table"]
pub async fn schema(
    State(state): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(at_param): Query<AtMultiParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let name = TableAtName::new(table_param, at_param);
    let request = context.read(name);
    let response = state.table_schema_service().await.oneshot(request).await?;
    Ok(GetStatus::OK(response.into()))
}
