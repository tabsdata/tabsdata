//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::data::DATA_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::router;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
#[allow(unused_imports)]
use serde_json::json;
use std::vec::Vec;
use td_apiforge::{api_server_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::datasets::dto::SchemaField;
use td_objects::rest_urls::{AtParam, TableCommitParam, TableParam, TABLE_SCHEMA};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        TABLE_SCHEMA => get(get_schema),
    }}
}

type Schema = Vec<SchemaField>;
get_status!(Schema);

#[api_server_path(method = get, path = TABLE_SCHEMA, tag = DATA_TAG)]
#[doc = "Get the schema of a table for a given version. The version can be a fixed \
version or a relative one (HEAD, HEAD^ and HEAD~## syntax)."]
pub async fn get_schema(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(at_param): Query<AtParam>,
) -> Result<GetStatus, GetErrorStatus> {
    let table_commit = TableCommitParam::new(&table_param, &at_param)?;
    let request = context.read(table_commit);
    let schema = state.schema().await.oneshot(request).await?;
    Ok(GetStatus::OK(schema.into()))
}
