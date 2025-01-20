//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::bin::apisrv::data::{ParquetFile, DATA_TAG};
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Extension;
#[allow(unused_imports)]
use serde_json::json;
use std::vec::Vec;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{AtParam, TableCommitParam, TableParam, TABLE_SAMPLE};
use td_utoipa::api_server_path;
use tower::ServiceExt;

router! {
    state => { DatasetsState },
    paths => {{
        TABLE_SAMPLE => get(get_sample),
    }}
}

#[api_server_path(method = get, path = TABLE_SAMPLE, tag = DATA_TAG, override_response = ParquetFile)]
#[doc = "Get a sample of a table for a given version. The version can be a fixed \
version or a relative one (HEAD, HEAD^ and HEAD~## syntax)."]
pub async fn get_sample(
    State(state): State<DatasetsState>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(at_param): Query<AtParam>,
    Query(list_params): Query<ListParams>,
) -> Result<impl IntoResponse, GetErrorStatus> {
    let table_commit = TableCommitParam::new(&table_param, &at_param)?;
    let request = context.list(table_commit, list_params);
    let response = state.sample().await.oneshot(request).await?;
    Ok(Body::from_stream(response.into_inner()))
}
