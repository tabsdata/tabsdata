//
//  Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::{DatasetsState, StorageState};
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
use td_common::error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{AtParam, TableCommitParam, TableParam, TABLE_DATA};
use td_utoipa::api_server_path;
use tower::ServiceExt;

router! {
    state => { DatasetsState, StorageState },
    paths => {{
        TABLE_DATA => get(get_data),
    }}
}

#[api_server_path(method = get, path = TABLE_DATA, tag = DATA_TAG, override_response = ParquetFile)]
#[doc = "Get the data of a table for a given version. The version can be a fixed \
version or a relative one (HEAD, HEAD^ and HEAD~## syntax)."]
pub async fn get_data(
    State((state, storage)): State<(DatasetsState, StorageState)>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(at_param): Query<AtParam>,
) -> Result<impl IntoResponse, GetErrorStatus> {
    let table_commit = TableCommitParam::new(&table_param, &at_param)?;
    let request = context.read(table_commit);
    let path = state
        .data()
        .await
        .oneshot(request)
        .await
        .map_err(TdError::from)?;
    let stream = storage.read_stream(&path).await.map_err(TdError::from)?;
    Ok(Body::from_stream(stream))
}
