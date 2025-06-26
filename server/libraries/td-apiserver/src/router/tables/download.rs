//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::state::{StorageRef, Tables};
use crate::router::tables::TABLES_TAG;
use crate::status::error_status::GetErrorStatus;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Extension;
use axum_extra::extract::Query;
#[allow(unused_imports)] // needed for response macro but rustc warns as unused
use serde_json::json;
use td_apiforge::{apiserver_path, apiserver_schema};
use td_error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{AtTimeParam, TableParam, DOWNLOAD_TABLE};
use td_objects::types::table::TableAtIdName;
use td_tower::ctx_service::RawOneshot;
use utoipa::IntoResponses;

router! {
    state => { Tables, StorageRef },
    routes => { download }
}

/// This struct is just used to document ParquetFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[allow(dead_code)]
#[apiserver_schema]
#[derive(IntoResponses)]
#[response(
    status = 200,
    description = "OK",
    example = json!([]),
    content_type = "application/vnd.apache.parquet"
)]
pub struct ParquetFile(Vec<u8>);

#[apiserver_path(method = get, path = DOWNLOAD_TABLE, tag = TABLES_TAG, override_response = ParquetFile)]
#[doc = "Download a table as a parquet file"]
pub async fn download(
    State((tables, storage)): State<(Tables, StorageRef)>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<impl IntoResponse, GetErrorStatus> {
    let name = TableAtIdName::new(table_param, at_param);
    let request = context.read(name);
    let path = tables
        .table_download_service()
        .await
        .raw_oneshot(request)
        .await?;
    match path {
        Some(path) => {
            let stream = storage.read_stream(&path).await.map_err(TdError::from)?;
            Ok(Body::from_stream(stream))
        }
        None => Ok(Body::empty()),
    }
}
