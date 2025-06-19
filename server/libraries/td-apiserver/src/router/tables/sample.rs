//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::state::Tables;
use crate::router::tables::TABLES_TAG;
use crate::status::error_status::GetErrorStatus;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Extension;
use axum_extra::extract::Query;
use td_apiforge::{apiserver_path, apiserver_schema};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{
    AtTimeParam, FileFormatParam, SampleOffsetLenParam, TableParam, SAMPLE_TABLE,
};
use td_objects::types::table::TableSampleAtName;
use td_tower::ctx_service::IntoData;
use tower::ServiceExt;
use utoipa::IntoResponses;

router! {
    state => { Tables },
    routes => { sample }
}

/// This struct is just used to document ParquetFile in the OpenAPI schema.
/// The server is just returning a stream of bytes, so we need to specify the content type.
#[allow(dead_code)]
#[apiserver_schema]
#[derive(IntoResponses)]
#[response(status = 200, description = "OK", content_type = "text/csv")]
pub struct CsvFile(Vec<u8>);

#[apiserver_path(method = get, path = SAMPLE_TABLE, tag = TABLES_TAG, override_response = CsvFile)]
#[doc = "Get a sample of a table as CSV"]
pub async fn sample(
    State(tables): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Path(table_param): Path<TableParam>,
    Query(at_param): Query<AtTimeParam>,
    Query(offset_len_param): Query<SampleOffsetLenParam>,
    Query(file_format_param): Query<FileFormatParam>,
) -> Result<impl IntoResponse, GetErrorStatus> {
    let name = TableSampleAtName::new(table_param, at_param, offset_len_param, file_format_param);
    let request = context.read(name);
    let sample = tables.table_sample_service().await.oneshot(request).await?;
    let stream = sample.into_data();
    Ok(Body::from_stream(stream.into_inner()))
}
