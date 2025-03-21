//
// Copyright 2025 Tabs Data Inc.
//

//! Dataset API Service for API Server.

pub mod get_data;
pub mod list_versions;
pub mod sample;
pub mod schema;
pub mod tables;

use crate::bin::apiserver::{DatasetsState, StorageState};
use crate::routers;
#[allow(unused_imports)]
use serde_json::json;
use td_apiforge::{apiserver_schema, apiserver_tag};
use utoipa::IntoResponses;

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

apiserver_tag!(name = "Data", description = "Data API");

routers! {
    state => { DatasetsState, StorageState },
    router => {
        tables => { state ( DatasetsState ) },
        list_versions => { state ( DatasetsState ) },
        get_data => { state ( DatasetsState, StorageState ) },
        schema => { state ( DatasetsState ) },
        sample => { state ( DatasetsState ) },
    }
}
