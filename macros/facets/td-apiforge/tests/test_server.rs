//
//  Copyright 2024 Tabs Data Inc.
//

#![allow(dead_code)]

use axum::extract::{Path, Query};
use axum::Json;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{api_server_path, api_server_schema, api_server_tag, get_status};
use utoipa::{IntoParams, IntoResponses};

api_server_tag!(name = "Test", description = "Test Service");

#[derive(Deserialize, IntoParams, Getters)]
#[getset(get = "pub")]
pub struct TestPathParams {
    /// Test ID
    tid: String,
}

#[derive(Deserialize, IntoParams, Getters)]
#[getset(get = "pub")]
pub struct TestQueryParams {
    /// Page number
    page: usize,
}

#[api_server_schema]
#[derive(Deserialize, Getters)]
#[getset(get = "pub")]
pub struct TestRequest {
    /// Request ID
    request_id: usize,
}

#[api_server_schema]
#[derive(Default, Serialize, Getters, IntoResponses)]
#[response(status = 200)]
#[getset(get = "pub")]
pub struct TestResponse {
    /// Response ID
    response_id: usize,
}

#[api_server_schema]
#[derive(Default, Serialize, Getters, IntoResponses)]
#[response(status = 500)]
#[getset(get = "pub")]
pub struct TestErrorResponse {
    /// Response ID
    error_id: usize,
}

// We mimic the CtxResponse and CtxMap struct from the td-tower crate,
// so we don't need to customize the td_concrete macros in ctx_macro_gen.
#[derive(Serialize, Builder, Getters)]
#[getset(get = "pub")]
pub struct CtxResponse<U> {
    version: String,
    context: CtxMap,
    data: U,
}

pub type CtxMap = String;

#[api_server_schema]
#[derive(Debug, Clone, Serialize)]
pub struct ConcreteResponse {
    response: String,
}

get_status!(ConcreteResponse);

pub const TEST_GET: &str = "/get";
#[api_server_path(method = get, path = TEST_GET, tag = TEST_TAG)]
#[doc = "Get test"]
pub async fn test_get(
    Path(_path_params): Path<TestPathParams>,
    Query(_query_params): Query<TestQueryParams>,
    Json(_request): Json<TestRequest>,
) -> Result<TestResponse, TestErrorResponse> {
    Ok(TestResponse::default())
}

pub const TEST_POST: &str = "/post";
#[api_server_path(method = post, path = TEST_POST, tag = TEST_TAG)]
#[doc = "Post test"]
pub async fn test_post(
    Path(_path_params): Path<TestPathParams>,
    Query(_query_params): Query<TestQueryParams>,
    Json(_request): Json<TestRequest>,
) -> Result<TestResponse, TestErrorResponse> {
    Ok(TestResponse::default())
}
