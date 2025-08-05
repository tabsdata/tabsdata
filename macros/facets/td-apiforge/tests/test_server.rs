//
//  Copyright 2024 Tabs Data Inc.
//

#![allow(dead_code)]

use axum::extract::{Path, Query};
use axum::Json;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_apiforge::{apiserver_path, apiserver_schema, apiserver_tag};
use utoipa::{IntoParams, IntoResponses};

apiserver_tag!(name = "Test", description = "Test Service");

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

#[apiserver_schema]
#[derive(Deserialize, Getters)]
#[getset(get = "pub")]
pub struct TestRequest {
    /// Request ID
    request_id: usize,
}

#[apiserver_schema]
#[derive(Default, Serialize, Getters, IntoResponses)]
#[response(status = 200)]
#[getset(get = "pub")]
pub struct TestResponse {
    /// Response ID
    response_id: usize,
}

#[apiserver_schema]
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

#[apiserver_schema]
#[derive(Debug, Clone, Serialize)]
pub struct ConcreteResponse {
    response: String,
}

pub const TEST_GET: &str = "/get";
#[apiserver_path(method = get, path = TEST_GET, tag = TEST_TAG)]
#[doc = "Get test"]
pub async fn test_get(
    Path(_path_params): Path<TestPathParams>,
    Query(_query_params): Query<TestQueryParams>,
    Json(_request): Json<TestRequest>,
) -> Result<TestResponse, TestErrorResponse> {
    Ok(TestResponse::default())
}

pub const TEST_POST: &str = "/post";
#[apiserver_path(method = post, path = TEST_POST, tag = TEST_TAG)]
#[doc = "Post test"]
pub async fn test_post(
    Path(_path_params): Path<TestPathParams>,
    Query(_query_params): Query<TestQueryParams>,
    Json(_request): Json<TestRequest>,
) -> Result<TestResponse, TestErrorResponse> {
    Ok(TestResponse::default())
}
