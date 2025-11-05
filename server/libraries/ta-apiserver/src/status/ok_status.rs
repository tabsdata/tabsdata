//
// Copyright 2025 Tabs Data Inc.
//

#![allow(clippy::upper_case_acronyms)]

use serde::Serialize;
use td_objects::dxo::crudl::ListResponse;
use td_tower::ctx_service::CtxResponse;
use utoipa::__dev::ComposeSchema;
use utoipa::ToSchema;

pub type NoContent = ();

// used so openapi docs get generated properly for raw types
pub type ByPass<T> = T;

#[derive(utoipa::ToSchema, utoipa::IntoResponses, serde::Serialize)]
pub enum RawStatus<T>
where
    T: ToSchema + ComposeSchema,
{
    #[response(status = StatusCode::OK, description = "OK")]
    OK(ByPass<T>),
}

impl<T> axum::response::IntoResponse for RawStatus<T>
where
    T: Serialize + ToSchema + ComposeSchema,
{
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::OK(response) => (
                http::StatusCode::OK,
                axum::Json(serde_json::json!(response)),
            )
                .into_response(),
        }
    }
}

#[derive(utoipa::ToSchema, utoipa::IntoResponses, serde::Serialize)]
pub enum ListStatus<T>
where
    T: Clone + ToSchema,
{
    #[response(status = StatusCode::OK, description = "OK")]
    OK(CtxResponse<ListResponse<T>>),
}

impl<T> axum::response::IntoResponse for ListStatus<T>
where
    T: Clone + ToSchema + Serialize,
{
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::OK(response) => (
                http::StatusCode::OK,
                axum::Json(serde_json::json!(response)),
            )
                .into_response(),
        }
    }
}

#[derive(utoipa::ToSchema, utoipa::IntoResponses, serde::Serialize)]
pub enum GetStatus<T>
where
    T: ToSchema,
{
    #[response(status = StatusCode::OK, description = "OK")]
    OK(CtxResponse<T>),
}

impl<T> axum::response::IntoResponse for GetStatus<T>
where
    T: Serialize + ToSchema,
{
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::OK(response) => (
                http::StatusCode::OK,
                axum::Json(serde_json::json!(response)),
            )
                .into_response(),
        }
    }
}

#[derive(utoipa::ToSchema, utoipa::IntoResponses, serde::Serialize)]
pub enum CreateStatus<T>
where
    T: ToSchema,
{
    #[response(status = StatusCode::CREATED, description = "OK")]
    CREATED(CtxResponse<T>),
}

impl<T> axum::response::IntoResponse for CreateStatus<T>
where
    T: Serialize + ToSchema,
{
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::CREATED(response) => (
                http::StatusCode::CREATED,
                axum::Json(serde_json::json!(response)),
            )
                .into_response(),
        }
    }
}

#[derive(utoipa::ToSchema, utoipa::IntoResponses, serde::Serialize)]
pub enum UpdateStatus<T>
where
    T: ToSchema,
{
    #[response(status = StatusCode::OK, description = "OK")]
    OK(CtxResponse<T>),
}

impl<T> axum::response::IntoResponse for UpdateStatus<T>
where
    T: Serialize + ToSchema,
{
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::OK(response) => (
                http::StatusCode::OK,
                axum::Json(serde_json::json!(response)),
            )
                .into_response(),
        }
    }
}

#[derive(utoipa::ToSchema, utoipa::IntoResponses, serde::Serialize)]
pub enum DeleteStatus<T>
where
    T: ToSchema,
{
    #[response(status = StatusCode::OK, description = "OK")]
    OK(CtxResponse<T>),
}

impl<T> axum::response::IntoResponse for DeleteStatus<T>
where
    T: Serialize + ToSchema,
{
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::OK(response) => (
                http::StatusCode::OK,
                axum::Json(serde_json::json!(response)),
            )
                .into_response(),
        }
    }
}
