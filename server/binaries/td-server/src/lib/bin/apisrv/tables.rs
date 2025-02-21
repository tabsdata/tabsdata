// //
// // Copyright 2024 Tabs Data Inc.
// //
//
// //! Tables API Service for API Server.
//
// #![allow(clippy::upper_case_acronyms)]
//
// use axum::extract::Path;
// use axum::routing::get;
// use getset::Getters;
// use serde::{Deserialize, Serialize};
// use utoipa::IntoParams;
//
// use crate::logic::apisrv::status::error_status::ListErrorStatus;
// use crate::{get_status, list_status, router};
// use td_apiforge::{api_server_path, api_server_schema, api_server_tag};
//
// pub const TABLES: &str = "/tables";
// pub const TABLE: &str = "/tables/{tid}";
//
// api_server_tag!(name = "Table", description = "Table Service for API Server");
//
// // TODO(TD-280) add Tables logic, clean unused code serving as example
// router! {
//     paths => {{
//         LIST_TABLES => get(list_tables),
//         GET_TABLE => get(get_table),
//     }}
// }
//
// #[derive(Deserialize, Getters, IntoParams)]
// #[getset(get = "pub")]
// pub struct TableUriParams {
//     #[allow(dead_code)]
//     /// Table ID
//     tid: String,
// }
//
// #[derive(Deserialize)]
// #[allow(dead_code)] // TODO remove this when used
// pub struct TableRequest {
//     name: String,
//     description: String,
// }
//
// #[api_server_schema]
// #[derive(Serialize)]
// #[allow(dead_code)] // TODO remove this when used
// pub struct TableResponse {
//     name: String,
//     description: String,
//     tid: String,
// }
//
// list_status!(TableResponse);
//
// const LIST_TABLES: &str = TABLES;
// #[api_server_path(method = get, path = LIST_TABLES, tag = TABLE_TAG)]
// #[doc = "Lists all tables"]
// pub async fn list_tables() -> Result<ListStatus, ListErrorStatus> {
//     Ok(ListStatus::OK(TableResponse {
//         name: "".to_string(),
//         description: "".to_string(),
//         tid: "".to_string(),
//     }))
// }
//
// get_status!(TableResponse);
//
// const GET_TABLE: &str = TABLE;
// #[api_server_path(method = get, path = GET_TABLE, tag = TABLE_TAG)]
// #[doc = "Get table by table ID"]
// pub async fn get_table(Path(_params): Path<TableUriParams>) -> Result<GetStatus, ListErrorStatus> {
//     Ok(GetStatus::OK(TableResponse {
//         name: "".to_string(),
//         description: "".to_string(),
//         tid: "".to_string(),
//     }))
// }
