//
// Copyright 2024 Tabs Data Inc.
//

//! Roles API Service for API Server.

#![allow(clippy::upper_case_acronyms)]

// use auto_utoipa::api_server_tag;
// use crate::bin::apisrv::api_server::RolesState;
// use crate::router;
// use axum::middleware::from_fn;

// pub const ROLES: &str = "/roles";
// pub const ROLE: &str = "/roles/{rid}";
//
// api_server_tag!(name = "Role", description = "Role Service for API Server");

// router! {
//     state => { RolesState },
//     paths => {
//         {
//             LIST_ROLES => get(list_roles),
//             GET_ROLE => get(get_role),
//             CREATE_ROLE => post(create_role),
//             UPDATE_ROLE => post(update_role),
//             DELETE_ROLE => delete(delete_role),
//         }
//         .layer => |_| from_fn(AdminOnly::layer),
//     }
// }

// #[derive(Deserialize, IntoParams, Getters)]
// #[getset(get = "pub")]
// pub struct RoleUriParams {
//     /// Role ID
//     rid: String,
// }
//
// #[concrete]
// #[api_server_schema]
// type ListResponseRole = ListResponse<RoleList>;
//
// list_status!(ListResponseRole);
//
// const LIST_ROLES: &str = ROLES;
// #[api_server_path(method = get, path = LIST_ROLES, tag = ROLE_TAG)]
// #[doc = "Lists all roles"]
// pub async fn list_roles(
//     State(roles_state): State<RolesState>,
//     Extension(context): Extension<RequestContext>,
//     Query(query_params): Query<ListParams>,
// ) -> Result<ListStatus, ListErrorStatus> {
//     let list_request = context.list(query_params);
//     match roles_state.list(list_request).await {
//         Ok(response) => Ok(ListStatus::OK(response.into())),
//         Err(error) => Err(error.into()),
//     }
// }
//
// get_status!(RoleRead);
//
// const GET_ROLE: &str = ROLE;
// #[api_server_path(method = get, path = GET_ROLE, tag = ROLE_TAG)]
// #[doc = "Get role by role ID"]
// pub async fn get_role(
//     State(roles_state): State<RolesState>,
//     Extension(context): Extension<RequestContext>,
//     Path(path_params): Path<RoleUriParams>,
// ) -> Result<GetStatus, GetErrorStatus> {
//     let get_request = context.read(path_params.rid());
//     match roles_state.read(get_request).await {
//         Ok(response) => Ok(GetStatus::OK(response)),
//         Err(error) => Err(error.into()),
//     }
// }
//
// create_status!(RoleRead);
//
// const CREATE_ROLE: &str = ROLES;
// #[api_server_path(method = post, path = CREATE_ROLE, tag = ROLE_TAG)]
// #[doc = "Create a new role"]
// pub async fn create_role(
//     State(roles_state): State<RolesState>,
//     Extension(context): Extension<RequestContext>,
//     Json(request): Json<RoleCreate>,
// ) -> Result<CreateStatus, CreateErrorStatus> {
//     let create_request = context.create(request);
//     match roles_state.create(create_request).await {
//         Ok(response) => Ok(CreateStatus::CREATED(response)),
//         Err(error) => Err(error.into()),
//     }
// }
//
// #[concrete]
// #[api_server_schema]
// type AddOrRemoveString = AddOrRemove<String>;
//
// #[concrete]
// #[api_server_schema]
// type AddOrRemovePermissionL = AddOrRemove<PermissionL>;
//
// #[concrete(into = RoleUpdate)]
// #[api_server_schema]
// pub type ConcreteRoleUpdate = GenericRoleUpdate<AddOrRemoveString, AddOrRemovePermissionL>;
// impl ApiData for ConcreteRoleUpdate {}
//
// update_status!(RoleRead);
//
// const UPDATE_ROLE: &str = ROLE;
// #[api_server_path(method = post, path = UPDATE_ROLE, tag = ROLE_TAG)]
// #[doc = "Update role by role ID"]
// pub async fn update_role(
//     State(roles_state): State<RolesState>,
//     Extension(context): Extension<RequestContext>,
//     Path(path_params): Path<RoleUriParams>,
//     Json(request): Json<ConcreteRoleUpdate>,
// ) -> Result<UpdateStatus, UpdateErrorStatus> {
//     let update_request = context.update(path_params.rid(), request.into());
//     match roles_state.update(update_request).await {
//         Ok(response) => Ok(UpdateStatus::OK(response)),
//         Err(error) => Err(error.into()),
//     }
// }
//
// delete_status!();
//
// const DELETE_ROLE: &str = ROLE;
// #[api_server_path(method = delete, path = DELETE_ROLE, tag = ROLE_TAG)]
// #[doc = "Delete role by role ID"]
// pub async fn delete_role(
//     State(roles_state): State<RolesState>,
//     Extension(context): Extension<RequestContext>,
//     Path(path_params): Path<RoleUriParams>,
// ) -> Result<DeleteStatus, DeleteErrorStatus> {
//     let delete_request = context.delete(path_params.rid());
//     match roles_state.delete(delete_request).await {
//         Ok(_) => Ok(DeleteStatus::OK(())),
//         Err(error) => Err(error.into()),
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//
//     use axum::body::{to_bytes, Body};
//     use axum::http::{Request, StatusCode};
//     use axum::Router;
//     use http::method::Method;
//     use serde_json::json;
//     use crate::common::xsql::DbPool;
//     use tower::ServiceExt;
//
//     use crate::common::sql::test_config;
//     use crate::logic::persistence::tabsdata;
//     use crate::logic::roles::RolesLogic;
//
//     use super::*;
//
//     async fn roles_state() -> RolesState {
//         let db: &'static DbPool =
//             Box::leak(Box::new(tabsdata::db(&test_config()).await.unwrap()));
//         let logic = RolesLogic::new(&db);
//         Arc::new(logic)
//     }
//
//     fn to_route(router: &Router) -> Router {
//         let context = RequestContext::with("", "", true);
//         router.clone().layer(Extension(context.clone()))
//     }
//
//     #[ignore] // TODO(TD-273) Roles and permissions in token
//     #[tokio::test]
//     async fn test_roles_lifecycle() {
//         let roles_state = roles_state().await;
//         let router = super::router(roles_state);
//
//         // List empty roles (only sysadmin will be there)
//         let response = to_route(&router)
//             .oneshot(
//                 Request::builder()
//                     .method(Method::GET)
//                     .uri(LIST_ROLES)
//                     .body(Body::empty())
//                     .unwrap(),
//             )
//             .await
//             .unwrap();
//         assert_eq!(response.status(), StatusCode::OK);
//
//         let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
//         let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
//         assert_eq!(body["len"], 1);
//
//         // Create a new role
//         let role_create = json!(
//             {
//                 "name": "joaquin",
//                 "description": "Joaquin role",
//             }
//         );
//
//         let response = to_route(&router)
//             .oneshot(
//                 Request::builder()
//                     .method(Method::POST)
//                     .uri(CREATE_ROLE)
//                     .header("content-type", "application/json")
//                     .body(serde_json::to_string(&role_create).unwrap())
//                     .unwrap(),
//             )
//             .await
//             .unwrap();
//         assert_eq!(response.status(), StatusCode::CREATED);
//
//         let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
//         let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
//         assert_eq!(body["name"], "joaquin");
//         assert_eq!(body["full_name"], "Joaquin");
//         assert_eq!(body["email"], "joaquin@tabsdata.com");
//
//         // List again and assert we have 2 roles
//         let response = to_route(&router)
//             .oneshot(
//                 Request::builder()
//                     .method(Method::GET)
//                     .uri(LIST_ROLES)
//                     .body(Body::empty())
//                     .unwrap(),
//             )
//             .await
//             .unwrap();
//         assert_eq!(response.status(), StatusCode::OK);
//
//         let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
//         let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
//         assert_eq!(body["len"], 2);
//
//         // Get the new role
//         let response = to_route(&router)
//             .oneshot(
//                 Request::builder()
//                     .method(Method::GET)
//                     .uri(GET_ROLE.replace("{uid}", "joaquin"))
//                     .body(Body::empty())
//                     .unwrap(),
//             )
//             .await
//             .unwrap();
//         assert_eq!(response.status(), StatusCode::OK);
//
//         let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
//         let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
//         assert_eq!(body["name"], "joaquin");
//         assert_eq!(body["full_name"], "Joaquin");
//         assert_eq!(body["email"], "joaquin@tabsdata.com");
//
//         // Update the new role
//         let role_update = json!(
//             {
//                 "full_name": "Mister Duck",
//                 "email": "quack@tabsdata.com"
//             }
//         );
//
//         let response = to_route(&router)
//             .oneshot(
//                 Request::builder()
//                     .method(Method::POST)
//                     .uri(UPDATE_ROLE.replace("{uid}", "joaquin"))
//                     .header("content-type", "application/json")
//                     .body(serde_json::to_string(&role_update).unwrap())
//                     .unwrap(),
//             )
//             .await
//             .unwrap();
//         assert_eq!(response.status(), StatusCode::OK);
//
//         let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
//         let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
//         assert_eq!(body["name"], "joaquin");
//         assert_eq!(body["full_name"], "Mister Duck");
//         assert_eq!(body["email"], "quack@tabsdata.com");
//
//         // Delete the new role
//         let response = to_route(&router)
//             .oneshot(
//                 Request::builder()
//                     .method(Method::DELETE)
//                     .uri(DELETE_ROLE.replace("{uid}", "joaquin"))
//                     .body(Body::empty())
//                     .unwrap(),
//             )
//             .await
//             .unwrap();
//         assert_eq!(response.status(), StatusCode::OK);
//
//         // List again and assert we are back to 1 role
//         let response = to_route(&router)
//             .oneshot(
//                 Request::builder()
//                     .method(Method::GET)
//                     .uri(LIST_ROLES)
//                     .body(Body::empty())
//                     .unwrap(),
//             )
//             .await
//             .unwrap();
//         assert_eq!(response.status(), StatusCode::OK);
//
//         let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
//         let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
//         assert_eq!(body["len"], 1);
//     }
// }
