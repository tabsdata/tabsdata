//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::UserRolesState;
use crate::bin::apisrv::roles::ROLES_TAG;
use crate::logic::apisrv::status::error_status::GetErrorStatus;
use crate::router;
use axum::extract::{Path, Query, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use serde::Serialize;
use td_apiforge::{api_server_path, list_status};
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::crudl::{ListResponse, ListResponseBuilder};
use td_objects::rest_urls::{RoleParam, LIST_USER_ROLES};
use td_objects::types::role::UserRole;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { UserRolesState },
    routes => { list_user_role }
}

list_status!(UserRole);

#[api_server_path(method = get, path = LIST_USER_ROLES, tag = ROLES_TAG)]
#[doc = "List users for a role"]
pub async fn list_user_role(
    State(state): State<UserRolesState>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Path(role_param): Path<RoleParam>,
) -> Result<ListStatus, GetErrorStatus> {
    let request = context.list(role_param, query_params);
    let response = state.list_user_roles().await.oneshot(request).await?;
    Ok(ListStatus::OK(response.into()))
}
