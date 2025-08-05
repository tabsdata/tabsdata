//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::UserRoles;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::extract::{Path, State};
use axum::Extension;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{RoleParam, LIST_USER_ROLES};
use td_objects::types::role::UserRole;
use tower::ServiceExt;

router! {
    state => { UserRoles },
    routes => { list_user_role }
}

#[apiserver_path(method = get, path = LIST_USER_ROLES, tag = AUTHZ_TAG)]
#[doc = "List users for a role"]
pub async fn list_user_role(
    State(state): State<UserRoles>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Path(role_param): Path<RoleParam>,
) -> Result<ListStatus<UserRole>, ErrorStatus> {
    let request = context.list(role_param, query_params);
    let response = state.list_user_roles().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
