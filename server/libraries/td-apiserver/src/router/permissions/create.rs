//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Permissions;
use crate::status::error_status::ErrorStatus;
use crate::status::extractors::Json;
use crate::status::ok_status::CreateStatus;
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{CREATE_PERMISSION, RoleParam};
use td_objects::types::permission::{Permission, PermissionCreate};
use tower::ServiceExt;

router! {
    state => { Permissions },
    routes => { create_permission }
}

#[apiserver_path(method = post, path = CREATE_PERMISSION, tag = AUTHZ_TAG)]
#[doc = "Create a permission"]
pub async fn create_permission(
    State(state): State<Permissions>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<RoleParam>,
    Json(request): Json<PermissionCreate>,
) -> Result<CreateStatus<Permission>, ErrorStatus> {
    let request = context.create(role_param, request);
    let response = state.create_permission().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response))
}
