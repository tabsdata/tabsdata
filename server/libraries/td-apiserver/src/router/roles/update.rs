//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Roles;
use crate::status::error_status::ErrorStatus;
use crate::status::extractors::Json;
use crate::status::ok_status::UpdateStatus;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RoleParam, UPDATE_ROLE};
use td_objects::types::role::{Role, RoleUpdate};
use tower::ServiceExt;

router! {
    state => { Roles },
    routes => { update_role }
}

#[apiserver_path(method = post, path = UPDATE_ROLE, tag = AUTHZ_TAG)]
#[doc = "Update a role"]
pub async fn update_role(
    State(state): State<Roles>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<RoleParam>,
    Json(request): Json<RoleUpdate>,
) -> Result<UpdateStatus<Role>, ErrorStatus> {
    let request = context.update(role_param, request);
    let response = state.update_role().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response))
}
