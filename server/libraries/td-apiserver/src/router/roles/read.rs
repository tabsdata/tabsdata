//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Roles;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::GetStatus;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RoleParam, GET_ROLE};
use td_objects::types::role::Role;
use tower::ServiceExt;

router! {
    state => { Roles },
    routes => { read_role }
}

#[apiserver_path(method = get, path = GET_ROLE, tag = AUTHZ_TAG)]
#[doc = "Read a role"]
pub async fn read_role(
    State(state): State<Roles>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<RoleParam>,
) -> Result<GetStatus<Role>, ErrorStatus> {
    let request = context.read(role_param);
    let response = state.read_role().await.oneshot(request).await?;
    Ok(GetStatus::OK(response))
}
