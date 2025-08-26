//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::Roles;
use crate::status::error_status::ErrorStatus;
use crate::status::extractors::Json;
use crate::status::ok_status::CreateStatus;
use axum::Extension;
use axum::extract::State;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CREATE_ROLE;
use td_objects::types::role::{Role, RoleCreate};
use tower::ServiceExt;

router! {
    state => { Roles },
    routes => { create_role }
}

#[apiserver_path(method = post, path = CREATE_ROLE, tag = AUTHZ_TAG)]
#[doc = "Create a role"]
pub async fn create_role(
    State(state): State<Roles>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<RoleCreate>,
) -> Result<CreateStatus<Role>, ErrorStatus> {
    let request = context.create((), request);
    let response = state.create_role().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response))
}
