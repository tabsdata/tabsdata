//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apiserver::roles::ROLES_TAG;
use crate::bin::apiserver::RolesState;
use crate::logic::apiserver::status::error_status::CreateErrorStatus;
use crate::logic::apiserver::status::extractors::Json;
use crate::router;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, update_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RoleParam, UPDATE_ROLE};
use td_objects::types::role::{Role, RoleUpdate};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { RolesState },
    routes => { update_role }
}

update_status!(Role);

#[apiserver_path(method = post, path = UPDATE_ROLE, tag = ROLES_TAG)]
#[doc = "Update a role"]
pub async fn update_role(
    State(state): State<RolesState>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<RoleParam>,
    Json(request): Json<RoleUpdate>,
) -> Result<UpdateStatus, CreateErrorStatus> {
    let request = context.update(role_param, request);
    let response = state.update_role().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response.into()))
}
