//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::ROLES_TAG;
use crate::router::state::Permissions;
use crate::status::error_status::CreateErrorStatus;
use crate::status::extractors::Json;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, create_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{RoleParam, CREATE_PERMISSION};
use td_objects::types::permission::{Permission, PermissionCreate};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { Permissions },
    routes => { create_permission }
}

create_status!(Permission);

#[apiserver_path(method = post, path = CREATE_PERMISSION, tag = ROLES_TAG)]
#[doc = "Create a permission"]
pub async fn create_permission(
    State(state): State<Permissions>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<RoleParam>,
    Json(request): Json<PermissionCreate>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(role_param, request);
    let response = state.create_permission().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}
