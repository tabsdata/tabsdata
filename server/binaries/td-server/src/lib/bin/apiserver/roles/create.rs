//
// Copyright 2025 Tabs Data Inc.
//

use crate::bin::apiserver::roles::ROLES_TAG;
use crate::bin::apiserver::RolesState;
use crate::logic::apiserver::status::error_status::CreateErrorStatus;
use crate::logic::apiserver::status::extractors::Json;
use crate::router;
use axum::extract::State;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, create_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CREATE_ROLE;
use td_objects::types::role::{Role, RoleCreate};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { RolesState },
    routes => { create_role }
}

create_status!(Role);

#[apiserver_path(method = post, path = CREATE_ROLE, tag = ROLES_TAG)]
#[doc = "Create a role"]
pub async fn create_role(
    State(state): State<RolesState>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<RoleCreate>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create((), request);
    let response = state.create_role().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}
