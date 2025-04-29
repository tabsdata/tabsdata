//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::InterCollectionPermissions;
use crate::status::error_status::CreateErrorStatus;
use crate::status::extractors::Json;
use axum::extract::{Path, State};
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, create_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{CollectionParam, CREATE_INTER_COLLECTION_PERMISSION};
use td_objects::types::permission::{InterCollectionPermission, InterCollectionPermissionCreate};
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { InterCollectionPermissions },
    routes => { create_permission }
}

create_status!(InterCollectionPermission);

#[apiserver_path(method = post, path = CREATE_INTER_COLLECTION_PERMISSION, tag = AUTHZ_TAG)]
#[doc = "Create an inter collection permission"]
pub async fn create_permission(
    State(state): State<InterCollectionPermissions>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<CollectionParam>,
    Json(request): Json<InterCollectionPermissionCreate>,
) -> Result<CreateStatus, CreateErrorStatus> {
    let request = context.create(role_param, request);
    let response = state.create_permission().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response.into()))
}
