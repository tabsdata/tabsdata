//
// Copyright 2025. Tabs Data Inc.
//
//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::InterCollectionPermissions;
use crate::status::error_status::GetErrorStatus;
use crate::status::DeleteStatus;
use axum::extract::{Path, State};
use axum::Extension;
// use derive_builder::Builder;
// use getset::Getters;
// use serde::Serialize;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{InterCollectionPermissionParam, DELETE_INTER_COLLECTION_PERMISSION};
//use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};
use tower::ServiceExt;

router! {
    state => { InterCollectionPermissions },
    routes => { delete_permission }
}

#[apiserver_path(method = delete, path = DELETE_INTER_COLLECTION_PERMISSION, tag = AUTHZ_TAG)]
#[doc = "Delete an inter collection permission"]
pub async fn delete_permission(
    State(state): State<InterCollectionPermissions>,
    Extension(context): Extension<RequestContext>,
    Path(param): Path<InterCollectionPermissionParam>,
) -> Result<DeleteStatus, GetErrorStatus> {
    let request = context.delete(param);
    let response = state.delete_permission().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response.into()))
}
