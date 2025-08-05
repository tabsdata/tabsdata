//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::roles::AUTHZ_TAG;
use crate::router::state::InterCollectionPermissions;
use crate::status::error_status::ErrorStatus;
use crate::status::extractors::Json;
use crate::status::ok_status::CreateStatus;
use axum::extract::{Path, State};
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{CollectionParam, CREATE_INTER_COLLECTION_PERMISSION};
use td_objects::types::permission::{InterCollectionPermission, InterCollectionPermissionCreate};
use tower::ServiceExt;

router! {
    state => { InterCollectionPermissions },
    routes => { create_inter_collection_permission }
}

#[apiserver_path(method = post, path = CREATE_INTER_COLLECTION_PERMISSION, tag = AUTHZ_TAG)]
#[doc = "Create an inter collection permission"]
pub async fn create_inter_collection_permission(
    State(state): State<InterCollectionPermissions>,
    Extension(context): Extension<RequestContext>,
    Path(role_param): Path<CollectionParam>,
    Json(request): Json<InterCollectionPermissionCreate>,
) -> Result<CreateStatus<InterCollectionPermission>, ErrorStatus> {
    let request = context.create(role_param, request);
    let response = state.create_permission().await.oneshot(request).await?;
    Ok(CreateStatus::CREATED(response))
}
