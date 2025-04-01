//
// Copyright 2025. Tabs Data Inc.
//

use crate::bin::apiserver::auth::{AuthStatusRaw, AUTH_TAG};
use crate::bin::apiserver::AuthState;
use crate::logic::apiserver::status::error_status::AuthorizeErrorStatus;
use crate::logic::apiserver::status::extractors::Json;
use crate::router;
use axum::extract::State;
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::AUTH_ROLE_CHANGE;
use td_objects::types::auth::RoleChange;
use td_tower::ctx_service::IntoData;
use tower::ServiceExt;

router! {
    state => { AuthState },
    routes => { role_change }
}

#[apiserver_path(method = post, path = AUTH_ROLE_CHANGE, tag = AUTH_TAG)]
#[doc = "Role change"]
pub async fn role_change(
    State(state): State<AuthState>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<RoleChange>,
) -> Result<AuthStatusRaw, AuthorizeErrorStatus> {
    let request = context.update((), request);
    let response = state.role_change_service().await.oneshot(request).await?;
    Ok(AuthStatusRaw::OK(response.into_data()))
}
