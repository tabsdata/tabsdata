//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::auth::AUTH_TAG;
use crate::router::state::Auth;
use crate::status::error_status::ErrorStatus;
use crate::status::extractors::Json;
use crate::status::ok_status::RawStatus;
use axum::Extension;
use axum::extract::State;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::AUTH_ROLE_CHANGE;
use td_objects::types::auth::{RoleChange, TokenResponseX};
use tower::ServiceExt;

router! {
    state => { Auth },
    routes => { role_change }
}

#[apiserver_path(method = post, path = AUTH_ROLE_CHANGE, tag = AUTH_TAG)]
#[doc = "Role change"]
pub async fn role_change(
    State(state): State<Auth>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<RoleChange>,
) -> Result<RawStatus<TokenResponseX>, ErrorStatus> {
    let request = context.update((), request);
    let response = state.role_change_service().await.oneshot(request).await?;
    Ok(RawStatus::OK(response))
}
