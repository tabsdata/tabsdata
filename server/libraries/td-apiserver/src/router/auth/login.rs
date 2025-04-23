//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::auth::{AuthStatusRaw, AUTH_TAG};
use crate::router::AuthState;
use crate::status::error_status::AuthorizeErrorStatus;
use crate::status::extractors::Json;
use axum::extract::State;
use td_apiforge::apiserver_path;
use td_objects::rest_urls::AUTH_LOGIN;
use td_objects::types::auth::Login;
use td_tower::ctx_service::IntoData;
use tower::ServiceExt;

router! {
    state => { AuthState },
    routes => { login }
}

#[apiserver_path(method = post, path = AUTH_LOGIN, tag = AUTH_TAG)]
#[doc = "User Login"]
pub async fn login(
    State(state): State<AuthState>,
    Json(request): Json<Login>,
) -> Result<AuthStatusRaw, AuthorizeErrorStatus> {
    let response = state.login_service().await.oneshot(request).await?;
    Ok(AuthStatusRaw::OK(response.into_data()))
}
