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
    Extension(context): Extension<RequestContext>,
    Json(request): Json<Login>,
) -> Result<AuthStatusRaw, AuthorizeErrorStatus> {
    let request = context.create((), request);
    let response = state.login_service().await.oneshot(request).await?;
    Ok(AuthStatusRaw::OK(response.into_data()))
}
