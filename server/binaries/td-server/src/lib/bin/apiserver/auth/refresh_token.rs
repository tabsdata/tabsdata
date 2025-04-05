//
// Copyright 2025. Tabs Data Inc.
//

use crate::bin::apiserver::auth::{AuthStatusRaw, AUTH_TAG};
use crate::bin::apiserver::AuthState;
use crate::logic::apiserver::status::error_status::AuthorizeErrorStatus;
use crate::router;
use axum::extract::State;
use axum::{Extension, Form};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::AUTH_REFRESH;
use td_objects::types::auth::RefreshRequestX;
use td_tower::ctx_service::IntoData;
use tower::ServiceExt;

router! {
    state => { AuthState },
    routes => { refresh }
}

#[apiserver_path(method = post, path = AUTH_REFRESH, tag = AUTH_TAG)]
#[doc = "Refresh Access Token"]
pub async fn refresh(
    State(state): State<AuthState>,
    Extension(context): Extension<RequestContext>,
    Form(request): Form<RefreshRequestX>,
) -> Result<AuthStatusRaw, AuthorizeErrorStatus> {
    let request = context.update((), request.refresh_token().clone());

    let response = state.refresh_service().await.oneshot(request).await?;
    Ok(AuthStatusRaw::OK(response.into_data()))
}
