//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::auth::AUTH_TAG;
use crate::router::state::Auth;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::RawStatus;
use axum::extract::State;
use axum::{Extension, Form};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::AUTH_REFRESH;
use td_objects::types::auth::{RefreshRequestX, TokenResponseX};
use tower::ServiceExt;

router! {
    state => { Auth },
    routes => { refresh }
}

#[apiserver_path(method = post, path = AUTH_REFRESH, tag = AUTH_TAG)]
#[doc = "Refresh Access Token"]
pub async fn refresh(
    State(state): State<Auth>,
    Extension(context): Extension<RequestContext>,
    Form(request): Form<RefreshRequestX>,
) -> Result<RawStatus<TokenResponseX>, ErrorStatus> {
    let request = context.update((), request.refresh_token().clone());
    let response = state.refresh_service().await.oneshot(request).await?;
    Ok(RawStatus::OK(response))
}
