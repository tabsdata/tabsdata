//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::auth::AUTH_TAG;
use crate::router::state::Auth;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::{NoContent, UpdateStatus};
use axum::extract::State;
use axum::Extension;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::AUTH_LOGOUT;
use tower::ServiceExt;

router! {
    state => { Auth },
    routes => { logout }
}

#[apiserver_path(method = post, path = AUTH_LOGOUT, tag = AUTH_TAG)]
#[doc = "User Logout"]
pub async fn logout(
    State(state): State<Auth>,
    Extension(context): Extension<RequestContext>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update((), ());
    let response = state.logout_service().await.oneshot(request).await?;
    Ok(UpdateStatus::OK(response))
}
