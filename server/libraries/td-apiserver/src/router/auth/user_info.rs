//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::auth::AUTH_TAG;
use crate::router::state::Auth;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::GetStatus;
use axum::Extension;
use axum::extract::State;
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::AUTH_USER_INFO;
use td_objects::types::auth::UserInfo;
use tower::ServiceExt;

router! {
    state => { Auth },
    routes => { user_info }
}

#[apiserver_path(method = get, path = AUTH_USER_INFO, tag = AUTH_TAG)]
#[doc = "User Info"]
pub async fn user_info(
    State(state): State<Auth>,
    Extension(context): Extension<RequestContext>,
) -> Result<GetStatus<UserInfo>, ErrorStatus> {
    let request = context.read(());
    let response = state.user_info_service().await.oneshot(request).await?;
    Ok(GetStatus::OK(response))
}
