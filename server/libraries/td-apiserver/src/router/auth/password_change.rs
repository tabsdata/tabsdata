//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::auth::AUTH_TAG;
use crate::router::state::Auth;
use crate::status::error_status::UpdateErrorStatus;
use crate::status::extractors::Json;
use crate::status::EmptyUpdateStatus;
use axum::extract::State;
use td_apiforge::apiserver_path;
use td_objects::rest_urls::AUTH_PASSWORD_CHANGE;
use td_objects::types::auth::PasswordChange;
use tower::ServiceExt;

router! {
    state => { Auth },
    routes => { password_change }
}

#[apiserver_path(method = post, path = AUTH_PASSWORD_CHANGE, tag = AUTH_TAG)]
#[doc = "Password change"]
pub async fn password_change(
    State(state): State<Auth>,
    Json(request): Json<PasswordChange>,
) -> Result<EmptyUpdateStatus, UpdateErrorStatus> {
    let response = state
        .password_change_service()
        .await
        .oneshot(request)
        .await?;
    // incorrect_role
    // user disabled
    // unauthorized
    Ok(EmptyUpdateStatus::OK(response.into()))
}
