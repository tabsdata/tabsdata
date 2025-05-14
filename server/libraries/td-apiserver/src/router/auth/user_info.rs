//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::auth::AUTH_TAG;
use crate::router::state::Auth;
use crate::status::error_status::GetErrorStatus;
use axum::extract::State;
use axum::Extension;
use derive_builder::Builder;
use getset::Getters;
use serde::Serialize;
use td_apiforge::{apiserver_path, get_status};
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::AUTH_USER_INFO;
use td_objects::types::auth::UserInfo;
use td_tower::ctx_service::CtxMap;
use td_tower::ctx_service::CtxResponse;
use td_tower::ctx_service::CtxResponseBuilder;
use tower::ServiceExt;

router! {
    state => { Auth },
    routes => { user_info }
}

get_status!(UserInfo);

#[apiserver_path(method = get, path = AUTH_USER_INFO, tag = AUTH_TAG)]
#[doc = "User Info"]
pub async fn user_info(
    State(state): State<Auth>,
    Extension(context): Extension<RequestContext>,
) -> Result<GetStatus, GetErrorStatus> {
    let request = context.read(());
    let response = state.user_info_service().await.oneshot(request).await?;
    Ok(GetStatus::OK(response.into()))
}
