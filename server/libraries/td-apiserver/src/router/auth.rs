//
// Copyright 2025 Tabsdata Inc.
//

const AUTH_TAG: &str = "Authentication";

use axum::extract::State;
use axum::{Extension, Form};
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{GetStatus, NoContent, RawStatus, UpdateStatus};
use td_apiforge::apiserver_path;
use td_objects::dxo::auth::{Login, PasswordChange, RefreshRequest, TokenResponse};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::file::DownloadResponse;
use td_objects::dxo::user::UserInfo;
use td_services::Services;

#[apiserver_path(
    mount(global),
    method(post),
    path = "/auth/logout",
    tag(AUTH_TAG),
    doc = "User Logout"
)]
pub async fn logout(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let response = state.logout(context).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    mount(global),
    method(post),
    path = "/auth/refresh",
    tag(AUTH_TAG),
    doc = "Refresh Access Token"
)]
pub async fn refresh(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Form(request): Form<RefreshRequest>,
) -> Result<RawStatus<TokenResponse>, ErrorStatus> {
    let request = context.update((), request.refresh_token.clone());
    let response = state.refresh(request).await?.data;
    Ok(RawStatus::OK(response))
}

#[apiserver_path(
    mount(global),
    method(get),
    path = "/auth/info",
    tag(AUTH_TAG),
    doc = "User Info"
)]
pub async fn user_info(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
) -> Result<GetStatus<UserInfo>, ErrorStatus> {
    let response = state.user_info(context).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    mount(unauthenticated),
    method(post),
    path = "/auth/login",
    tag(AUTH_TAG),
    doc = "User Login"
)]
pub async fn login(
    State(state): State<Services>,
    Json(request): Json<Login>,
) -> Result<RawStatus<TokenResponse>, ErrorStatus> {
    let response = state.login(request).await?.data;
    Ok(RawStatus::OK(response))
}

#[apiserver_path(
    mount(unauthenticated),
    method(post),
    path = "/auth/password_change",
    tag(AUTH_TAG),
    doc = "Password change"
)]
pub async fn password_change(
    State(state): State<Services>,
    Json(request): Json<PasswordChange>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let response = state.password_change(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    mount(unauthenticated),
    method(get),
    path = "/ssl-cert",
    tag(AUTH_TAG),
    content_type = "application/x-pem-file",
    doc = "PEM certificate download"
)]
pub async fn cert_download(State(state): State<Services>) -> Result<DownloadResponse, ErrorStatus> {
    let response = state.cert_download(()).await?.data;
    Ok(DownloadResponse::pem(response).attachment("tabsdata.pem"))
}
