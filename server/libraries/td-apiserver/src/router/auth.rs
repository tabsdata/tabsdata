//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

const AUTH_TAG: &str = "Authentication";

#[router_ext(SecureAuthRouter)]
mod secure_routes {
    use crate::router::auth::AUTH_TAG;
    use axum::extract::State;
    use axum::{Extension, Form};
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{GetStatus, NoContent, RawStatus, UpdateStatus};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::auth::{RefreshRequestX, RoleChange, TokenResponseX};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user::UserInfo;
    use td_objects::rest_urls::{AUTH_LOGOUT, AUTH_REFRESH, AUTH_ROLE_CHANGE, AUTH_USER_INFO};
    use td_services::auth::services::AuthServices;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;

    #[apiserver_path(method = post, path = AUTH_LOGOUT, tag = AUTH_TAG)]
    #[doc = "User Logout"]
    pub async fn logout(
        State(state): State<Arc<AuthServices>>,
        Extension(context): Extension<RequestContext>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update((), ());
        let response = state.logout.service().oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = post, path = AUTH_REFRESH, tag = AUTH_TAG)]
    #[doc = "Refresh Access Token"]
    pub async fn refresh(
        State(state): State<Arc<AuthServices>>,
        Extension(context): Extension<RequestContext>,
        Form(request): Form<RefreshRequestX>,
    ) -> Result<RawStatus<TokenResponseX>, ErrorStatus> {
        let request = context.update((), request.refresh_token.clone());
        let response = state.refresh.service().raw_oneshot(request).await?;
        Ok(RawStatus::OK(response))
    }

    #[apiserver_path(method = post, path = AUTH_ROLE_CHANGE, tag = AUTH_TAG)]
    #[doc = "Role change"]
    pub async fn role_change(
        State(state): State<Arc<AuthServices>>,
        Extension(context): Extension<RequestContext>,
        Json(request): Json<RoleChange>,
    ) -> Result<RawStatus<TokenResponseX>, ErrorStatus> {
        let request = context.update((), request);
        let response = state
            .role_change
            .service()
            .raw_oneshot(request)
            .await?;
        Ok(RawStatus::OK(response))
    }

    #[apiserver_path(method = get, path = AUTH_USER_INFO, tag = AUTH_TAG)]
    #[doc = "User Info"]
    pub async fn user_info(
        State(state): State<Arc<AuthServices>>,
        Extension(context): Extension<RequestContext>,
    ) -> Result<GetStatus<UserInfo>, ErrorStatus> {
        let request = context.read(());
        let response = state.user_info.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }
}

#[router_ext(UnsecureAuthRouter)]
mod unsecure_routes {
    use crate::router::auth::AUTH_TAG;
    use axum::extract::State;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::extractors::Json;
    use ta_apiserver::status::ok_status::{NoContent, RawStatus, UpdateStatus};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::auth::{Login, PasswordChange, TokenResponseX};
    use td_objects::dxo::file::PemFile;
    use td_objects::rest_urls::{AUTH_LOGIN, AUTH_PASSWORD_CHANGE, CERT_DOWNLOAD};
    use td_services::auth::services::AuthServices;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;

    #[apiserver_path(method = post, path = AUTH_LOGIN, tag = AUTH_TAG)]
    #[doc = "User Login"]
    pub async fn login(
        State(state): State<Arc<AuthServices>>,
        Json(request): Json<Login>,
    ) -> Result<RawStatus<TokenResponseX>, ErrorStatus> {
        let response = state.login.service().raw_oneshot(request).await?;
        // incorrect_role
        // user disabled
        // unauthorized
        Ok(RawStatus::OK(response))
    }

    #[apiserver_path(method = post, path = AUTH_PASSWORD_CHANGE, tag = AUTH_TAG)]
    #[doc = "Password change"]
    pub async fn password_change(
        State(state): State<Arc<AuthServices>>,
        Json(request): Json<PasswordChange>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let response = state
            .password_change
            .service()
            .oneshot(request)
            .await?;
        // incorrect_role
        // user disabled
        // unauthorized
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = get, path = CERT_DOWNLOAD, tag = AUTH_TAG)]
    #[doc = "PEM certificate download"]
    pub async fn cert_download(
        State(state): State<Arc<AuthServices>>,
    ) -> Result<PemFile, ErrorStatus> {
        let response = state.cert_download.service().raw_oneshot(()).await?;
        Ok(PemFile(response))
    }
}
