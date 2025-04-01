//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::services::login::LoginService;
use crate::auth::services::logout::LogoutService;
use crate::auth::services::password_change::PasswordChangeService;
use crate::auth::services::refresh::RefreshService;
use crate::auth::services::role_change::RoleChangeService;
use crate::auth::services::user_info::UserInfoService;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, ReadRequest, UpdateRequest};
use td_objects::types::auth::{Login, PasswordChange, RoleChange, TokenResponseX, UserInfo};
use td_objects::types::basic::RefreshToken;
use td_tower::service_provider::TdBoxService;

mod login;
mod logout;
mod password_change;
mod refresh;
mod role_change;
mod user_info;

pub struct AuthServices {
    login: LoginService,
    refresh: RefreshService,
    logout: LogoutService,
    user_info: UserInfoService,
    role_change: RoleChangeService,
    password_change: PasswordChangeService,
}

impl AuthServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            login: LoginService::new(db.clone()),
            refresh: RefreshService::new(db.clone()),
            logout: LogoutService::new(db.clone()),
            user_info: UserInfoService::new(db.clone()),
            role_change: RoleChangeService::new(db.clone()),
            password_change: PasswordChangeService::new(db),
        }
    }

    pub async fn login_service(
        &self,
    ) -> TdBoxService<CreateRequest<(), Login>, TokenResponseX, TdError> {
        self.login.service().await
    }

    pub async fn refresh_service(&self) -> TdBoxService<RefreshToken, TokenResponseX, TdError> {
        self.refresh.service().await
    }

    pub async fn logout_service(&self) -> TdBoxService<UpdateRequest<(), ()>, (), TdError> {
        self.logout.service().await
    }

    pub async fn user_info_service(&self) -> TdBoxService<ReadRequest<()>, UserInfo, TdError> {
        self.user_info.service().await
    }

    pub async fn role_change_service(
        &self,
    ) -> TdBoxService<UpdateRequest<(), RoleChange>, TokenResponseX, TdError> {
        self.role_change.service().await
    }

    pub async fn password_change_service(
        &self,
    ) -> TdBoxService<UpdateRequest<(), PasswordChange>, (), TdError> {
        self.password_change.service().await
    }
}
