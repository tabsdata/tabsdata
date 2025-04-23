//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::service::authenticate_user::AuthenticateUserService;
use crate::users::service::create_user::CreateUserService;
use crate::users::service::delete_user::DeleteUserService;
use crate::users::service::list_users::ListUsersService;
use crate::users::service::read_user::ReadUserService;
use crate::users::service::update_user::UpdateUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{
    CreateRequest, DeleteRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest,
};
use td_objects::jwt::jwt_logic::{JwtLogic, TokenResponse};
use td_objects::users::dto::{AuthenticateRequest, UserCreate, UserList, UserRead, UserUpdate};
use td_security::config::PasswordHashingConfig;
use td_tower::service_provider::TdBoxService;

pub mod authenticate_user;
pub mod create_user;
pub mod delete_user;
pub mod list_users;
pub mod read_user;
pub mod update_user;

#[cfg(test)]
mod test_errors;

pub struct UserServices {
    create_service_provider: CreateUserService,
    read_service_provider: ReadUserService,
    update_service_provider: UpdateUserService,
    delete_service_provider: DeleteUserService,
    list_service_provider: ListUsersService,
    authenticate_service_provider: AuthenticateUserService,
}

impl UserServices {
    pub fn new(
        db: DbPool,
        password_hashing_config: Arc<PasswordHashingConfig>,
        jwt_logic: Arc<JwtLogic>,
        authz_context: Arc<AuthzContext>,
    ) -> Self {
        Self {
            create_service_provider: CreateUserService::new(
                db.clone(),
                password_hashing_config.clone(),
                authz_context.clone(),
            ),
            read_service_provider: ReadUserService::new(db.clone(), authz_context.clone()),
            update_service_provider: UpdateUserService::new(
                db.clone(),
                password_hashing_config.clone(),
                authz_context.clone(),
            ),
            delete_service_provider: DeleteUserService::new(db.clone(), authz_context.clone()),
            list_service_provider: ListUsersService::new(db.clone(), authz_context.clone()),
            authenticate_service_provider: AuthenticateUserService::new(db.clone(), jwt_logic),
        }
    }

    pub async fn create_user(
        &self,
    ) -> TdBoxService<CreateRequest<(), UserCreate>, UserRead, TdError> {
        self.create_service_provider.service().await
    }

    pub async fn read_user(&self) -> TdBoxService<ReadRequest<String>, UserRead, TdError> {
        self.read_service_provider.service().await
    }

    pub async fn delete_user(&self) -> TdBoxService<DeleteRequest<String>, (), TdError> {
        self.delete_service_provider.service().await
    }

    pub async fn update_user(
        &self,
    ) -> TdBoxService<UpdateRequest<String, UserUpdate>, UserRead, TdError> {
        self.update_service_provider.service().await
    }

    pub async fn list_users(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<UserList>, TdError> {
        self.list_service_provider.service().await
    }

    pub async fn authenticate_user(
        &self,
    ) -> TdBoxService<AuthenticateRequest, TokenResponse, TdError> {
        self.authenticate_service_provider.service().await
    }
}
