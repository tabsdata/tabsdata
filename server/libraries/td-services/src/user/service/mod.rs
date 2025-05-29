//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::service::create::CreateUserService;
use crate::user::service::delete::DeleteUserService;
use crate::user::service::list::ListUsersService;
use crate::user::service::read::ReadUserService;
use crate::user::service::update::UpdateUserService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{
    CreateRequest, DeleteRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest,
};
use td_objects::rest_urls::UserParam;
use td_objects::types::user::{UserCreate, UserRead, UserUpdate};
use td_security::config::PasswordHashingConfig;
use td_tower::service_provider::TdBoxService;

pub mod create;
pub mod delete;
pub mod list;
pub mod read;
pub mod update;

#[cfg(test)]
mod test_errors;

pub struct UserServices {
    create_service_provider: CreateUserService,
    read_service_provider: ReadUserService,
    update_service_provider: UpdateUserService,
    delete_service_provider: DeleteUserService,
    list_service_provider: ListUsersService,
}

impl UserServices {
    pub fn new(
        db: DbPool,
        password_hashing_config: Arc<PasswordHashingConfig>,
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
        }
    }

    pub async fn create_user(
        &self,
    ) -> TdBoxService<CreateRequest<(), UserCreate>, UserRead, TdError> {
        self.create_service_provider.service().await
    }

    pub async fn read_user(&self) -> TdBoxService<ReadRequest<UserParam>, UserRead, TdError> {
        self.read_service_provider.service().await
    }

    pub async fn delete_user(&self) -> TdBoxService<DeleteRequest<UserParam>, (), TdError> {
        self.delete_service_provider.service().await
    }

    pub async fn update_user(
        &self,
    ) -> TdBoxService<UpdateRequest<UserParam, UserUpdate>, UserRead, TdError> {
        self.update_service_provider.service().await
    }

    pub async fn list_users(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<UserRead>, TdError> {
        self.list_service_provider.service().await
    }
}
