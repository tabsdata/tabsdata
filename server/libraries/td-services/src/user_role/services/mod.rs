//
// Copyright 2025 Tabs Data Inc.
//

use crate::user_role::services::create::CreateUserRoleService;
use crate::user_role::services::delete::DeleteUserRoleService;
use crate::user_role::services::list::ListUserRoleService;
use crate::user_role::services::read::ReadUserRoleService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, DeleteRequest, ListRequest, ListResponse, ReadRequest};
use td_objects::rest_urls::{RoleParam, UserRoleParam};
use td_objects::sql::DaoQueries;
use td_objects::types::role::{UserRole, UserRoleCreate};
use td_tower::service_provider::TdBoxService;

mod create;
mod delete;
mod list;
mod read;

pub struct UserRoleServices {
    create: CreateUserRoleService,
    read: ReadUserRoleService,
    delete: DeleteUserRoleService,
    list: ListUserRoleService,
}

impl UserRoleServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            create: CreateUserRoleService::new(db.clone(), queries.clone(), authz_context.clone()),
            read: ReadUserRoleService::new(db.clone(), queries.clone(), authz_context.clone()),
            delete: DeleteUserRoleService::new(db.clone(), queries.clone(), authz_context.clone()),
            list: ListUserRoleService::new(db.clone(), queries.clone(), authz_context.clone()),
        }
    }

    pub async fn create_user_role(
        &self,
    ) -> TdBoxService<CreateRequest<RoleParam, UserRoleCreate>, UserRole, TdError> {
        self.create.service().await
    }

    pub async fn read_user_roles(
        &self,
    ) -> TdBoxService<ReadRequest<UserRoleParam>, UserRole, TdError> {
        self.read.service().await
    }

    pub async fn delete_user_role(
        &self,
    ) -> TdBoxService<DeleteRequest<UserRoleParam>, (), TdError> {
        self.delete.service().await
    }

    pub async fn list_user_roles(
        &self,
    ) -> TdBoxService<ListRequest<RoleParam>, ListResponse<UserRole>, TdError> {
        self.list.service().await
    }
}
