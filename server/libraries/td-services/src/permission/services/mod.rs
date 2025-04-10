//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::services::create::CreatePermissionService;
use crate::permission::services::delete::DeletePermissionService;
use crate::permission::services::list::ListPermissionService;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, DeleteRequest, ListRequest, ListResponse};
use td_objects::rest_urls::{RoleParam, RolePermissionParam};
use td_objects::types::permission::{Permission, PermissionCreate};
use td_tower::service_provider::TdBoxService;

mod create;
mod delete;
mod list;

pub struct PermissionServices {
    create: CreatePermissionService,
    delete: DeletePermissionService,
    list: ListPermissionService,
}

impl PermissionServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            create: CreatePermissionService::new(db.clone()),
            delete: DeletePermissionService::new(db.clone()),
            list: ListPermissionService::new(db.clone()),
        }
    }

    pub async fn create_permission(
        &self,
    ) -> TdBoxService<CreateRequest<RoleParam, PermissionCreate>, Permission, TdError> {
        self.create.service().await
    }

    pub async fn delete_permission(
        &self,
    ) -> TdBoxService<DeleteRequest<RolePermissionParam>, (), TdError> {
        self.delete.service().await
    }

    pub async fn list_permission(
        &self,
    ) -> TdBoxService<ListRequest<RoleParam>, ListResponse<Permission>, TdError> {
        self.list.service().await
    }
}
