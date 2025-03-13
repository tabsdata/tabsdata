//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::services::create::CreateRoleService;
use crate::role::services::delete::DeleteRoleService;
use crate::role::services::list::ListRoleService;
use crate::role::services::read::ReadRoleService;
use crate::role::services::update::UpdateRoleService;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{
    CreateRequest, DeleteRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest,
};
use td_objects::types::role::{Role, RoleCreate, RoleParam, RoleUpdate};
use td_tower::service_provider::TdBoxService;

mod create;
mod delete;
mod list;
mod read;
mod update;

pub struct RoleServices {
    create: CreateRoleService,
    read: ReadRoleService,
    update: UpdateRoleService,
    delete: DeleteRoleService,
    list: ListRoleService,
}

impl RoleServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            create: CreateRoleService::new(db.clone()),
            read: ReadRoleService::new(db.clone()),
            update: UpdateRoleService::new(db.clone()),
            delete: DeleteRoleService::new(db.clone()),
            list: ListRoleService::new(db.clone()),
        }
    }

    pub async fn create_role(&self) -> TdBoxService<CreateRequest<(), RoleCreate>, Role, TdError> {
        self.create.service().await
    }

    pub async fn read_role(&self) -> TdBoxService<ReadRequest<RoleParam>, Role, TdError> {
        self.read.service().await
    }

    pub async fn update_role(
        &self,
    ) -> TdBoxService<UpdateRequest<RoleParam, RoleUpdate>, Role, TdError> {
        self.update.service().await
    }

    pub async fn delete_role(&self) -> TdBoxService<DeleteRequest<RoleParam>, (), TdError> {
        self.delete.service().await
    }

    pub async fn list_role(&self) -> TdBoxService<ListRequest<()>, ListResponse<Role>, TdError> {
        self.list.service().await
    }
}
