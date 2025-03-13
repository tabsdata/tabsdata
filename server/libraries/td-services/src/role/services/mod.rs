//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::services::create::CreateRoleService;
use crate::role::services::read::ReadRoleService;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, ReadRequest};
use td_objects::types::role::{Role, RoleCreate, RoleParam};
use td_tower::service_provider::TdBoxService;

mod create;
mod read;

pub struct RoleServices {
    create: CreateRoleService,
    read: ReadRoleService,
}

impl RoleServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            create: CreateRoleService::new(db.clone()),
            read: ReadRoleService::new(db.clone()),
        }
    }

    pub async fn create_role(&self) -> TdBoxService<CreateRequest<(), RoleCreate>, Role, TdError> {
        self.create.service().await
    }

    pub async fn read_role(&self) -> TdBoxService<ReadRequest<RoleParam>, Role, TdError> {
        self.read.service().await
    }
}
