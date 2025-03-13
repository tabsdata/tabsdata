//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::services::create::CreateRoleService;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::CreateRequest;
use td_objects::types::role::{Role, RoleCreate};
use td_tower::service_provider::TdBoxService;

mod create;

pub struct RoleServices {
    create: CreateRoleService,
}

impl RoleServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            create: CreateRoleService::new(db),
        }
    }

    pub async fn create_role(&self) -> TdBoxService<CreateRequest<(), RoleCreate>, Role, TdError> {
        self.create.service().await
    }
}
