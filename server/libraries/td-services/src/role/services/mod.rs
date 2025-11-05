//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::services::create::CreateRoleService;
use crate::role::services::delete::DeleteRoleService;
use crate::role::services::list::ListRoleService;
use crate::role::services::read::ReadRoleService;
use crate::role::services::update::UpdateRoleService;
use ta_services::factory::ServiceFactory;

mod create;
mod delete;
mod list;
mod read;
mod update;

#[derive(ServiceFactory)]
pub struct RoleServices {
    pub create: CreateRoleService,
    pub read: ReadRoleService,
    pub update: UpdateRoleService,
    pub delete: DeleteRoleService,
    pub list: ListRoleService,
}
