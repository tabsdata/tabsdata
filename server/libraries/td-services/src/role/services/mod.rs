//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::services::create::CreateRoleService;
use crate::role::services::delete::DeleteRoleService;
use crate::role::services::list::ListRoleService;
use crate::role::services::read::ReadRoleService;
use crate::role::services::update::UpdateRoleService;
use getset::Getters;
use td_tower::ServiceFactory;

mod create;
mod delete;
mod list;
mod read;
mod update;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct RoleServices {
    create: CreateRoleService,
    read: ReadRoleService,
    update: UpdateRoleService,
    delete: DeleteRoleService,
    list: ListRoleService,
}
