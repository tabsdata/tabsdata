//
// Copyright 2025 Tabs Data Inc.
//

use crate::user_role::services::create::CreateUserRoleService;
use crate::user_role::services::delete::DeleteUserRoleService;
use crate::user_role::services::list::ListUserRoleService;
use crate::user_role::services::read::ReadUserRoleService;
use ta_services::factory::ServiceFactory;

mod create;
mod delete;
mod list;
mod read;

#[derive(ServiceFactory)]
pub struct UserRoleServices {
    pub create: CreateUserRoleService,
    pub read: ReadUserRoleService,
    pub delete: DeleteUserRoleService,
    pub list: ListUserRoleService,
}
