//
// Copyright 2025 Tabs Data Inc.
//

use crate::user_role::services::create::CreateUserRoleService;
use crate::user_role::services::delete::DeleteUserRoleService;
use crate::user_role::services::list::ListUserRoleService;
use crate::user_role::services::read::ReadUserRoleService;
use getset::Getters;
use td_tower::ServiceFactory;

mod create;
mod delete;
mod list;
mod read;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct UserRoleServices {
    create: CreateUserRoleService,
    read: ReadUserRoleService,
    delete: DeleteUserRoleService,
    list: ListUserRoleService,
}
