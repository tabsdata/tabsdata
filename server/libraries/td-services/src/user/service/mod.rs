//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::service::create::CreateUserService;
use crate::user::service::delete::DeleteUserService;
use crate::user::service::list::ListUsersService;
use crate::user::service::read::ReadUserService;
use crate::user::service::update::UpdateUserService;
use ta_services::factory::ServiceFactory;

pub mod create;
pub mod delete;
pub mod list;
pub mod read;
pub mod update;

#[cfg(test)]
mod test_errors;

#[derive(ServiceFactory)]
pub struct UserServices {
    pub create: CreateUserService,
    pub read: ReadUserService,
    pub update: UpdateUserService,
    pub delete: DeleteUserService,
    pub list: ListUsersService,
}
