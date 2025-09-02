//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::service::create::CreateUserService;
use crate::user::service::delete::DeleteUserService;
use crate::user::service::list::ListUsersService;
use crate::user::service::read::ReadUserService;
use crate::user::service::update::UpdateUserService;
use getset::Getters;
use td_tower::ServiceFactory;

pub mod create;
pub mod delete;
pub mod list;
pub mod read;
pub mod update;

#[cfg(test)]
mod test_errors;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct UserServices {
    create: CreateUserService,
    read: ReadUserService,
    update: UpdateUserService,
    delete: DeleteUserService,
    list: ListUsersService,
}
