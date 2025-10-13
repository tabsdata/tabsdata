//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::services::create::CreatePermissionService;
use crate::permission::services::delete::DeletePermissionService;
use crate::permission::services::list::ListPermissionService;
use getset::Getters;
use ta_services::factory::ServiceFactory;

mod create;
mod delete;
mod list;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct PermissionServices {
    create: CreatePermissionService,
    delete: DeletePermissionService,
    list: ListPermissionService,
}
