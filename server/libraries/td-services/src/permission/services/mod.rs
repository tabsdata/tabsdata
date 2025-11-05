//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::services::create::CreatePermissionService;
use crate::permission::services::delete::DeletePermissionService;
use crate::permission::services::list::ListPermissionService;
use ta_services::factory::ServiceFactory;

mod create;
mod delete;
mod list;

#[derive(ServiceFactory)]
pub struct PermissionServices {
    pub create: CreatePermissionService,
    pub delete: DeletePermissionService,
    pub list: ListPermissionService,
}
