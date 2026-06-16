//
// Copyright 2026 Tabs Data Inc.
//

use crate::simple_permission::services::grant::GrantSimplePermissionService;
use crate::simple_permission::services::list_by_project::ListSimplePermissionByProjectService;
use crate::simple_permission::services::list_by_user::ListSimplePermissionByUserService;
use crate::simple_permission::services::revoke::RevokeSimplePermissionService;
use ta_services::factory::ServiceFactory;

mod grant;
mod list_by_project;
mod list_by_user;
mod revoke;

#[derive(ServiceFactory)]
pub struct SimplePermissionServices {
    pub grant: GrantSimplePermissionService,
    pub revoke: RevokeSimplePermissionService,
    pub list_by_user: ListSimplePermissionByUserService,
    pub list_by_project: ListSimplePermissionByProjectService,
}
