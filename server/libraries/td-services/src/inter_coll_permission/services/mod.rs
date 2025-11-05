//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::services::create::CreateInterCollectionPermissionService;
use crate::inter_coll_permission::services::delete::DeleteInterCollectionPermissionService;
use crate::inter_coll_permission::services::list::ListInterCollectionPermissionService;
use ta_services::factory::ServiceFactory;

pub mod create;
pub mod delete;
pub mod list;

#[derive(ServiceFactory)]
pub struct InterCollectionPermissionServices {
    pub create: CreateInterCollectionPermissionService,
    pub delete: DeleteInterCollectionPermissionService,
    pub list: ListInterCollectionPermissionService,
}
