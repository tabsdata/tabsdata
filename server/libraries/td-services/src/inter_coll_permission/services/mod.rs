//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::services::create::CreateInterCollectionPermissionService;
use crate::inter_coll_permission::services::delete::DeleteInterCollectionPermissionService;
use crate::inter_coll_permission::services::list::ListInterCollectionPermissionService;
use getset::Getters;
use ta_services::factory::ServiceFactory;

pub mod create;
pub mod delete;
pub mod list;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct InterCollectionPermissionServices {
    create: CreateInterCollectionPermissionService,
    delete: DeleteInterCollectionPermissionService,
    list: ListInterCollectionPermissionService,
}
