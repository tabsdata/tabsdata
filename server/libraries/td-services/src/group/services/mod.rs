//
// Copyright 2026 Tabs Data Inc.
//

use crate::group::services::create::CreateGroupService;
use crate::group::services::delete::DeleteGroupService;
use crate::group::services::list::ListGroupService;
use crate::group::services::read::ReadGroupService;
use crate::group::services::update::UpdateGroupService;
use ta_services::factory::ServiceFactory;

mod create;
mod delete;
mod list;
mod read;
mod update;

#[derive(ServiceFactory)]
pub struct GroupServices {
    pub create: CreateGroupService,
    pub read: ReadGroupService,
    pub update: UpdateGroupService,
    pub delete: DeleteGroupService,
    pub list: ListGroupService,
}
