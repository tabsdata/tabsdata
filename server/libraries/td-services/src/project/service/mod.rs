//
// Copyright 2026 Tabs Data Inc.
//

use crate::project::service::create::CreateProjectService;
use crate::project::service::delete::DeleteProjectService;
use crate::project::service::list::ListProjectsService;
use crate::project::service::read::ReadProjectService;
use crate::project::service::update::UpdateProjectService;
use ta_services::factory::ServiceFactory;

mod create;
mod delete;
mod layer;
mod list;
mod read;
mod update;

#[cfg(test)]
mod tests;

#[derive(ServiceFactory)]
pub struct ProjectServices {
    pub create: CreateProjectService,
    pub read: ReadProjectService,
    pub update: UpdateProjectService,
    pub delete: DeleteProjectService,
    pub list: ListProjectsService,
}
