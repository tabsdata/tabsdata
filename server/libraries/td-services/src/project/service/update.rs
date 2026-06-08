//
// Copyright 2026 Tabs Data Inc.
//

use crate::project::service::layer::update::{
    UpdateProjectDBBuilderUpdate, update_project_validate,
};
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, SysAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::project::{
    ProjectDB, ProjectDBWithNames, ProjectRead, ProjectReadBuilder, ProjectUpdate, ProjectUpdateDB,
    ProjectUpdateDBBuilder,
};
use td_objects::rest_urls::ProjectParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{ProjectId, ProjectIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

// System-scoped: the `projects` registry is global, not RLS-scoped. Spans all
// projects (see CreateProjectService).
#[service_factory(
    name = UpdateProjectService,
    request = UpdateRequest<ProjectParam, ProjectUpdate>,
    response = ProjectRead,
    connection = TransactionProvider,
    context = AuthzContext,
    project_scope = bypass,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract_name::<ProjectParam>),
        from_fn(With::<ProjectParam>::extract::<ProjectIdName>),
        from_fn(By::<ProjectIdName>::fetch_one::<ProjectDB>),
        from_fn(With::<ProjectDB>::extract::<ProjectId>),
        from_fn(With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract_data::<ProjectUpdate>),
        from_fn(update_project_validate),
        from_fn(With::<ProjectDB>::try_convert_to::<ProjectUpdateDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<ProjectUpdateDBBuilder, _>),
        from_fn(With::<ProjectUpdate>::update_project_update_db_builder),
        from_fn(With::<ProjectUpdateDBBuilder>::build::<ProjectUpdateDB>),
        from_fn(By::<ProjectId>::update::<ProjectUpdateDB, ProjectDB>),
        from_fn(By::<ProjectId>::fetch_one::<ProjectDBWithNames>),
        from_fn(With::<ProjectDBWithNames>::try_convert_to::<ProjectReadBuilder, _>),
        from_fn(With::<ProjectReadBuilder>::build::<ProjectRead>),
    )
}
