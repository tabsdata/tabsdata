//
// Copyright 2026 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, NoPermissions, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::project::{ProjectDBWithNames, ProjectRead, ProjectReadBuilder};
use td_objects::rest_urls::ProjectParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::ProjectIdName;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

// System-scoped: the `projects` registry is global, not RLS-scoped. Spans all
// projects (see CreateProjectService).
#[service_factory(
    name = ReadProjectService,
    request = ReadRequest<ProjectParam>,
    response = ProjectRead,
    connection = ConnectionProvider,
    context = AuthzContext,
    project_scope = bypass,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<ProjectParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        // Read is open to any authenticated role; the apiserver may further
        // restrict who sees what when project visibility is added.
        from_fn(Authz::<NoPermissions>::check),
        from_fn(With::<ReadRequest<ProjectParam>>::extract_name::<ProjectParam>),
        from_fn(With::<ProjectParam>::extract::<ProjectIdName>),
        from_fn(By::<ProjectIdName>::fetch_one::<ProjectDBWithNames>),
        from_fn(With::<ProjectDBWithNames>::try_convert_to::<ProjectReadBuilder, _>),
        from_fn(With::<ProjectReadBuilder>::build::<ProjectRead>),
    )
}
