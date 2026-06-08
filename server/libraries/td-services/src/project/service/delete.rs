//
// Copyright 2026 Tabs Data Inc.
//

use crate::project::service::layer::guards::{
    reject_delete_non_empty_project, reject_delete_sandbox_on_single_project_backend,
};
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, SysAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::project::{ProjectDB, ProjectDeleteDB, ProjectDeleteDBBuilder};
use td_objects::rest_urls::ProjectParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{ProjectId, ProjectIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

// Project management is a system-scoped operation, not tenant work: the
// `projects` registry is not RLS-protected, and the non-empty guard below
// must see the *target* project's collections regardless of the caller's own
// scope. So this service spans all projects.
#[service_factory(
    name = DeleteProjectService,
    request = DeleteRequest<ProjectParam>,
    response = (),
    connection = TransactionProvider,
    context = AuthzContext,
    project_scope = bypass,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<ProjectParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<DeleteRequest<ProjectParam>>::extract_name::<ProjectParam>),
        from_fn(With::<ProjectParam>::extract::<ProjectIdName>),
        from_fn(By::<ProjectIdName>::fetch_one::<ProjectDB>),
        from_fn(With::<ProjectDB>::extract::<ProjectId>),
        // Guard before the soft-delete: keep sandbox on single-project
        // backends, and never strand a non-empty project's data (any backend).
        from_fn(reject_delete_sandbox_on_single_project_backend),
        from_fn(By::<ProjectId>::fetch_all::<CollectionDB>),
        from_fn(reject_delete_non_empty_project),
        from_fn(With::<ProjectDB>::try_convert_to::<ProjectDeleteDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<ProjectDeleteDBBuilder, _>),
        from_fn(With::<ProjectDeleteDBBuilder>::build::<ProjectDeleteDB>),
        from_fn(By::<ProjectId>::update::<ProjectDeleteDB, ProjectDB>),
    )
}
