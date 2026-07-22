//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::service::layer::delete_project_roles::delete_project_roles;
use crate::project::service::layer::guards::reject_delete_non_empty_project;
use ta_services::factory::service_factory;
use td_authz::rbac::{ManageProject, Project, Rbac, Scope};
use td_authz::{AuthzContext, refresh_authz_context};
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::project::{ActiveProjectDB, ProjectDeleteDB, ProjectDeleteDBBuilder};
use td_objects::rest_urls::ProjectParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{ProjectId, ProjectIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DeleteProjectService,
    request = DeleteRequest<ProjectParam>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<ProjectParam>>::extract::<RequestContext>),
        from_fn(With::<DeleteRequest<ProjectParam>>::extract_name::<ProjectParam>),
        from_fn(With::<ProjectParam>::extract::<ProjectIdName>),
        from_fn(By::<ProjectIdName>::fetch_one::<ActiveProjectDB>),
        from_fn(With::<ActiveProjectDB>::extract::<ProjectId>),
        // Deleting a project requires `manage_project` on that project.
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ManageProject>::check),
        // Guard before the soft-delete: never strand a non-empty project's data.
        from_fn(By::<ProjectId>::fetch_all::<ActiveCollectionDB>),
        from_fn(reject_delete_non_empty_project),
        from_fn(With::<ActiveProjectDB>::try_convert_to::<ProjectDeleteDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<ProjectDeleteDBBuilder, _>),
        from_fn(With::<ProjectDeleteDBBuilder>::build::<ProjectDeleteDB>),
        from_fn(By::<ProjectId>::update::<ProjectDeleteDB, ActiveProjectDB>),
        // System cascade: drop the project's three per-project roles together
        // with their permission + membership rows (bypasses the `fixed` guard;
        // see `delete_project_roles`).
        from_fn(delete_project_roles),
        // `delete_project_roles` dropped the project's roles, permissions, and
        // memberships; invalidate the cached authz grants.
        from_fn(refresh_authz_context),
    )
}

#[cfg(all(test, feature = "test_tower_metadata"))]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_tower::metadata::type_of_val;

    #[td_test::test(db)]
    async fn test_tower_metadata_delete_project(db: TdPool) {
        DeleteProjectService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<DeleteRequest<ProjectParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<ProjectParam>>::extract::<RequestContext>),
                type_of_val(&With::<DeleteRequest<ProjectParam>>::extract_name::<ProjectParam>),
                type_of_val(&With::<ProjectParam>::extract::<ProjectIdName>),
                type_of_val(&By::<ProjectIdName>::fetch_one::<ActiveProjectDB>),
                type_of_val(&With::<ActiveProjectDB>::extract::<ProjectId>),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ManageProject>::check),
                type_of_val(&By::<ProjectId>::fetch_all::<ActiveCollectionDB>),
                type_of_val(&reject_delete_non_empty_project),
                type_of_val(&With::<ActiveProjectDB>::try_convert_to::<ProjectDeleteDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<ProjectDeleteDBBuilder, _>),
                type_of_val(&With::<ProjectDeleteDBBuilder>::build::<ProjectDeleteDB>),
                type_of_val(&By::<ProjectId>::update::<ProjectDeleteDB, ActiveProjectDB>),
                type_of_val(&delete_project_roles),
                type_of_val(&refresh_authz_context),
            ]);
    }
}
