//
// Copyright 2026 Tabs Data Inc.
//

use crate::project::service::layer::scope_to_project::scope_to_created_project;
use crate::project::service::layer::seed_groups::seed_default_groups;
use crate::project::service::layer::seed_project_roles::seed_project_roles;
use crate::project::service::layer::single_project::reject_create_on_single_project_backend;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{CreateProjects, Rbac, Scope, System};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::project::{
    ProjectCreate, ProjectCreateDB, ProjectCreateDBBuilder, ProjectDBWithNames, ProjectRead,
    ProjectReadBuilder,
};
use td_objects::tower_service::with::With;
use td_objects::types::basic::ProjectId;
use td_queries::{By, insert};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateProjectService,
    request = CreateRequest<(), ProjectCreate>,
    response = ProjectRead,
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<(), ProjectCreate>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, CreateProjects>::check),
        from_fn(reject_create_on_single_project_backend),
        from_fn(With::<CreateRequest<(), ProjectCreate>>::extract_data::<ProjectCreate>),
        from_fn(With::<ProjectCreate>::try_convert_to::<ProjectCreateDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<ProjectCreateDBBuilder, _>),
        from_fn(With::<ProjectCreateDBBuilder>::build::<ProjectCreateDB>),
        from_fn(insert::<ProjectCreateDB>),
        from_fn(With::<ProjectCreateDB>::extract::<ProjectId>),
        // Scope the connection to the new project so the group seeds default
        // `project_id` from the GUC instead of stamping it by hand.
        from_fn(scope_to_created_project),
        // A collection resolves its group within its own project, so seed the
        // system groups (default/source/destination) into the new project.
        from_fn(seed_default_groups),
        // Mint the three per-project roles (`<name>_admin/_dev/_view`) and add
        // the creating user to `<name>_admin` (TEDA per-project roles).
        from_fn(seed_project_roles),
        from_fn(By::<ProjectId>::fetch_one::<ProjectDBWithNames>),
        from_fn(With::<ProjectDBWithNames>::try_convert_to::<ProjectReadBuilder, _>),
        from_fn(With::<ProjectReadBuilder>::build::<ProjectRead>),
    )
}

#[cfg(all(test, feature = "test_tower_metadata"))]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_tower::metadata::type_of_val;

    #[td_test::test(db)]
    async fn test_tower_metadata_create_project(db: TdPool) {
        CreateProjectService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<(), ProjectCreate>, ProjectRead>(&[
                type_of_val(&With::<CreateRequest<(), ProjectCreate>>::extract::<RequestContext>),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, CreateProjects>::check),
                type_of_val(&reject_create_on_single_project_backend),
                type_of_val(
                    &With::<CreateRequest<(), ProjectCreate>>::extract_data::<ProjectCreate>,
                ),
                type_of_val(&With::<ProjectCreate>::try_convert_to::<ProjectCreateDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<ProjectCreateDBBuilder, _>),
                type_of_val(&With::<ProjectCreateDBBuilder>::build::<ProjectCreateDB>),
                type_of_val(&insert::<ProjectCreateDB>),
                type_of_val(&With::<ProjectCreateDB>::extract::<ProjectId>),
                type_of_val(&scope_to_created_project),
                type_of_val(&seed_default_groups),
                type_of_val(&seed_project_roles),
                type_of_val(&By::<ProjectId>::fetch_one::<ProjectDBWithNames>),
                type_of_val(&With::<ProjectDBWithNames>::try_convert_to::<ProjectReadBuilder, _>),
                type_of_val(&With::<ProjectReadBuilder>::build::<ProjectRead>),
            ]);
    }
}
