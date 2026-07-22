//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::service::layer::rename_project_roles::{
    assert_project_roles_rename_available, rename_project_roles,
};
use crate::project::service::layer::update::{
    UpdateProjectDBBuilderUpdate, assert_project_rename_available, update_project_validate,
};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project, Rbac, Scope};
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

#[service_factory(
    name = UpdateProjectService,
    request = UpdateRequest<ProjectParam, ProjectUpdate>,
    response = ProjectRead,
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract::<RequestContext>),
        from_fn(With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract_name::<ProjectParam>),
        from_fn(With::<ProjectParam>::extract::<ProjectIdName>),
        from_fn(By::<ProjectIdName>::fetch_one::<ProjectDB>),
        from_fn(With::<ProjectDB>::extract::<ProjectId>),
        // Renaming a project requires `manage_project` on that project.
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ManageProject>::check),
        from_fn(With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract_data::<ProjectUpdate>),
        from_fn(update_project_validate),
        // A rename to a name already taken by another project reports a clean
        // "already exists" error instead of the raw unique-constraint
        // violation from the update write.
        from_fn(assert_project_rename_available),
        // Keep the derived per-project roles in step with the rename.
        from_fn(assert_project_roles_rename_available),
        from_fn(rename_project_roles),
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

#[cfg(all(test, feature = "test_tower_metadata"))]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_tower::metadata::type_of_val;

    #[td_test::test(db)]
    async fn test_tower_metadata_update_project(db: TdPool) {
        UpdateProjectService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<ProjectParam, ProjectUpdate>, ProjectRead>(&[
                type_of_val(
                    &With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract::<RequestContext>,
                ),
                type_of_val(
                    &With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract_name::<ProjectParam>,
                ),
                type_of_val(&With::<ProjectParam>::extract::<ProjectIdName>),
                type_of_val(&By::<ProjectIdName>::fetch_one::<ProjectDB>),
                type_of_val(&With::<ProjectDB>::extract::<ProjectId>),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ManageProject>::check),
                type_of_val(
                    &With::<UpdateRequest<ProjectParam, ProjectUpdate>>::extract_data::<ProjectUpdate>,
                ),
                type_of_val(&update_project_validate),
                type_of_val(&assert_project_rename_available),
                type_of_val(&assert_project_roles_rename_available),
                type_of_val(&rename_project_roles),
                type_of_val(&With::<ProjectDB>::try_convert_to::<ProjectUpdateDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<ProjectUpdateDBBuilder, _>),
                type_of_val(&With::<ProjectUpdate>::update_project_update_db_builder),
                type_of_val(&With::<ProjectUpdateDBBuilder>::build::<ProjectUpdateDB>),
                type_of_val(&By::<ProjectId>::update::<ProjectUpdateDB, ProjectDB>),
                type_of_val(&By::<ProjectId>::fetch_one::<ProjectDBWithNames>),
                type_of_val(&With::<ProjectDBWithNames>::try_convert_to::<ProjectReadBuilder, _>),
                type_of_val(&With::<ProjectReadBuilder>::build::<ProjectRead>),
            ]);
    }
}
