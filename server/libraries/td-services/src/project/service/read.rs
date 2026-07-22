//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::project::{ProjectDBWithNames, ProjectRead, ProjectReadBuilder};
use td_objects::rest_urls::ProjectParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{ProjectId, ProjectIdName};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReadProjectService,
    request = ReadRequest<ProjectParam>,
    response = ProjectRead,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<ProjectParam>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<ProjectParam>>::extract_name::<ProjectParam>),
        from_fn(With::<ProjectParam>::extract::<ProjectIdName>),
        from_fn(By::<ProjectIdName>::fetch_one::<ProjectDBWithNames>),
        // Reading a project's metadata requires `view_project` on that project.
        from_fn(With::<ProjectDBWithNames>::extract::<ProjectId>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
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
    async fn test_tower_metadata_read_project(db: TdPool) {
        ReadProjectService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<ProjectParam>, ProjectRead>(&[
                type_of_val(&With::<ReadRequest<ProjectParam>>::extract::<RequestContext>),
                type_of_val(&With::<ReadRequest<ProjectParam>>::extract_name::<ProjectParam>),
                type_of_val(&With::<ProjectParam>::extract::<ProjectIdName>),
                type_of_val(&By::<ProjectIdName>::fetch_one::<ProjectDBWithNames>),
                type_of_val(&With::<ProjectDBWithNames>::extract::<ProjectId>),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ViewProject>::check),
                type_of_val(&With::<ProjectDBWithNames>::try_convert_to::<ProjectReadBuilder, _>),
                type_of_val(&With::<ProjectReadBuilder>::build::<ProjectRead>),
            ]);
    }
}
