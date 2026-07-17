//
// Copyright 2026 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{CreateProjects, Rbac, Scope, System, Visible};
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::project::ProjectRead;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_queries::list_filter::VisibleProjectsFilter;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListProjectsService,
    request = ListRequest<()>,
    response = ListResponse<ProjectRead>,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        // List is open to any authenticated role: every user holds the
        // `default` system role, whose `create_projects` grant gates this.
        from_fn(Rbac::<System, CreateProjects>::check),
        // Resolve the requester's viewable projects; the list is filtered to them.
        from_fn(Visible::projects),
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<ProjectRead>, _>),
        from_fn(By::<()>::list::<VisibleProjectsFilter, ProjectRead>),
    )
}

#[cfg(all(test, feature = "test_tower_metadata"))]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_tower::metadata::type_of_val;

    #[td_test::test(db)]
    async fn test_tower_metadata_list_projects(db: TdPool) {
        ListProjectsService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<ProjectRead>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, CreateProjects>::check),
                type_of_val(&Visible::projects),
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<ProjectRead>, _>),
                type_of_val(&By::<()>::list::<VisibleProjectsFilter, ProjectRead>),
            ]);
    }
}
