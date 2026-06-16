//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, ManageProject, OnAnyProject, Rbac, Scope, System};
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::role::Role;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListRoleService,
    request = ListRequest<()>,
    response = ListResponse<Role>,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, (ManageAccess, OnAnyProject<ManageProject>)>::check),
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Role>, _>),
        from_fn(By::<()>::list::<NoFilter, Role>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::role::RoleDB;
    use td_objects::types::basic::{AccessTokenId, Description, RoleId, RoleName, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_role(db: TdPool) {
        use td_authz::rbac::{ManageAccess, ManageProject, OnAnyProject, Rbac, Scope, System};
        use td_tower::metadata::type_of_val;

        ListRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<Role>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, (ManageAccess, OnAnyProject<ManageProject>)>::check),
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Role>, _>),
                type_of_val(&By::<()>::list::<NoFilter, Role>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_role(db: TdPool) -> Result<(), TdError> {
        let _role = db
            .seed()
            .role(
                RoleName::try_from("joaquin")?,
                Description::try_from("super user")?,
            )
            .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .list((), ListParams::default());

        let service = ListRoleService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(response.len, 3); // 2 default roles + 1
        let response = response.data;
        assert_eq!(response[0].name, RoleName::try_from("sys_admin")?);
        assert_eq!(response[1].name, RoleName::try_from("default")?);

        let found = db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin").unwrap())
            .await?;
        let role = response.get(2).unwrap();
        assert_eq!(role.id, found.id);
        assert_eq!(role.name, found.name);
        assert_eq!(role.description, found.description);
        assert_eq!(role.created_on, found.created_on);
        assert_eq!(role.created_by_id, found.created_by_id);
        assert_eq!(role.modified_on, found.modified_on);
        assert_eq!(role.modified_by_id, found.modified_by_id);
        assert_eq!(role.fixed, found.fixed);
        Ok(())
    }
}
