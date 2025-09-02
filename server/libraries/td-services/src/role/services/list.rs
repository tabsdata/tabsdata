//
// Copyright 2025 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::role::Role;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = ListRoleService,
    request = ListRequest<()>,
    response = ListResponse<Role>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(By::<()>::list::<(), NoListFilter, Role>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_role::{get_role, seed_role};
    use td_objects::types::basic::{AccessTokenId, Description, RoleId, RoleName, UserId};
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_role(db: DbPool) {
        use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
        use td_tower::metadata::type_of_val;

        ListRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<Role>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(&By::<()>::list::<(), NoListFilter, Role>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_role(db: DbPool) -> Result<(), TdError> {
        let _role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .list((), ListParams::default());

        let service = ListRoleService::with_defaults(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(*response.len(), 4); // 3 default roles + 1
        let response = response.data();
        assert_eq!(*response[0].name(), RoleName::try_from("sys_admin")?);
        assert_eq!(*response[1].name(), RoleName::try_from("sec_admin")?);
        assert_eq!(*response[2].name(), RoleName::try_from("user")?);

        let found = get_role(&db, &RoleName::try_from("joaquin").unwrap()).await?;
        let role = response.get(3).unwrap();
        assert_eq!(role.id(), found.id());
        assert_eq!(role.name(), found.name());
        assert_eq!(role.description(), found.description());
        assert_eq!(role.created_on(), found.created_on());
        assert_eq!(role.created_by_id(), found.created_by_id());
        assert_eq!(role.modified_on(), found.modified_on());
        assert_eq!(role.modified_by_id(), found.modified_by_id());
        assert_eq!(role.fixed(), found.fixed());
        Ok(())
    }
}
