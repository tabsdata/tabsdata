//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, insert};
use td_objects::types::basic::RoleId;
use td_objects::types::role::{
    Role, RoleBuilder, RoleCreate, RoleDB, RoleDBBuilder, RoleDBWithNames,
};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateRoleService,
    request = CreateRequest<(), RoleCreate>,
    response = Role,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<(), RoleCreate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin>::check),
        from_fn(With::<CreateRequest<(), RoleCreate>>::extract_data::<RoleCreate>),
        from_fn(With::<RoleCreate>::convert_to::<RoleDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<RoleDBBuilder, _>),
        from_fn(With::<RoleDBBuilder>::build::<RoleDB, _>),
        from_fn(insert::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(By::<RoleId>::select::<RoleDBWithNames>),
        from_fn(With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
        from_fn(With::<RoleBuilder>::build::<Role, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::get_role;
    use td_objects::types::basic::{AccessTokenId, RoleName, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_create_role(db: DbPool) {
        use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
        use td_tower::metadata::type_of_val;

        CreateRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<(), RoleCreate>, Role>(&[
                type_of_val(&With::<CreateRequest<(), RoleCreate>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin>::check),
                type_of_val(&With::<CreateRequest<(), RoleCreate>>::extract_data::<RoleCreate>),
                type_of_val(&With::<RoleCreate>::convert_to::<RoleDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<RoleDBBuilder, _>),
                type_of_val(&With::<RoleDBBuilder>::build::<RoleDB, _>),
                type_of_val(&insert::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&By::<RoleId>::select::<RoleDBWithNames>),
                type_of_val(&With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
                type_of_val(&With::<RoleBuilder>::build::<Role, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_role(db: DbPool) -> Result<(), TdError> {
        let create = RoleCreate::builder()
            .try_name("test")?
            .try_description("test desc")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .create((), create);

        let service = CreateRoleService::with_defaults(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_role(&db, &RoleName::try_from("test")?).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.name(), found.name());
        assert_eq!(response.description(), found.description());
        assert_eq!(response.created_on(), found.created_on());
        assert_eq!(response.created_by_id(), found.created_by_id());
        assert_eq!(response.modified_on(), found.modified_on());
        assert_eq!(response.modified_by_id(), found.modified_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
