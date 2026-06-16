//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Rbac, RoleMgnt, Scope, System};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::role::{
    Role, RoleBuilder, RoleCreate, RoleDB, RoleDBBuilder, RoleDBWithNames,
};
use td_objects::tower_service::with::With;
use td_objects::types::basic::RoleId;
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateRoleService,
    request = CreateRequest<(), RoleCreate>,
    response = Role,
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<(), RoleCreate>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, RoleMgnt>::check),
        from_fn(With::<CreateRequest<(), RoleCreate>>::extract_data::<RoleCreate>),
        from_fn(With::<RoleCreate>::try_convert_to::<RoleDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<RoleDBBuilder, _>),
        from_fn(With::<RoleDBBuilder>::build::<RoleDB>),
        from_fn(insert::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(By::<RoleId>::fetch_one::<RoleDBWithNames>),
        from_fn(With::<RoleDBWithNames>::try_convert_to::<RoleBuilder, _>),
        from_fn(With::<RoleBuilder>::build::<Role>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleName, UserId};
    use td_queries::LiveEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_create_role(db: TdPool) {
        use td_authz::rbac::{Rbac, RoleMgnt, Scope, System};
        use td_tower::metadata::type_of_val;

        CreateRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<(), RoleCreate>, Role>(&[
                type_of_val(&With::<CreateRequest<(), RoleCreate>>::extract::<RequestContext>),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, RoleMgnt>::check),
                type_of_val(&With::<CreateRequest<(), RoleCreate>>::extract_data::<RoleCreate>),
                type_of_val(&With::<RoleCreate>::try_convert_to::<RoleDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<RoleDBBuilder, _>),
                type_of_val(&With::<RoleDBBuilder>::build::<RoleDB>),
                type_of_val(&insert::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&By::<RoleId>::fetch_one::<RoleDBWithNames>),
                type_of_val(&With::<RoleDBWithNames>::try_convert_to::<RoleBuilder, _>),
                type_of_val(&With::<RoleBuilder>::build::<Role>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_create_role(db: TdPool) -> Result<(), TdError> {
        let create = RoleCreate::builder()
            .try_name("test")?
            .try_description("test desc")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .create((), create);

        let service = CreateRoleService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from("test")?)
            .await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.name, found.name);
        assert_eq!(response.description, found.description);
        assert_eq!(response.created_on, found.created_on);
        assert_eq!(response.created_by_id, found.created_by_id);
        assert_eq!(response.modified_on, found.modified_on);
        assert_eq!(response.modified_by_id, found.modified_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }
}
