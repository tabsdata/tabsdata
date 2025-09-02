//
// Copyright 2025 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractNameService, ExtractService, TryIntoService,
    UpdateService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_objects::types::role::{
    Role, RoleBuilder, RoleDB, RoleDBUpdate, RoleDBUpdateBuilder, RoleDBWithNames, RoleUpdate,
};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = UpdateRoleService,
    request = UpdateRequest<RoleParam, RoleUpdate>,
    response = Role,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<RoleParam, RoleUpdate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin>::check),
        from_fn(With::<UpdateRequest<RoleParam, RoleUpdate>>::extract_name::<RoleParam>),
        from_fn(With::<UpdateRequest<RoleParam, RoleUpdate>>::extract_data::<RoleUpdate>),
        from_fn(With::<RoleUpdate>::convert_to::<RoleDBUpdateBuilder, _>),
        from_fn(With::<RequestContext>::update::<RoleDBUpdateBuilder, _>),
        from_fn(With::<RoleDBUpdateBuilder>::build::<RoleDBUpdate, _>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::select::<RoleDBWithNames>),
        from_fn(With::<RoleDBWithNames>::extract::<RoleId>),
        from_fn(By::<RoleId>::update::<RoleDBUpdate, RoleDB>),
        from_fn(By::<RoleId>::select::<RoleDBWithNames>),
        from_fn(With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
        from_fn(With::<RoleBuilder>::build::<Role, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::{get_role, seed_role};
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserId};
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_update_role(db: DbPool) {
        use td_tower::metadata::type_of_val;

        UpdateRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<RoleParam, RoleUpdate>, Role>(&[
                type_of_val(
                    &With::<UpdateRequest<RoleParam, RoleUpdate>>::extract::<RequestContext>,
                ),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin>::check),
                type_of_val(
                    &With::<UpdateRequest<RoleParam, RoleUpdate>>::extract_name::<RoleParam>,
                ),
                type_of_val(
                    &With::<UpdateRequest<RoleParam, RoleUpdate>>::extract_data::<RoleUpdate>,
                ),
                type_of_val(&With::<RoleUpdate>::convert_to::<RoleDBUpdateBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<RoleDBUpdateBuilder, _>),
                type_of_val(&With::<RoleDBUpdateBuilder>::build::<RoleDBUpdate, _>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::select::<RoleDBWithNames>),
                type_of_val(&With::<RoleDBWithNames>::extract::<RoleId>),
                type_of_val(&By::<RoleId>::update::<RoleDBUpdate, RoleDB>),
                type_of_val(&By::<RoleId>::select::<RoleDBWithNames>),
                type_of_val(&With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
                type_of_val(&With::<RoleBuilder>::build::<Role, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_update_role(db: DbPool) -> Result<(), TdError> {
        let _role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let update = RoleUpdate::builder()
            .name(RoleName::try_from("not_joaquin_anymore")?)
            .description(Description::try_from("new desc")?)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .update(
            RoleParam::builder()
                .role(RoleIdName::try_from("joaquin")?)
                .build()?,
            update,
        );

        let service = UpdateRoleService::with_defaults(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let not_found = get_role(&db, &RoleName::try_from("joaquin")?).await;
        assert!(not_found.is_err());

        let found = get_role(&db, &RoleName::try_from("not_joaquin_anymore")?).await?;
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
