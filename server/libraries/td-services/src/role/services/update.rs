//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Rbac, ManageRoles, Scope, System};
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::role::{
    Role, RoleBuilder, RoleDB, RoleDBUpdate, RoleDBUpdateBuilder, RoleDBWithNames, RoleUpdate,
};
use td_objects::rest_urls::RoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{RoleId, RoleIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = UpdateRoleService,
    request = UpdateRequest<RoleParam, RoleUpdate>,
    response = Role,
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<RoleParam, RoleUpdate>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, ManageRoles>::check),
        from_fn(With::<UpdateRequest<RoleParam, RoleUpdate>>::extract_name::<RoleParam>),
        from_fn(With::<UpdateRequest<RoleParam, RoleUpdate>>::extract_data::<RoleUpdate>),
        from_fn(With::<RoleUpdate>::try_convert_to::<RoleDBUpdateBuilder, _>),
        from_fn(With::<RequestContext>::update::<RoleDBUpdateBuilder, _>),
        from_fn(With::<RoleDBUpdateBuilder>::build::<RoleDBUpdate>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDBWithNames>),
        from_fn(With::<RoleDBWithNames>::extract::<RoleId>),
        from_fn(By::<RoleId>::update::<RoleDBUpdate, RoleDB>),
        from_fn(By::<RoleId>::fetch_one::<RoleDBWithNames>),
        from_fn(With::<RoleDBWithNames>::try_convert_to::<RoleBuilder, _>),
        from_fn(With::<RoleBuilder>::build::<Role>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_update_role(db: TdPool) {
        use td_tower::metadata::type_of_val;

        UpdateRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<UpdateRequest<RoleParam, RoleUpdate>, Role>(&[
                type_of_val(
                    &With::<UpdateRequest<RoleParam, RoleUpdate>>::extract::<RequestContext>,
                ),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, ManageRoles>::check),
                type_of_val(
                    &With::<UpdateRequest<RoleParam, RoleUpdate>>::extract_name::<RoleParam>,
                ),
                type_of_val(
                    &With::<UpdateRequest<RoleParam, RoleUpdate>>::extract_data::<RoleUpdate>,
                ),
                type_of_val(&With::<RoleUpdate>::try_convert_to::<RoleDBUpdateBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<RoleDBUpdateBuilder, _>),
                type_of_val(&With::<RoleDBUpdateBuilder>::build::<RoleDBUpdate>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::fetch_one::<RoleDBWithNames>),
                type_of_val(&With::<RoleDBWithNames>::extract::<RoleId>),
                type_of_val(&By::<RoleId>::update::<RoleDBUpdate, RoleDB>),
                type_of_val(&By::<RoleId>::fetch_one::<RoleDBWithNames>),
                type_of_val(&With::<RoleDBWithNames>::try_convert_to::<RoleBuilder, _>),
                type_of_val(&With::<RoleBuilder>::build::<Role>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_update_role(db: TdPool) -> Result<(), TdError> {
        let _role = db
            .seed()
            .role(
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
            UserId::admin()
        )
        .update(
            RoleParam::builder()
                .role(RoleIdName::from_str("joaquin")?)
                .build()?,
            update,
        );

        let service = UpdateRoleService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let not_found = db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin")?)
            .await;
        assert!(not_found.is_err());

        let found = db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from("not_joaquin_anymore")?)
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
