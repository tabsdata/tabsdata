//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, SecAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::role::{Role, RoleBuilder, RoleDBWithNames};
use td_objects::rest_urls::RoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::RoleIdName;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReadRoleService,
    request = ReadRequest<RoleParam>,
    response = Role,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<RoleParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<ReadRequest<RoleParam>>::extract_name::<RoleParam>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDBWithNames>),
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
    use td_objects::dxo::role::RoleDB;
    use td_objects::types::basic::{AccessTokenId, Description, RoleId, RoleName, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_read_role(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ReadRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<RoleParam>, Role>(&[
                type_of_val(&With::<ReadRequest<RoleParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(&With::<ReadRequest<RoleParam>>::extract_name::<RoleParam>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::fetch_one::<RoleDBWithNames>),
                type_of_val(&With::<RoleDBWithNames>::try_convert_to::<RoleBuilder, _>),
                type_of_val(&With::<RoleBuilder>::build::<Role>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_read_role_with_id(db: TdPool) -> Result<(), TdError> {
        let role = db
            .seed()
            .role(
                RoleName::try_from("joaquin")?,
                Description::try_from("super user")?,
            )
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .read(
            RoleParam::builder()
                .role(RoleIdName::from_str(format!("~{}", role.id).as_str())?)
                .build()?,
        );

        let service = ReadRoleService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let found = db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin").unwrap())
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

    #[td_test::test(db)]
    async fn test_read_role_with_name(db: TdPool) -> Result<(), TdError> {
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
            RoleId::sec_admin(),
        )
        .read(
            RoleParam::builder()
                .role(RoleIdName::from_str("joaquin")?)
                .build()?,
        );

        let service = ReadRoleService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let found = db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin").unwrap())
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
