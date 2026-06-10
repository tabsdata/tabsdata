//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, SecAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::And;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::{UserRole, UserRoleBuilder, UserRoleDBWithNames};
use td_objects::rest_urls::UserRoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{RoleId, RoleIdName, UserId, UserIdName};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReadUserRoleService,
    request = ReadRequest<UserRoleParam>,
    response = UserRole,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<UserRoleParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<ReadRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
        from_fn(With::<UserRoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(With::<UserRoleParam>::extract::<UserIdName>),
        from_fn(By::<UserIdName>::fetch_one::<UserDB>),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(By::<And![RoleId, UserId]>::fetch_one::<UserRoleDBWithNames>),
        from_fn(With::<UserRoleDBWithNames>::try_convert_to::<UserRoleBuilder, _>),
        from_fn(With::<UserRoleBuilder>::build::<UserRole>),
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
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserEnabled, UserName};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_read_user_role(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ReadUserRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<UserRoleParam>, UserRole>(&[
                type_of_val(&With::<ReadRequest<UserRoleParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(&With::<ReadRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
                type_of_val(&With::<UserRoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::fetch_one::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&With::<UserRoleParam>::extract::<UserIdName>),
                type_of_val(&By::<UserIdName>::fetch_one::<UserDB>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&By::<And![RoleId, UserId]>::fetch_one::<UserRoleDBWithNames>),
                type_of_val(&With::<UserRoleDBWithNames>::try_convert_to::<UserRoleBuilder, _>),
                type_of_val(&With::<UserRoleBuilder>::build::<UserRole>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_read_user_role(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(
                &UserName::try_from("joaquin").unwrap(),
                &UserEnabled::from(false),
            )
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;
        let user_role = db.seed().user_role(&user.id, &role.id).await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .read(
            UserRoleParam::builder()
                .role(RoleIdName::from_str("king")?)
                .user(UserIdName::from_str("joaquin")?)
                .build()?,
        );

        let service = ReadUserRoleService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = db.live().fetch_one::<UserRoleDB>(&user_role.id).await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.user_id, found.user_id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.added_on, found.added_on);
        assert_eq!(response.added_by_id, found.added_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }
}
