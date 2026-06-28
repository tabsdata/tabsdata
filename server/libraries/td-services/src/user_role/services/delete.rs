//
// Copyright 2025 Tabs Data Inc.
//

use crate::user_role::layers::assert_not_fixed;
use ta_services::factory::service_factory;
use td_authz::rbac::{ManageAccess, Rbac, Scope, System};
use td_authz::{AuthzContext, refresh_authz_context};
use td_objects::And;
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::{UserRoleDB, UserRoleDBWithNames};
use td_objects::rest_urls::UserRoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{RoleId, RoleIdName, UserId, UserIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DeleteUserRoleService,
    request = DeleteRequest<UserRoleParam>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<UserRoleParam>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, ManageAccess>::check),
        from_fn(With::<DeleteRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
        from_fn(With::<UserRoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(With::<UserRoleParam>::extract::<UserIdName>),
        from_fn(By::<UserIdName>::fetch_one::<UserDB>),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(By::<And![RoleId, UserId]>::fetch_one::<UserRoleDBWithNames>),
        from_fn(assert_not_fixed),
        from_fn(By::<And![RoleId, UserId]>::delete::<UserRoleDB>),
        // Removed a role membership; invalidate the cached authz grants.
        from_fn(refresh_authz_context),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::user_role::UserRoleError;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserEnabled, UserName};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_delete_user_role(db: TdPool) {
        use td_tower::metadata::type_of_val;

        DeleteUserRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<DeleteRequest<UserRoleParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<UserRoleParam>>::extract::<RequestContext>),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, ManageAccess>::check),
                type_of_val(&With::<DeleteRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
                type_of_val(&With::<UserRoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::fetch_one::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&With::<UserRoleParam>::extract::<UserIdName>),
                type_of_val(&By::<UserIdName>::fetch_one::<UserDB>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&By::<And![RoleId, UserId]>::fetch_one::<UserRoleDBWithNames>),
                type_of_val(&assert_not_fixed),
                type_of_val(&By::<And![RoleId, UserId]>::delete::<UserRoleDB>),
                type_of_val(&refresh_authz_context),
            ]);
    }

    #[td_test::test(db)]
    async fn test_delete_user_role(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(
                &UserName::try_from("joaquin").unwrap(),
                &UserEnabled::from(true),
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

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(
            UserRoleParam::builder()
                .role(RoleIdName::from_str("king")?)
                .user(UserIdName::from_str("joaquin")?)
                .build()?,
        );

        let service = DeleteUserRoleService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        let not_found = db.live().fetch_one::<UserRoleDB>(&user_role.id).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_fixed_user_role(db: TdPool) -> Result<(), TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(
            UserRoleParam::builder()
                .role(RoleIdName::from_str(&RoleName::sys_admin().to_string())?)
                .user(UserIdName::from_str(&UserName::admin().to_string())?)
                .build()?,
        );

        let service = DeleteUserRoleService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<UserRoleError>();
        assert!(matches!(err, UserRoleError::FixedUserRole(_, _)));
        Ok(())
    }
}
