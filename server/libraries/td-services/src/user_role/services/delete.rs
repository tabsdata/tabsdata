//
// Copyright 2025 Tabs Data Inc.
//

use crate::user_role::layers::assert_not_fixed;
use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::UserRoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With, combine};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectService};
use td_objects::types::basic::{RoleId, RoleIdName, UserId, UserIdName};
use td_objects::types::role::{RoleDB, UserRoleDB, UserRoleDBWithNames};
use td_objects::types::user::UserDB;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = DeleteUserRoleService,
    request = DeleteRequest<UserRoleParam>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<UserRoleParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin>::check),
        from_fn(With::<DeleteRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
        from_fn(With::<UserRoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::select::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(With::<UserRoleParam>::extract::<UserIdName>),
        from_fn(By::<UserIdName>::select::<UserDB>),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(combine::<RoleId, UserId>),
        from_fn(By::<(RoleId, UserId)>::select::<UserRoleDBWithNames>),
        from_fn(assert_not_fixed),
        from_fn(By::<(RoleId, UserId)>::delete::<UserRoleDB>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::user_role::UserRoleError;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::{get_user_role, seed_user_role};
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserEnabled, UserName};
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_delete_user_role(db: DbPool) {
        use td_tower::metadata::type_of_val;

        DeleteUserRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<DeleteRequest<UserRoleParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<UserRoleParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin>::check),
                type_of_val(&With::<DeleteRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
                type_of_val(&With::<UserRoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::select::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&With::<UserRoleParam>::extract::<UserIdName>),
                type_of_val(&By::<UserIdName>::select::<UserDB>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&combine::<RoleId, UserId>),
                type_of_val(&By::<(RoleId, UserId)>::select::<UserRoleDBWithNames>),
                type_of_val(&assert_not_fixed),
                type_of_val(&By::<(RoleId, UserId)>::delete::<UserRoleDB>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_user_role(db: DbPool) -> Result<(), TdError> {
        let user = seed_user(
            &db,
            &UserName::try_from("joaquin").unwrap(),
            &UserEnabled::from(true),
        )
        .await;
        let role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;
        let user_role = seed_user_role(&db, user.id(), role.id()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .delete(
            UserRoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .user(UserIdName::try_from("joaquin")?)
                .build()?,
        );

        let service = DeleteUserRoleService::with_defaults(db.clone())
            .service()
            .await;
        service.raw_oneshot(request).await?;

        let not_found = get_user_role(&db, user_role.id()).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_fixed_user_role(db: DbPool) -> Result<(), TdError> {
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .delete(
            UserRoleParam::builder()
                .role(RoleIdName::try_from(RoleName::sys_admin().to_string())?)
                .user(UserIdName::try_from(UserName::admin().to_string())?)
                .build()?,
        );

        let service = DeleteUserRoleService::with_defaults(db.clone())
            .service()
            .await;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<UserRoleError>();
        assert!(matches!(err, UserRoleError::FixedUserRole(_, _)));
        Ok(())
    }
}
