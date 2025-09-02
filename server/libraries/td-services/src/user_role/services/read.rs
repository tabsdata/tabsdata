//
// Copyright 2025 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::UserRoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, TryIntoService, With, combine,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{RoleId, RoleIdName, UserId, UserIdName};
use td_objects::types::role::RoleDB;
use td_objects::types::role::{UserRole, UserRoleBuilder, UserRoleDBWithNames};
use td_objects::types::user::UserDB;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = ReadUserRoleService,
    request = ReadRequest<UserRoleParam>,
    response = UserRole,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<ReadRequest<UserRoleParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<ReadRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
        from_fn(With::<UserRoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::select::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(With::<UserRoleParam>::extract::<UserIdName>),
        from_fn(By::<UserIdName>::select::<UserDB>),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(combine::<RoleId, UserId>),
        from_fn(By::<(RoleId, UserId)>::select::<UserRoleDBWithNames>),
        from_fn(With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
        from_fn(With::<UserRoleBuilder>::build::<UserRole, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::{get_user_role, seed_user_role};
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserEnabled, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_user_role(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ReadUserRoleService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<ReadRequest<UserRoleParam>, UserRole>(&[
                type_of_val(&With::<ReadRequest<UserRoleParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(&With::<ReadRequest<UserRoleParam>>::extract_name::<UserRoleParam>),
                type_of_val(&With::<UserRoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::select::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&With::<UserRoleParam>::extract::<UserIdName>),
                type_of_val(&By::<UserIdName>::select::<UserDB>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&combine::<RoleId, UserId>),
                type_of_val(&By::<(RoleId, UserId)>::select::<UserRoleDBWithNames>),
                type_of_val(&With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
                type_of_val(&With::<UserRoleBuilder>::build::<UserRole, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read_user_role(db: DbPool) -> Result<(), TdError> {
        let user = seed_user(
            &db,
            &UserName::try_from("joaquin").unwrap(),
            &UserEnabled::from(false),
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
        .read(
            UserRoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .user(UserIdName::try_from("joaquin")?)
                .build()?,
        );

        let service = ReadUserRoleService::with_defaults(db.clone())
            .await
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_user_role(&db, user_role.id()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.user_id(), found.user_id());
        assert_eq!(response.role_id(), found.role_id());
        assert_eq!(response.added_on(), found.added_on());
        assert_eq!(response.added_by_id(), found.added_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
