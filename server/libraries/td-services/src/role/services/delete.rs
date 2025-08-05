//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::layers::assert_not_fixed;
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectService};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_objects::types::permission::PermissionDB;
use td_objects::types::role::{RoleDB, RoleDBWithNames, UserRoleDB};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = DeleteRoleService,
    request = DeleteRequest<RoleParam>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<DeleteRequest<RoleParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin>::check),
        from_fn(With::<DeleteRequest<RoleParam>>::extract_name::<RoleParam>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        // Find role to delete
        from_fn(By::<RoleIdName>::select::<RoleDBWithNames>),
        from_fn(With::<RoleDBWithNames>::extract::<RoleId>),
        // Assert role can be deleted
        from_fn(assert_not_fixed),
        // Delete all permissions with that role
        from_fn(By::<RoleId>::delete::<PermissionDB>),
        // Delete all user roles with that role
        from_fn(By::<RoleId>::delete::<UserRoleDB>),
        // Delete the role
        from_fn(By::<RoleId>::delete::<RoleDB>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::role::RoleError;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_permission::seed_permission;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::seed_user_role;
    use td_objects::types::basic::{
        AccessTokenId, Description, PermissionType, RoleName, UserEnabled, UserId, UserName,
    };
    use td_objects::types::permission::PermissionDBWithNames;
    use td_objects::types::role::UserRoleDBWithNames;
    use td_objects::types::{IdOrName, SqlEntity};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_role(db: DbPool) {
        use td_tower::metadata::type_of_val;

        DeleteRoleService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<DeleteRequest<RoleParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<RoleParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin>::check),
                type_of_val(&With::<DeleteRequest<RoleParam>>::extract_name::<RoleParam>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                // Find role to delete
                type_of_val(&By::<RoleIdName>::select::<RoleDBWithNames>),
                type_of_val(&With::<RoleDBWithNames>::extract::<RoleId>),
                // Assert role can be deleted
                type_of_val(&assert_not_fixed),
                // Delete all permissions with that role
                type_of_val(&By::<RoleId>::delete::<PermissionDB>),
                // Delete all user roles with that role
                type_of_val(&By::<RoleId>::delete::<UserRoleDB>),
                // Delete the role
                type_of_val(&By::<RoleId>::delete::<RoleDB>),
            ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_role_by_id(db: DbPool) -> Result<(), TdError> {
        let (hero, _villain) = setup_roles(&db).await;
        test_delete_role(&db, RoleIdName::try_from(format!("~{}", hero.id()))?).await
    }

    #[td_test::test(sqlx)]
    async fn test_delete_role_by_name(db: DbPool) -> Result<(), TdError> {
        let (hero, _villain) = setup_roles(&db).await;
        test_delete_role(&db, RoleIdName::try_from(format!("{}", hero.name()))?).await
    }

    async fn setup_roles(db: &DbPool) -> (RoleDB, RoleDB) {
        // Users
        let user = seed_user(
            db,
            &UserName::try_from("joaquin").unwrap(),
            &UserEnabled::from(true),
        )
        .await;

        // Roles
        let hero_role = seed_role(
            db,
            RoleName::try_from("hero").unwrap(),
            Description::try_from("Hero Role").unwrap(),
        )
        .await;
        let villain_role = seed_role(
            db,
            RoleName::try_from("villain").unwrap(),
            Description::try_from("Villain Role").unwrap(),
        )
        .await;

        // User Roles
        let _user_hero_role = seed_user_role(db, user.id(), hero_role.id()).await;
        let _user_hero_role = seed_user_role(db, user.id(), villain_role.id()).await;

        // Permissions
        let _hero_permissions = seed_permission(
            db,
            PermissionType::try_from("cr").unwrap(),
            None,
            None,
            &hero_role,
        )
        .await;
        let _villain_permissions = seed_permission(
            db,
            PermissionType::try_from("cr").unwrap(),
            None,
            None,
            &villain_role,
        )
        .await;

        (hero_role, villain_role)
    }

    async fn test_delete_role(db: &DbPool, role_id_name: RoleIdName) -> Result<(), TdError> {
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .delete(RoleParam::builder().role(role_id_name.clone()).build()?);
        let service = DeleteRoleService::with_defaults(db.clone())
            .await
            .service()
            .await;
        service.raw_oneshot(request).await?;

        if let Some(role_id) = role_id_name.id() {
            assert_deleted_role(db, role_id).await
        } else if let Some(role_name) = role_id_name.name() {
            assert_deleted_role(db, role_name).await
        } else {
            panic!("RoleIdName must have either id or name")
        }
    }

    async fn assert_deleted_role<R: SqlEntity>(db: &DbPool, role_ref: &R) -> Result<(), TdError> {
        // Assert just one of the roles in the db got deleted
        let found: Vec<RoleDB> = DaoQueries::default()
            .select_by::<RoleDB>(&role_ref)?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(found.is_empty());

        let found: Vec<RoleDB> = DaoQueries::default()
            .select_by::<RoleDB>(&())?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(!found.is_empty());

        // Assert that associated user roles got deleted, but not others
        let found: Vec<UserRoleDBWithNames> = DaoQueries::default()
            .select_by::<UserRoleDBWithNames>(&role_ref)?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(found.is_empty());

        let found: Vec<UserRoleDBWithNames> = DaoQueries::default()
            .select_by::<UserRoleDBWithNames>(&())?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(!found.is_empty());

        // Assert that associated role permissions got deleted, but not others
        let found: Vec<PermissionDBWithNames> = DaoQueries::default()
            .select_by::<PermissionDBWithNames>(&role_ref)?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(found.is_empty());

        let found: Vec<PermissionDBWithNames> = DaoQueries::default()
            .select_by::<PermissionDBWithNames>(&())?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(!found.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_delete_fixed_role(db: DbPool) -> Result<(), TdError> {
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .delete(
            RoleParam::builder()
                .try_role(RoleName::sys_admin().to_string())?
                .build()?,
        );
        let service = DeleteRoleService::with_defaults(db.clone())
            .await
            .service()
            .await;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<RoleError>();
        assert!(matches!(err, RoleError::FixedRole(_)));
        Ok(())
    }
}
