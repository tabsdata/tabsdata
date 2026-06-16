//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::layers::assert_not_fixed;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Rbac, ManageRoles, Scope, System};
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::role::{RoleDB, RoleDBWithNames};
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::rest_urls::RoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{RoleId, RoleIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DeleteRoleService,
    request = DeleteRequest<RoleParam>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<RoleParam>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, ManageRoles>::check),
        from_fn(With::<DeleteRequest<RoleParam>>::extract_name::<RoleParam>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        // Find role to delete
        from_fn(By::<RoleIdName>::fetch_one::<RoleDBWithNames>),
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
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::permission::PermissionDBWithNames;
    use td_objects::dxo::user_role::UserRoleDBWithNames;
    use td_objects::types::basic::{
        AccessTokenId, Description, PermissionType, RoleName, UserEnabled, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_delete_role(db: TdPool) {
        use td_tower::metadata::type_of_val;

        DeleteRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<DeleteRequest<RoleParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<RoleParam>>::extract::<RequestContext>),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, ManageRoles>::check),
                type_of_val(&With::<DeleteRequest<RoleParam>>::extract_name::<RoleParam>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                // Find role to delete
                type_of_val(&By::<RoleIdName>::fetch_one::<RoleDBWithNames>),
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

    #[td_test::test(db)]
    async fn test_delete_role_by_id(db: TdPool) -> Result<(), TdError> {
        let (hero, _villain) = setup_roles(&db).await;
        test_delete_role(&db, RoleIdName::from_str(&format!("~{}", hero.id))?).await
    }

    #[td_test::test(db)]
    async fn test_delete_role_by_name(db: TdPool) -> Result<(), TdError> {
        let (hero, _villain) = setup_roles(&db).await;
        test_delete_role(&db, RoleIdName::from_str(&format!("{}", hero.name))?).await
    }

    async fn setup_roles(db: &TdPool) -> (RoleDB, RoleDB) {
        // Users
        let user = db
            .seed()
            .user(
                &UserName::try_from("joaquin").unwrap(),
                &UserEnabled::from(true),
            )
            .await
            .unwrap();

        // Roles
        let hero_role = db
            .seed()
            .role(
                RoleName::try_from("hero").unwrap(),
                Description::try_from("Hero Role").unwrap(),
            )
            .await
            .unwrap();
        let villain_role = db
            .seed()
            .role(
                RoleName::try_from("villain").unwrap(),
                Description::try_from("Villain Role").unwrap(),
            )
            .await
            .unwrap();

        // User Roles
        let _user_hero_role = db.seed().user_role(&user.id, &hero_role.id).await.unwrap();
        let _user_hero_role = db
            .seed()
            .user_role(&user.id, &villain_role.id)
            .await
            .unwrap();

        // Permissions
        let _hero_permissions = db
            .seed()
            .permission(
                PermissionType::try_from("manage_roles").unwrap(),
                None,
                None,
                &hero_role,
            )
            .await
            .unwrap();
        let _villain_permissions = db
            .seed()
            .permission(
                PermissionType::try_from("manage_roles").unwrap(),
                None,
                None,
                &villain_role,
            )
            .await
            .unwrap();

        (hero_role, villain_role)
    }

    async fn test_delete_role(db: &TdPool, role_id_name: RoleIdName) -> Result<(), TdError> {
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin()
        )
        .delete(RoleParam::builder().role(role_id_name.clone()).build()?);
        let service = DeleteRoleService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        // Assert just one of the roles in the db got deleted
        let found = db.live().fetch_all::<RoleDB>(&role_id_name).await?;
        assert!(found.is_empty());
        let found = db.live().fetch_all::<RoleDB>(()).await?;
        assert!(!found.is_empty());

        // Assert that associated user roles got deleted, but not others
        let found = db
            .live()
            .fetch_all::<UserRoleDBWithNames>(&role_id_name)
            .await?;
        assert!(found.is_empty());
        let found = db.live().fetch_all::<UserRoleDBWithNames>(()).await?;
        assert!(!found.is_empty());

        // Assert that associated role permissions got deleted, but not others
        let found = db
            .live()
            .fetch_all::<PermissionDBWithNames>(&role_id_name)
            .await?;
        assert!(found.is_empty());
        let found = db.live().fetch_all::<PermissionDBWithNames>(()).await?;
        assert!(!found.is_empty());

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_fixed_role(db: TdPool) -> Result<(), TdError> {
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin()
        )
        .delete(
            RoleParam::builder()
                .role(RoleIdName::from_str(&RoleName::sys_admin().to_string())?)
                .build()?,
        );
        let service = DeleteRoleService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<RoleError>();
        assert!(matches!(err, RoleError::FixedRole(_)));
        Ok(())
    }
}
