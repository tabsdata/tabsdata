//
// Copyright 2025 Tabsdata Inc.
//

use crate::role::assert_not_fixed;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageRoles, RbacSource, System, SystemEntity};
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::role::RoleParam;
use td_objects::dxo::role::{RoleDB, RoleDBWithNames};
use td_objects::dxo::user_role::UserRoleDB;
use td_queries::Queries;

#[service_factory]
async fn delete_role(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: DeleteRequest<RoleParam>,
) -> Result<(), TdError> {
    let (ctx, param) = request.into_parts();
    retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let grant = authz
            .require::<System, ManageRoles>(&mut trx, &ctx, &SystemEntity)
            .await?;
        let conn = trx.get(grant);

        let role: RoleDBWithNames = conn.fetch_one(&param.role).await?;
        assert_not_fixed(&role)?;

    // Drop the role's permissions and memberships, then the role itself.
    conn.delete::<PermissionDB>(role.base.id).await?;
    conn.delete::<UserRoleDB>(role.base.id).await?;
    conn.delete::<RoleDB>(role.base.id).await?;
    trx.commit().await?;

    // Dropped the role's permissions and memberships; with the transaction
    // committed, invalidate the cached authz grants.
    let mut conn = db.read().await?;
    authz.refresh(conn.get(grant)).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::role::RoleError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::permission::PermissionDBWithNames;
    use td_objects::dxo::user_role::UserRoleDBWithNames;
    use td_objects::types::basic::RoleIdName;
    use td_objects::types::basic::{
        AccessTokenId, Description, PermissionType, RoleName, UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

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
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();

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

        let _user_hero_role = db.seed().user_role(&user.id, &hero_role.id).await.unwrap();
        let _user_hero_role = db
            .seed()
            .user_role(&user.id, &villain_role.id)
            .await
            .unwrap();

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
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(RoleParam {
                role: role_id_name.clone(),
            });
        let service = Services::with_defaults(db.clone());
        service.delete_role(request).await?;

        // Assert just one of the roles in the db got deleted
        let found = db.fetch_all::<RoleDB>(&role_id_name).await?;
        assert!(found.is_empty());
        let found = db.fetch_all::<RoleDB>(()).await?;
        assert!(!found.is_empty());

        // Assert that associated user roles got deleted, but not others
        let found = db.fetch_all::<UserRoleDBWithNames>(&role_id_name).await?;
        assert!(found.is_empty());
        let found = db.fetch_all::<UserRoleDBWithNames>(()).await?;
        assert!(!found.is_empty());

        // Assert that associated role permissions got deleted, but not others
        let found = db.fetch_all::<PermissionDBWithNames>(&role_id_name).await?;
        assert!(found.is_empty());
        let found = db.fetch_all::<PermissionDBWithNames>(()).await?;
        assert!(!found.is_empty());

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_fixed_role(db: TdPool) -> Result<(), TdError> {
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(RoleParam {
                role: RoleIdName::from_str(&RoleName::sys_admin().to_string())?,
            });
        let service = Services::with_defaults(db.clone());
        let res = service.delete_role(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<RoleError>();
        assert!(matches!(err, RoleError::FixedRole(_)));
        Ok(())
    }
}
