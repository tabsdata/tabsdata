//
// Copyright 2025 Tabsdata Inc.
//

use crate::user_role::assert_not_fixed;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, RbacSource, System, SystemEntity};
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::role::{RoleDB, RoleParam};
use td_objects::dxo::user::{UserDB, UserParam};
use td_objects::dxo::user_role::{UserRoleDB, UserRoleDBWithNames};
use td_queries::LiveEntry;

#[service_factory]
async fn delete_user_role(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: DeleteRequest<(RoleParam, UserParam)>,
) -> Result<(), TdError> {
    let (ctx, (role_param, user_param)) = request.into_parts();
    let mut trx = db.begin().await?;
    let grant = authz
        .require::<System, ManageAccess>(&mut trx, &ctx, &SystemEntity)
        .await?;
    let conn = trx.get(grant);

    let role: RoleDB = conn.live().fetch_one(&role_param.role).await?;
    let user: UserDB = conn.live().fetch_one(&user_param.user).await?;

    let row: UserRoleDBWithNames = conn.live().fetch_one(and!(&role.id, &user.id)).await?;
    assert_not_fixed(&row)?;
    conn.live()
        .delete::<UserRoleDB>(and!(&role.id, &user.id))
        .await?;
    trx.commit().await?;

    // Removed a role membership; with the transaction committed, invalidate
    // the cached authz grants.
    let mut conn = db.read().await?;
    authz.refresh(conn.get(grant)).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::user_role::UserRoleError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, Description, RoleIdName, RoleName, UserEnabled, UserId, UserIdName, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_delete_user_role(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin").unwrap(), &UserEnabled::TRUE)
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;
        let user_role = db.seed().user_role(&user.id, &role.id).await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete((
            RoleParam {
                role: RoleIdName::from_str("king")?,
            },
            UserParam {
                user: UserIdName::from_str("joaquin")?,
            },
        ));

        let service = Services::with_defaults(db.clone());
        service.delete_user_role(request).await?;

        let not_found = db.live().fetch_one::<UserRoleDB>(&user_role.id).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_fixed_user_role(db: TdPool) -> Result<(), TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete((
            RoleParam {
                role: RoleIdName::from_str(&RoleName::sys_admin().to_string())?,
            },
            UserParam {
                user: UserIdName::from_str(&UserName::admin().to_string())?,
            },
        ));

        let service = Services::with_defaults(db.clone());
        let res = service.delete_user_role(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<UserRoleError>();
        assert!(matches!(err, UserRoleError::FixedUserRole(_, _)));
        Ok(())
    }
}
