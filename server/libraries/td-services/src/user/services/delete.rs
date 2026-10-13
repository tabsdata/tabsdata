//
// Copyright 2024 Tabsdata Inc.
//

use crate::auth::retire_sessions::retire_sessions;
use crate::user::UserError;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, System, SystemEntity};
use td_database::access::{DbPool, Global, retrying};
use td_error::TdError;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::service_token::ServiceTokenDB;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user::UserParam;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{SessionStatus, UserId};
use td_queries::Queries;

#[service_factory]
async fn delete_user(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: DeleteRequest<UserParam>,
) -> Result<(), TdError> {
    let (ctx, param) = request.into_parts();
    retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let grant = authz
            .require::<System, ManageAccess>(&mut trx, &ctx, &SystemEntity)
            .await?;
        let conn = trx.get(grant);

        let user: UserDB = conn.fetch_one(&param.user).await?;
        delete_user_validate(&user, &ctx.user_id)?;

        retire_sessions(conn, &user.id, ctx.time, SessionStatus::InvalidUserDisabled).await?;
        conn.delete::<UserRoleDB>(user.id).await?;
        // Restricting foreign key: token rows go first.
        conn.delete::<ServiceTokenDB>(user.id).await?;
        conn.delete::<UserDB>(user.id).await?;
        trx.commit().await
    })
    .await?;
    Ok(())
}

pub fn delete_user_validate(user_db: &UserDB, user_id: &UserId) -> Result<(), TdError> {
    if *user_db.system {
        Err(UserError::SystemUserProtected)?
    }
    if user_db.id == *user_id {
        Err(UserError::NotAllowedToDeleteThemselves)?
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user::{UserDB, UserParam};
    use td_objects::types::basic::{AccessTokenId, UserEnabled, UserId, UserIdName, UserName};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_delete_user(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();

        let service = Services::with_defaults(db.clone());

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(UserParam {
                user: UserIdName::from_str("u0").unwrap(),
            });
        let response = service.delete_user(request).await;
        assert!(response.is_ok());

        let res: Option<UserDB> = db
            .fetch_optional::<UserDB>(&UserName::try_from("u0").unwrap())
            .await
            .unwrap();
        assert!(res.is_none());

        let res: Option<UserRoleDB> = db.fetch_first::<UserRoleDB>(&user.id).await.unwrap();
        assert!(res.is_none());
    }
}
