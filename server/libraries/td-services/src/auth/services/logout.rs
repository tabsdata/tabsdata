//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{DbPool, Global, retrying};
use td_error::TdError;
use td_objects::dxo::auth::SessionInvalidateDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::SessionStatus;
use td_queries::Queries;

#[service_factory]
async fn logout(
    authz: Ctx<AuthzContext>,
    sessions: Ctx<Sessions>,
    db: DbPool<Global>,
    ctx: RequestContext,
) -> Result<(), TdError> {
    retrying(|attempt| async {
        let mut gated_trx = db.begin(attempt).await?;
        let conn = gated_trx.get(authz.none());

    let logout = SessionInvalidateDB::new(ctx.time, SessionStatus::InvalidLogout);
    conn.update(&logout, ctx.access_token_id).await?;

    // Housekeeping purge commits with the transaction; the cache invalidation
    // must wait for the commit (see LoginService).
    sessions.purge(conn).await?;
    gated_trx.commit().await?;
    sessions.invalidate().await;

        gated_trx.commit().await?;
        Ok(())
    })
    .await?;
    purge_expired_sessions(&db).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_common::jwt::decode_token;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::auth::{Login, SessionDB};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::AccessTokenId;
    use td_objects::types::basic::{Password, SessionStatus, UserId, UserName};
    use td_queries::Queries;

    #[td_test::test(db)]
    async fn test_logout_ok(db: TdPool) -> Result<(), TdError> {
        let context = Services::with_defaults(db.clone());
        let auth_services = context.clone();
        let service = auth_services.clone();

        let request = Login {
            name: UserName::try_from("admin")?,
            password: Password::try_from("tabsdata")?,
        };
        let res = service.login(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id =
            AccessTokenId::from(decode_token(&context.jwt_config, access_token)?.jti);

        let service = auth_services.clone();

        let ctx = RequestContext::with(access_token_id, UserId::admin());
        let res = service.logout(ctx).await;
        assert!(res.is_ok());

        let session = db.fetch_optional::<SessionDB>(access_token_id).await?;
        match session {
            Some(session) => {
                assert_eq!(session.status, SessionStatus::InvalidLogout);
            }
            None => {
                panic!("Session not found");
            }
        }
        Ok(())
    }
}
