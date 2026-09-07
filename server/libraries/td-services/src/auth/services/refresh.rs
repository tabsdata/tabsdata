//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::assert_session_active::assert_session_active;
use crate::auth::assert_user_enabled::assert_user_enabled;
use crate::auth::create_access_token::create_access_token;
use crate::auth::decode_refresh_token::decode_refresh_token;
use crate::auth::set_session_expiration::session_expiration;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::session_provider::Sessions;
use td_common::jwt::JwtConfig;
use td_common::provider::Provider;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::auth::{SessionDB, SessionNewTokenDB, TokenResponseX};
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::user::UserDB;
use td_objects::types::basic::RefreshToken;
use td_queries::LiveEntry;

#[service_factory]
async fn refresh(
    authz: Ctx<AuthzContext>,
    jwt_config: Ctx<JwtConfig>,
    sessions: Ctx<Sessions>,
    db: DbPool<Global>,
    request: UpdateRequest<RefreshToken>,
) -> Result<TokenResponseX, TdError> {
    let (ctx, _, refresh_token) = request.into_parts();
    let mut gated_trx = db.begin().await?;
    let conn = gated_trx.get(authz.none());
    let access_token_id = ctx.access_token_id;
    let at = ctx.time;

    let refresh_token_id = decode_refresh_token(&jwt_config, &refresh_token)?;

    // find and lock the session by access token ID and refresh token ID; the
    // lock serializes concurrent refreshes so the token can be redeemed once
    let session: SessionDB = conn
        .live()
        .fetch_one_for_update(and!(&access_token_id, &refresh_token_id))
        .await?;
    assert_session_active(&session)?;
    let user: UserDB = conn.live().fetch_one(&ctx.user_id).await?;
    assert_user_enabled(&user)?;

    let new_token = SessionNewTokenDB::new(at);
    conn.live()
        .update::<SessionNewTokenDB, SessionDB>(&new_token, access_token_id)
        .await?;

    let new_session = SessionDB::new(ctx.user_id, at, session_expiration(&jwt_config)?);
    conn.live().insert(&new_session).await?;

    let token = create_access_token(&jwt_config, &new_session)?;

    // Housekeeping purge commits with the transaction; the cache invalidation
    // must wait for the commit (see LoginService).
    sessions.purge(conn).await?;
    gated_trx.commit().await?;
    sessions.invalidate().await;

    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::auth::services::tests::assert_session;
    use td_common::jwt::decode_token;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::auth::Login;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::UserId;
    use td_objects::types::basic::{AccessTokenId, Password, SessionStatus, UserName};
    use td_queries::LiveEntry;

    #[td_test::test(db)]
    async fn test_refresh_ok(db: TdPool) -> Result<(), TdError> {
        let context = Services::with_defaults(db.clone());
        let auth_services = context.clone();

        // doing a login before password change to verify it will be invalidated
        let service = auth_services.clone();

        let request = Login {
            name: UserName::try_from("admin")?,
            password: Password::try_from("tabsdata")?,
        };
        let res = service.login(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let original_access_token_id =
            AccessTokenId::from(decode_token(&context.jwt_config, access_token)?.jti);
        let refresh_token = &token_response.refresh_token;

        let service = auth_services.clone();

        let request = RequestContext::with(original_access_token_id, UserId::admin())
            .update((), refresh_token.clone());
        let res = service.refresh(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id =
            AccessTokenId::from(decode_token(&context.jwt_config, access_token)?.jti);
        assert_session(&db, &Some(access_token_id)).await;

        let session = db
            .live()
            .fetch_optional::<SessionDB>(original_access_token_id)
            .await?;
        match session {
            Some(session) => {
                assert_eq!(session.status, SessionStatus::InvalidNewToken);
            }
            None => {
                panic!("Session not found");
            }
        }

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_refresh_rejects_reused_token(db: TdPool) -> Result<(), TdError> {
        let context = Services::with_defaults(db.clone());
        let auth_services = context.clone();

        let request = Login {
            name: UserName::try_from("admin")?,
            password: Password::try_from("tabsdata")?,
        };
        let token_response = auth_services.login(request).await?.data;
        let original_access_token_id = AccessTokenId::from(
            decode_token(&context.jwt_config, &token_response.access_token)?.jti,
        );
        let refresh_token = token_response.refresh_token;

        let request = RequestContext::with(original_access_token_id, UserId::admin())
            .update((), refresh_token.clone());
        auth_services.refresh(request).await?.data;

        let request = RequestContext::with(original_access_token_id, UserId::admin())
            .update((), refresh_token);
        let reused = auth_services.refresh(request).await;
        assert!(
            reused.is_err(),
            "a redeemed refresh token must not be reusable"
        );

        Ok(())
    }
}
