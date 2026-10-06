//
// Copyright 2026 Tabsdata Inc.
//

use crate::token::expiry::expires_on;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{SelfUser, User};
use td_authz::token_provider::ServiceTokens;
use td_database::access::{DbPool, Global};
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::service_token::{ServiceTokenDB, TokenCreate, TokenCreated, TokenSlotParam};
use td_objects::types::basic::{
    Created, ServiceToken, ServiceTokenId, TokenDigest, TokenHint, TokenScope, TokenSlot,
    TokenSystem,
};
use td_queries::Queries;
use td_security::token;
use td_security::token::config::ServiceTokenConfig;
use tracing::info;

/// Its route admits sessions only, so a token cannot mint its successor: an
/// identity token carries [`SelfUser`].
#[service_factory]
async fn create_service_token(
    authz: Ctx<AuthzContext>,
    settings: Ctx<ServiceTokenConfig>,
    tokens: Ctx<ServiceTokens>,
    db: DbPool<Global>,
    request: CreateRequest<TokenCreate, TokenSlotParam>,
) -> Result<TokenCreated, TdError> {
    let (ctx, param, create) = request.into_parts();
    let expires_on = expires_on(&settings, &ctx.time, &create.expires_in_days)?;

    let mut trx = db.begin().await?;
    let grant = authz
        .require::<User, SelfUser>(&mut trx, &ctx, &ctx.user_id)
        .await?;
    let conn = trx.get(grant);

    let token = ServiceToken::try_from(token::generate())?;
    let hint = token::hint(&token).ok_or_else(|| {
        api_error!(
            ApiError::InternalError,
            "a freshly generated token has no hint"
        )
    })?;
    let row = ServiceTokenDB {
        id: ServiceTokenId::default(),
        user_id: ctx.user_id,
        slot: param.slot,
        digest: TokenDigest::try_from(token::digest(&token))?,
        hint: TokenHint::try_from(hint)?,
        comment: create.comment,
        created_on: Created::from(ctx.time),
        expires_on,
        last_used_on: None,
        scope: TokenScope::Delegated,
        system: TokenSystem::FALSE,
    };

    // A concurrent create that slips past this check loses on the unique
    // `(user_id, slot)`.
    if conn
        .fetch_optional::<ServiceTokenDB>(and!(&row.user_id, &row.slot))
        .await?
        .is_some()
    {
        Err(occupied(&row.slot))?
    }
    conn.insert(&row).await?;
    trx.commit().await?;
    tokens.invalidate().await;

    info!(
        user_id = %row.user_id,
        slot = %row.slot,
        token_id = %row.id,
        "service token created"
    );
    Ok(TokenCreated {
        id: row.id,
        token,
        slot: row.slot,
        hint: row.hint,
        expires_on: row.expires_on,
        comment: row.comment,
    })
}

fn occupied(slot: &TokenSlot) -> TdError {
    api_error!(
        ApiError::Any(409),
        "token slot {} is already in use; revoke it before creating another",
        **slot
    )
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use chrono::Duration;
    use td_authz::rbac::enforce::RbacError;
    use td_common::datetime::Timestamp;
    use td_database::pool::TdPool;
    use td_error::{ApiError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::service_token::{ServiceTokenDB, TokenCreate, TokenSlotParam};
    use td_objects::types::basic::{
        AccessTokenId, ExpiresInDays, TokenComment, TokenExpiresOn, TokenScope, TokenSlot,
        TokenSystem, UserId,
    };
    use td_queries::Queries;
    use td_security::token;

    fn create(days: i64) -> TokenCreate {
        TokenCreate {
            expires_in_days: ExpiresInDays::try_from(days).unwrap(),
            comment: Some(TokenComment::try_from("ci").unwrap()),
        }
    }

    fn slot(slot: i32) -> TokenSlotParam {
        TokenSlotParam {
            slot: TokenSlot::try_from(slot).unwrap(),
        }
    }

    #[td_test::test(db)]
    async fn test_create_token_stores_only_the_digest(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(slot(1), create(30));
        let at = request.context.time;
        let created = services.create_service_token(request).await.unwrap().data;

        let plaintext = created.token.to_string();
        assert!(token::parse(&plaintext).is_some());
        assert_eq!(*created.slot, 1);
        assert_eq!(created.hint.as_str(), token::hint(&plaintext).unwrap());
        assert_eq!(
            created.expires_on,
            TokenExpiresOn::from(Timestamp::from(**at + Duration::days(30)))
        );

        let row: ServiceTokenDB = db.fetch_one(&created.id).await.unwrap();
        assert_eq!(row.user_id, UserId::admin());
        assert_eq!(row.digest.as_str(), token::digest(&plaintext));
        assert_eq!(row.scope, TokenScope::Delegated);
        assert_eq!(row.system, TokenSystem::FALSE);
        assert!(row.last_used_on.is_none());
    }

    #[td_test::test(db)]
    async fn test_create_token_never_expiring(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(slot(2), create(-1));
        let created = services.create_service_token(request).await.unwrap().data;
        assert_eq!(created.expires_on, TokenExpiresOn::never());
    }

    #[td_test::test(db)]
    async fn test_create_token_occupied_slot_is_a_conflict(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());
        services
            .create_service_token(ctx.create(slot(1), create(1)))
            .await
            .unwrap();
        let err = services
            .create_service_token(ctx.create(slot(1), create(1)))
            .await
            .unwrap_err();
        assert!(matches!(err.api_error(), ApiError::Any(409)));
        services
            .create_service_token(ctx.create(slot(2), create(1)))
            .await
            .unwrap();
    }

    #[td_test::test(db)]
    async fn test_create_token_is_refused_to_a_service_token(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let request = RequestContext::service_token(
            AccessTokenId::default(),
            UserId::admin(),
            TokenScope::Delegated,
        )
        .create(slot(1), create(1));
        assert_service_error(
            services.create_service_token(request).await,
            |err| match err {
                RbacError::Forbidden(_, _) => {}
                other => panic!("expected Forbidden, got {other:?}"),
            },
        );
    }
}
