//
// Copyright 2026 Tabsdata Inc.
//

use crate::token::TokenError;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{SelfUser, User};
use td_authz::token_provider::ServiceTokens;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::service_token::{ServiceTokenDB, TokenSlotParam};
use td_queries::Queries;
use tracing::info;

#[service_factory]
async fn revoke_service_token(
    authz: Ctx<AuthzContext>,
    tokens: Ctx<ServiceTokens>,
    db: DbPool<Global>,
    request: DeleteRequest<TokenSlotParam>,
) -> Result<(), TdError> {
    let (ctx, param) = request.into_parts();
    let mut trx = db.begin().await?;
    let grant = authz
        .require::<User, SelfUser>(&mut trx, &ctx, &ctx.user_id)
        .await?;
    let conn = trx.get(grant);

    let row: ServiceTokenDB = conn
        .fetch_optional(and!(&ctx.user_id, &param.slot))
        .await?
        .ok_or(TokenError::NotFound)?;
    if *row.system {
        Err(TokenError::SystemManaged)?
    }
    conn.delete::<ServiceTokenDB>(row.id).await?;
    trx.commit().await?;
    // After the commit, so a reload cannot keep the revoked token.
    tokens.invalidate().await;

    info!(
        user_id = %row.user_id,
        slot = %row.slot,
        token_id = %row.id,
        "service token revoked"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use crate::token::TokenError;
    use td_authz::rbac::enforce::RbacError;
    use td_authz::system_token::seed_system_token;
    use td_database::access::{Global, RW};
    use td_database::domain::{Apiserver, DomainConnection};
    use td_database::pool::TdPool;
    use td_error::assert_service_error;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::service_token::{TokenCreate, TokenSlotParam};
    use td_objects::types::basic::{
        AccessTokenId, ExpiresInDays, ServiceToken, TokenComment, TokenScope, TokenSlot, UserId,
    };
    use td_security::token;

    fn slot(slot: i32) -> TokenSlotParam {
        TokenSlotParam {
            slot: TokenSlot::try_from(slot).unwrap(),
        }
    }

    #[td_test::test(db)]
    async fn test_revoke_token_frees_the_slot(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let create = TokenCreate {
            expires_in_days: ExpiresInDays::NEVER,
            comment: None,
        };
        services
            .create_service_token(ctx.create(slot(1), create.clone()))
            .await
            .unwrap();
        services
            .create_service_token(ctx.create(slot(2), create.clone()))
            .await
            .unwrap();

        services
            .revoke_service_token(ctx.delete(slot(1)))
            .await
            .unwrap();
        let listed = services
            .list_service_tokens(ctx.list((), ListParams::default()))
            .await
            .unwrap();
        let slots: Vec<i32> = listed.data.data.iter().map(|token| *token.slot).collect();
        assert_eq!(slots, vec![2]);

        services
            .create_service_token(ctx.create(slot(1), create))
            .await
            .unwrap();
    }

    #[td_test::test(db)]
    async fn test_revoke_empty_slot_is_not_found(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());
        assert_service_error(
            services.revoke_service_token(ctx.delete(slot(1))).await,
            |err| match err {
                TokenError::NotFound => {}
                other => panic!("expected NotFound, got {other:?}"),
            },
        );
    }

    #[td_test::test(db)]
    async fn test_revoke_token_is_refused_to_a_service_token(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let request = RequestContext::service_token(
            AccessTokenId::default(),
            UserId::admin(),
            TokenScope::Delegated,
        )
        .delete(slot(1));
        assert_service_error(
            services.revoke_service_token(request).await,
            |err| match err {
                RbacError::Forbidden(_, _) => {}
                other => panic!("expected Forbidden, got {other:?}"),
            },
        );
    }

    #[td_test::test(db)]
    async fn test_a_system_token_cannot_be_revoked_by_its_owner(db: TdPool) {
        let mut conn = DomainConnection::<Apiserver, Global, RW>::new(db.begin().await.unwrap());
        seed_system_token(
            &mut conn,
            &UserId::admin(),
            Some(&ServiceToken::try_from(token::generate()).unwrap()),
            &TokenComment::try_from("seeded").unwrap(),
        )
        .await
        .unwrap();
        conn.commit().await.unwrap();

        let services = Services::with_defaults(db.clone());
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(slot(1));
        assert_service_error(
            services.revoke_service_token(request).await,
            |err| match err {
                TokenError::SystemManaged => {}
                other => panic!("expected SystemManaged, got {other:?}"),
            },
        );
    }
}
