//
// Copyright 2026 Tabsdata Inc.
//

use crate::token::TokenError;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::service_token::{ServiceTokenDBWithNames, TokenSelf};
use td_objects::types::basic::ServiceTokenId;
use td_queries::Queries;

/// The route accepts tokens only, so the access-token id is a token id; no
/// permission gates a caller asking about its own credential.
#[service_factory]
async fn read_own_service_token(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    ctx: RequestContext,
) -> Result<TokenSelf, TdError> {
    let conn = gated_conn.get(authz.none());
    let id = ServiceTokenId::from(*ctx.access_token_id);
    let row: ServiceTokenDBWithNames =
        conn.fetch_optional(id).await?.ok_or(TokenError::NotFound)?;
    TokenSelf::from_dao(row)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::service_token::{TokenCreate, TokenSlotParam};
    use td_objects::types::basic::{
        AccessTokenId, ExpiresInDays, TokenScope, TokenSlot, UserId, UserName,
    };

    #[td_test::test(db)]
    async fn test_read_own_token_answers_for_the_presenting_token(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let session = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let created = services
            .create_service_token(session.create(
                TokenSlotParam {
                    slot: TokenSlot::try_from(2).unwrap(),
                },
                TokenCreate {
                    expires_in_days: ExpiresInDays::NEVER,
                    comment: None,
                },
            ))
            .await
            .unwrap()
            .data;

        let service_token = RequestContext::service_token(
            AccessTokenId::from(*created.id),
            UserId::admin(),
            TokenScope::Delegated,
        );
        let own = services
            .read_own_service_token(service_token)
            .await
            .unwrap()
            .data;
        assert_eq!(own.id, created.id);
        assert_eq!(*own.slot, 2);
        assert_eq!(own.user_name, UserName::admin());
        assert_eq!(own.hint, created.hint);
    }
}
