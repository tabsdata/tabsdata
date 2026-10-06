//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{SelfUser, User};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::service_token::TokenRead;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn list_service_tokens(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest,
) -> Result<ListResponse<TokenRead>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    let grant = authz
        .require::<User, SelfUser>(gated_conn, &ctx, &ctx.user_id)
        .await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<TokenRead>::try_from(&list_params)?;
    let rows = conn.list(&ctx.user_id, &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_authz::rbac::enforce::RbacError;
    use td_database::pool::TdPool;
    use td_error::assert_service_error;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::service_token::{TokenCreate, TokenSlotParam};
    use td_objects::types::basic::{
        AccessTokenId, ExpiresInDays, TokenScope, TokenSlot, UserEnabled, UserId, UserName,
    };
    use td_queries::test_utils::SeedEntry;

    async fn fill(services: &Services, user: UserId, slot: i32) {
        let request = RequestContext::with(AccessTokenId::default(), user).create(
            TokenSlotParam {
                slot: TokenSlot::try_from(slot).unwrap(),
            },
            TokenCreate {
                expires_in_days: ExpiresInDays::NEVER,
                comment: None,
            },
        );
        services.create_service_token(request).await.unwrap();
    }

    #[td_test::test(db)]
    async fn test_list_tokens_is_the_requesters_own(db: TdPool) {
        let other = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let services = Services::with_defaults(db.clone());
        fill(&services, UserId::admin(), 1).await;
        fill(&services, UserId::admin(), 2).await;
        fill(&services, other.id, 1).await;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());
        let listed = services.list_service_tokens(request).await.unwrap();
        let slots: Vec<i32> = listed.data.data.iter().map(|token| *token.slot).collect();
        assert_eq!(slots, vec![1, 2]);
        assert!(
            !serde_json::to_string(&listed.data.data)
                .unwrap()
                .contains("digest")
        );

        let request = RequestContext::with(AccessTokenId::default(), other.id)
            .list((), ListParams::default());
        let listed = services.list_service_tokens(request).await.unwrap();
        assert_eq!(listed.data.len, 1);
    }

    #[td_test::test(db)]
    async fn test_list_tokens_is_refused_to_a_service_token(db: TdPool) {
        let services = Services::with_defaults(db.clone());
        let request = RequestContext::service_token(
            AccessTokenId::default(),
            UserId::admin(),
            TokenScope::Delegated,
        )
        .list((), ListParams::default());
        assert_service_error(
            services.list_service_tokens(request).await,
            |err| match err {
                RbacError::Forbidden(_, _) => {}
                other => panic!("expected Forbidden, got {other:?}"),
            },
        );
    }
}
