//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, System, SystemEntity};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::service_token::TokenRead;
use td_objects::dxo::user::{UserDB, UserParam};
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn list_user_service_tokens(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest<UserParam>,
) -> Result<ListResponse<TokenRead>, TdError> {
    let (ctx, param, list_params) = request.into_parts();
    let grant = authz
        .require::<System, ManageAccess>(gated_conn, &ctx, &SystemEntity)
        .await?;
    let conn = gated_conn.get(grant);

    let user: UserDB = conn.fetch_one(&param.user).await?;
    let query_params = ListQueryParams::<TokenRead>::try_from(&list_params)?;
    let rows = conn.list(&user.id, &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use std::str::FromStr;
    use td_authz::rbac::enforce::RbacError;
    use td_database::pool::TdPool;
    use td_error::assert_service_error;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::service_token::{TokenCreate, TokenSlotParam};
    use td_objects::dxo::user::UserParam;
    use td_objects::types::basic::{
        AccessTokenId, ExpiresInDays, TokenSlot, UserEnabled, UserId, UserIdName, UserName,
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

    fn by_name(name: &str) -> UserParam {
        UserParam {
            user: UserIdName::from_str(name).unwrap(),
        }
    }

    #[td_test::test(db)]
    async fn test_admin_lists_another_users_tokens(db: TdPool) {
        let other = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let services = Services::with_defaults(db.clone());
        fill(&services, UserId::admin(), 1).await;
        fill(&services, other.id, 2).await;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(by_name("u0"), ListParams::default());
        let listed = services.list_user_service_tokens(request).await.unwrap();
        let slots: Vec<i32> = listed.data.data.iter().map(|token| *token.slot).collect();
        assert_eq!(slots, vec![2]);
    }

    #[td_test::test(db)]
    async fn test_listing_another_users_tokens_needs_manage_access(db: TdPool) {
        let other = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let services = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), other.id)
            .list(by_name("admin"), ListParams::default());
        assert_service_error(
            services.list_user_service_tokens(request).await,
            |err| match err {
                RbacError::Forbidden(_, _) => {}
                other => panic!("expected Forbidden, got {other:?}"),
            },
        );
    }
}
