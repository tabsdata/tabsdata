//
// Copyright 2026 Tabsdata Inc.
//

use crate::token::TokenError;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, System, SystemEntity};
use td_database::access::{DbPool, Global, retrying};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::service_token::{ServiceTokenDB, UserSlotParam};
use td_objects::dxo::user::UserDB;
use td_queries::Queries;
use tracing::info;

#[service_factory]
async fn revoke_user_service_token(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: DeleteRequest<UserSlotParam>,
) -> Result<(), TdError> {
    let (ctx, param) = request.into_parts();
    let row = retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let grant = authz
            .require::<System, ManageAccess>(&mut trx, &ctx, &SystemEntity)
            .await?;
        let conn = trx.get(grant);

        let user: UserDB = conn.fetch_one(&param.user).await?;
        let row: ServiceTokenDB = conn
            .fetch_optional(and!(&user.id, &param.slot))
            .await?
            .ok_or(TokenError::NotFound)?;
        if *row.system {
            Err(TokenError::SystemManaged)?
        }
        conn.delete::<ServiceTokenDB>(row.id).await?;
        trx.commit().await?;
        Ok(row)
    })
    .await?;

    info!(
        revoked_by = %ctx.user_id,
        user_id = %row.user_id,
        slot = %row.slot,
        token_id = %row.id,
        "service token revoked by an administrator"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use crate::token::TokenError;
    use std::str::FromStr;
    use td_authz::rbac::enforce::RbacError;
    use td_authz::system_token::seed_system_token;
    use td_database::access::{Global, RW};
    use td_database::domain::{Apiserver, DomainConnection};
    use td_database::pool::TdPool;
    use td_error::assert_service_error;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::service_token::{TokenCreate, TokenSlotParam, UserSlotParam};
    use td_objects::dxo::user::UserParam;
    use td_objects::types::basic::{
        AccessTokenId, ExpiresInDays, ServiceToken, TokenComment, TokenScope, TokenSlot,
        TokenSystem, UserEnabled, UserId, UserIdName, UserName,
    };
    use td_queries::test_utils::SeedEntry;
    use td_security::token;

    fn slot(slot: i32) -> TokenSlot {
        TokenSlot::try_from(slot).unwrap()
    }

    fn target(name: &str, slot_number: i32) -> UserSlotParam {
        UserSlotParam {
            user: UserIdName::from_str(name).unwrap(),
            slot: slot(slot_number),
        }
    }

    async fn fill(services: &Services, user: UserId, slot_number: i32) {
        let request = RequestContext::with(AccessTokenId::default(), user).create(
            TokenSlotParam {
                slot: slot(slot_number),
            },
            TokenCreate {
                expires_in_days: ExpiresInDays::NEVER,
                comment: None,
            },
        );
        services.create_service_token(request).await.unwrap();
    }

    #[td_test::test(db)]
    async fn test_admin_revokes_another_users_token(db: TdPool) {
        let other = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let services = Services::with_defaults(db.clone());
        fill(&services, other.id, 1).await;
        fill(&services, other.id, 2).await;

        let admin = RequestContext::with(AccessTokenId::default(), UserId::admin());
        services
            .revoke_user_service_token(admin.delete(target("u0", 1)))
            .await
            .unwrap();

        let owner = RequestContext::with(AccessTokenId::default(), other.id);
        let listed = services
            .list_service_tokens(owner.list((), ListParams::default()))
            .await
            .unwrap();
        let slots: Vec<i32> = listed.data.data.iter().map(|token| *token.slot).collect();
        assert_eq!(slots, vec![2]);

        assert_service_error(
            services
                .revoke_user_service_token(admin.delete(target("u0", 1)))
                .await,
            |err| match err {
                TokenError::NotFound => {}
                other => panic!("expected NotFound, got {other:?}"),
            },
        );
    }

    #[td_test::test(db)]
    async fn test_revoking_another_users_token_needs_manage_access(db: TdPool) {
        let other = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let services = Services::with_defaults(db.clone());
        fill(&services, UserId::admin(), 1).await;

        let request =
            RequestContext::with(AccessTokenId::default(), other.id).delete(target("admin", 1));
        assert_service_error(
            services.revoke_user_service_token(request).await,
            |err| match err {
                RbacError::Forbidden(_, _) => {}
                other => panic!("expected Forbidden, got {other:?}"),
            },
        );
    }

    #[td_test::test(db)]
    async fn test_an_administrator_cannot_revoke_a_system_token(db: TdPool) {
        let other = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let mut conn = DomainConnection::<Apiserver, Global, RW>::new(db.begin().await.unwrap());
        seed_system_token(
            &mut conn,
            &other.id,
            Some(&ServiceToken::try_from(token::generate()).unwrap()),
            &TokenComment::try_from("seeded").unwrap(),
        )
        .await
        .unwrap();
        conn.commit().await.unwrap();

        let services = Services::with_defaults(db.clone());
        let admin = RequestContext::with(AccessTokenId::default(), UserId::admin());
        assert_service_error(
            services
                .revoke_user_service_token(admin.delete(target("u0", 1)))
                .await,
            |err| match err {
                TokenError::SystemManaged => {}
                other => panic!("expected SystemManaged, got {other:?}"),
            },
        );

        let listed = services
            .list_user_service_tokens(admin.list(
                UserParam {
                    user: UserIdName::from_str("u0").unwrap(),
                },
                ListParams::default(),
            ))
            .await
            .unwrap();
        let token = &listed.data.data[0];
        assert_eq!(token.scope, TokenScope::Identity);
        assert_eq!(token.system, TokenSystem::TRUE);
    }
}
