//
// Copyright 2024 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, System, SystemEntity};
use td_database::access::{GatedConnection, RW};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::user::{UserCreate, UserDB, UserDBWithNames, UserRead};
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::Fixed;
use td_objects::types::basic::RoleId;
use td_queries::LiveEntry;
use td_security::config::PasswordHashingConfig;

#[service_factory]
async fn create_user(
    authz: Ctx<AuthzContext>,
    password_hashing_config: Ctx<PasswordHashingConfig>,
    gated_trx: &mut GatedConnection<RW>,
    request: CreateRequest<UserCreate>,
) -> Result<UserRead, TdError> {
    let (ctx, _, create) = request.into_parts();
    let grant = authz
        .require::<System, ManageAccess>(gated_trx, &ctx, &SystemEntity)
        .await?;
    let conn = gated_trx.get(grant);

    let user = create.to_dao(&ctx, &password_hashing_config)?;
    // A duplicate user name reports a clean "already exists" error instead
    // of the raw unique-constraint violation from the insert.
    conn.live().assert_not_exists::<UserDB>(&user.name).await?;
    conn.live().insert(&user).await?;

    let user_role = UserRoleDB::new(user.id, RoleId::default_role(), &ctx, Fixed::TRUE);
    conn.live().insert(&user_role).await?;

    let with_names: UserDBWithNames = conn.live().fetch_one(&user.id).await?;
    let read = UserRead::from_dao(with_names)?;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, Email, FullName, RoleId, UserEnabled, UserId, UserName,
    };
    use td_queries::LiveEntry;

    async fn test_create_user(
        db: &TdPool,
        enabled: UserEnabled,
        expected_enabled: bool,
        with_email: bool,
    ) {
        let service = Services::with_defaults(db.clone());

        let create = UserCreate {
            name: "u1".to_string().try_into().unwrap(),
            full_name: "U1".to_string().try_into().unwrap(),
            email: if with_email {
                Some(Email::try_from("u1@email.com").unwrap())
            } else {
                None
            },
            password: "password".to_string().try_into().unwrap(),
            enabled,
        };

        let before = AtTime::now();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).create((), create);
        let response = service.create_user(request).await;
        let created = response.unwrap().data;

        assert_eq!(created.name, UserName::try_from("u1").unwrap());
        assert_eq!(created.full_name, FullName::try_from("U1").unwrap());
        if with_email {
            assert_eq!(
                created.email.unwrap(),
                Email::try_from("u1@email.com").unwrap()
            );
        } else {
            assert!(created.email.is_none());
        }
        assert!(*created.created_on >= before);
        assert_eq!(*created.created_by_id, UserId::admin());
        assert_eq!(*created.created_by, UserName::admin());
        assert_eq!(*created.modified_on, *created.created_on);
        assert_eq!(*created.modified_by_id, UserId::admin());
        assert_eq!(*created.modified_by, UserName::admin());
        assert_eq!(*created.enabled, expected_enabled);

        let res: Option<UserRoleDB> = db
            .live()
            .fetch_optional::<UserRoleDB>(&created.id)
            .await
            .unwrap();
        assert!(res.is_some());
        assert_eq!(RoleId::default_role(), res.unwrap().role_id)
    }

    #[td_test::test(db)]
    async fn test_create_user_enabled_true_with_email(db: TdPool) {
        test_create_user(&db, UserEnabled::TRUE, true, true).await;
    }

    #[td_test::test(db)]
    async fn test_create_user_enabled_false_without_email(db: TdPool) {
        test_create_user(&db, UserEnabled::FALSE, false, false).await;
    }

    #[td_test::test(db)]
    async fn test_create_user_enabled_default_without_email(db: TdPool) {
        test_create_user(&db, UserEnabled::default(), true, false).await;
    }
}
