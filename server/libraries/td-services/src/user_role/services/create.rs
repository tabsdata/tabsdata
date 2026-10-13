//
// Copyright 2025 Tabsdata Inc.
//

use crate::user_role::assert_user_is_not_system;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, System, SystemEntity};
use td_database::access::{DbPool, Global, retrying};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::role::RoleParam;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::{UserRole, UserRoleCreate, UserRoleDB, UserRoleDBWithNames};
use td_objects::types::basic::Fixed;
use td_queries::Queries;

#[service_factory]
async fn create_user_role(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: CreateRequest<UserRoleCreate, RoleParam>,
) -> Result<UserRole, TdError> {
    let (ctx, param, create) = request.into_parts();
    retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let grant = authz
            .require::<System, ManageAccess>(&mut trx, &ctx, &SystemEntity)
            .await?;
        let conn = trx.get(grant);

        let role: RoleDB = conn.fetch_one(&param.role).await?;

        let user: UserDB = conn.fetch_one(&create.user).await?;
        assert_user_is_not_system(&user)?;

        let user_role = UserRoleDB::new(user.id, role.id, &ctx, Fixed::FALSE);
        conn.insert(&user_role).await?;

        let with_names: UserRoleDBWithNames = conn.fetch_one(&user_role.id).await?;
        let read = UserRole::from_dao(with_names)?;
        trx.commit().await?;
        Ok(read)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::user_role::UserRoleError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, Description, RoleIdName, RoleName, UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_create_user_role(db: TdPool) -> Result<(), TdError> {
        let _user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::FALSE)
            .await;
        let _role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await;

        let create = UserRoleCreate {
            user: UserName::try_from("joaquin")?,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam {
                role: RoleIdName::from_str("king")?,
            },
            create,
        );

        let service = Services::with_defaults(db.clone());
        let response = service.create_user_role(request).await;
        let response = response?;

        let found = db.fetch_one::<UserRoleDB>(&response.id).await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.user_id, found.user_id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.added_on, found.added_on);
        assert_eq!(response.added_by_id, found.added_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_create_user_role_rejects_system_user(db: TdPool) -> Result<(), TdError> {
        db.seed().system_user(&UserName::try_from("svc")?).await?;
        db.seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;

        let create = UserRoleCreate {
            user: UserName::try_from("svc")?,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam {
                role: RoleIdName::from_str("king")?,
            },
            create,
        );

        let service = Services::with_defaults(db.clone());
        assert_service_error(service.create_user_role(request).await, |err| match err {
            UserRoleError::CannotGrantRoleToSystemUser(name) => {
                assert_eq!(name, &UserName::try_from("svc").unwrap());
            }
            other => panic!("Expected CannotGrantRoleToSystemUser, got {other:?}"),
        });
        Ok(())
    }
}
