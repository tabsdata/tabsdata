//
// Copyright 2025 Tabsdata Inc.
//

use crate::{LiveEntry, QueryExecutor};
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{AccessTokenId, Fixed, RoleId, UserId, UserRoleId};

pub(crate) async fn seed_user_role(
    db: impl QueryExecutor + Domained<Domain = Apiserver>,
    user: &UserId,
    role: &RoleId,
) -> Result<UserRoleDB, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let user_role_db = UserRoleDB {
        id: UserRoleId::default(),
        user_id: *user,
        role_id: *role,
        added_on: request_context.time,
        added_by_id: request_context.user_id,
        fixed: Fixed::FALSE,
    };

    db.live().insert(&user_role_db).await?;
    Ok(user_role_db)
}

#[cfg(test)]
pub mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::types::basic::{Description, RoleName, UserEnabled, UserName};

    #[td_test::test(db)]
    async fn test_seed_user_role(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin").unwrap(), &UserEnabled::FALSE)
            .await
            .unwrap();
        let role = db
            .seed()
            .role(
                RoleName::try_from("king").unwrap(),
                Description::try_from("super user").unwrap(),
            )
            .await
            .unwrap();

        let user_role = db.seed().user_role(&user.id, &role.id).await.unwrap();

        let found = db.live().fetch_one::<UserRoleDB>(&role.id).await.unwrap();
        assert_eq!(user_role.id, found.id);
        assert_eq!(user_role.user_id, found.user_id);
        assert_eq!(user_role.role_id, found.role_id);
        assert_eq!(user_role.added_on, found.added_on);
        assert_eq!(user_role.added_by_id, found.added_by_id);
        assert_eq!(user_role.fixed, found.fixed);
    }
}
