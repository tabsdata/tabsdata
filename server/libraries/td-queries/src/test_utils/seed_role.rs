//
// Copyright 2025 Tabsdata Inc.
//

use crate::{LiveEntry, QueryExecutor};
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::role::{RoleCreate, RoleDB};
use td_objects::types::basic::{
    AccessTokenId, Description, Fixed, RoleId, RoleKind, RoleName, UserId,
};

pub(crate) async fn seed_role(
    db: impl QueryExecutor + Domained<Domain = Apiserver>,
    name: RoleName,
    description: Description,
) -> Result<RoleDB, TdError> {
    let role_create = RoleCreate { name, description };

    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let role_db = RoleDB {
        id: RoleId::default(),
        name: role_create.name,
        description: role_create.description,
        created_on: request_context.time,
        created_by_id: request_context.user_id,
        modified_on: request_context.time,
        modified_by_id: request_context.user_id,
        kind: RoleKind::Default,
        project_id: None,
        fixed: Fixed::FALSE,
    };

    db.live().insert(&role_db).await?;
    Ok(role_db)
}

#[cfg(test)]
pub mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::role::RoleDB;
    use td_objects::types::basic::{Description, RoleName};

    #[td_test::test(db)]
    async fn test_seed_role(db: TdPool) {
        let role = db
            .seed()
            .role(
                RoleName::try_from("joaquin").unwrap(),
                Description::try_from("super user").unwrap(),
            )
            .await
            .unwrap();

        let found = db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from("joaquin").unwrap())
            .await
            .unwrap();
        assert_eq!(role.id, found.id);
        assert_eq!(role.name, found.name);
        assert_eq!(role.description, found.description);
        assert_eq!(role.created_on, found.created_on);
        assert_eq!(role.created_by_id, found.created_by_id);
        assert_eq!(role.modified_on, found.modified_on);
        assert_eq!(role.modified_by_id, found.modified_by_id);
        assert_eq!(role.fixed, found.fixed);
    }
}
