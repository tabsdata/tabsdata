//
// Copyright 2025 Tabs Data Inc.
//

use crate::{LiveEntry, QueryExecutor};
use ta_objects::{Buildable, TryUpdater};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::role::{RoleCreate, RoleDB, RoleDBBuilder};
use td_objects::types::basic::{AccessTokenId, Description, RoleId, RoleName, UserId};

pub(crate) async fn seed_role(
    db: impl QueryExecutor,
    name: RoleName,
    description: Description,
) -> Result<RoleDB, TdError> {
    let role_create = RoleCreate::builder()
        .name(name)
        .description(description)
        .build()?;

    let request_context = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    );

    let role_db = RoleDBBuilder::try_from(&role_create)?
        .try_update_from(&request_context)?
        .build()?;

    db.live().insert(&role_db).await?;
    Ok(role_db)
}

#[cfg(test)]
pub mod tests {
    use crate::test_utils::SeedEntry;
    use crate::LiveEntry;
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
