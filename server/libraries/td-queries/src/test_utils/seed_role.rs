//
// Copyright 2025 Tabs Data Inc.
//

use crate::BasicQueries;
use ta_objects::{Buildable, TryUpdater};
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::role::{RoleCreate, RoleDB, RoleDBBuilder};
use td_objects::types::basic::{AccessTokenId, Description, RoleId, RoleName, UserId};

pub(crate) async fn seed_role(
    db: impl SeaExecutor + Send,
    name: RoleName,
    description: Description,
) -> Result<RoleDB, TdError> {
    let role_create = RoleCreate::builder()
        .name(name)
        .description(description)
        .build()?;

    let request_context: ReadRequest<String> = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    )
    .read("");
    let request_context = request_context.context;

    let role_db = RoleDBBuilder::try_from(&role_create)?
        .try_update_from(&request_context)?
        .build()?;

    db.insert(&role_db).await?;
    Ok(role_db)
}

#[cfg(test)]
pub mod tests {
    use crate::BasicQueries;
    use crate::test_utils::SeedQueries;
    use td_database::pool::TdPool;
    use td_objects::dxo::role::RoleDB;
    use td_objects::types::basic::{Description, RoleName};

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_role(db: TdPool) {
        let role = db
            .seed_role(
                RoleName::try_from("joaquin").unwrap(),
                Description::try_from("super user").unwrap(),
            )
            .await
            .unwrap();

        let found = db
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
