//
// Copyright 2025 Tabs Data Inc.
//

use crate::BasicQueries;
use ta_objects::{Buildable, TryUpdater};
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::permission::{PermissionCreate, PermissionDB, PermissionDBBuilder};
use td_objects::dxo::role::RoleDB;
use td_objects::types::basic::{
    AccessTokenId, EntityId, EntityName, PermissionType, RoleId, UserId,
};

pub async fn seed_permission(
    db: impl SeaExecutor + Send,
    permission_type: PermissionType,
    entity_name: Option<EntityName>,
    entity_id: Option<EntityId>,
    role_db: &RoleDB,
) -> Result<PermissionDB, TdError> {
    let permission_create = PermissionCreate::builder()
        .permission_type(permission_type.clone())
        .entity_name(entity_name)
        .build()?;

    let request_context: ReadRequest<String> = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    )
    .read("");
    let request_context = request_context.context;

    let mut builder = PermissionDBBuilder::try_from(&permission_create)?;
    let builder = builder
        .try_update_from(&request_context)?
        .try_update_from(role_db)?;

    let permission_entity_type = permission_type.on_entity_type();
    builder.entity_type(permission_entity_type);
    let entity_id = entity_id.unwrap_or_else(EntityId::all_entities);
    builder.entity_id(entity_id);
    let permission_db = builder.build()?;

    db.insert(&permission_db).await?;
    Ok(permission_db)
}

#[cfg(test)]
mod tests {
    use crate::BasicQueries;
    use crate::test_utils::SeedQueries;
    use td_database::pool::TdPool;
    use td_objects::dxo::permission::PermissionDB;
    use td_objects::types::basic::{Description, PermissionType, RoleName};

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_permission(db: TdPool) {
        let role = db
            .seed_role(
                RoleName::try_from("joaquin").unwrap(),
                Description::try_from("super user").unwrap(),
            )
            .await
            .unwrap();
        let permission = db
            .seed_permission(PermissionType::try_from("sa").unwrap(), None, None, &role)
            .await
            .unwrap();

        let found = db.fetch_one::<PermissionDB>(&permission.id).await.unwrap();
        assert_eq!(permission.id, found.id);
        assert_eq!(permission.role_id, found.role_id);
        assert_eq!(permission.permission_type, found.permission_type);
        assert_eq!(permission.entity_type, found.entity_type);
        assert_eq!(permission.entity_id, found.entity_id);
        assert_eq!(permission.granted_by_id, found.granted_by_id);
        assert_eq!(permission.granted_on, found.granted_on);
        assert_eq!(permission.fixed, found.fixed);
    }
}
