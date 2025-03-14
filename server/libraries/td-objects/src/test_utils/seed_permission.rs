//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{handle_sql_err, ReadRequest, RequestContext};
use crate::sql::{DaoQueries, Insert, SelectBy};
use crate::test_utils::seed_user::admin_user;
use crate::types::basic::{EntityId, EntityName, PermissionEntityType, PermissionType};
use crate::types::permission::{PermissionCreate, PermissionDB, PermissionDBBuilder};
use crate::types::role::RoleDB;
use crate::types::SqlEntity;
use td_database::sql::DbPool;
use td_error::TdError;

pub async fn seed_permission(
    db: &DbPool,
    permission_type: PermissionType,
    entity_name: Option<EntityName>,
    entity_id: Option<EntityId>,
    role_db: &RoleDB,
) -> PermissionDB {
    let permission_create = PermissionCreate::builder()
        .permission_type(&permission_type)
        .entity_name(entity_name)
        .build()
        .unwrap();

    let admin_id = admin_user(db).await;
    let request_context: ReadRequest<String> =
        RequestContext::with(&admin_id, "r", true).await.read("");
    let request_context = request_context.context();

    let builder = PermissionDBBuilder::try_from(&permission_create).unwrap();
    let builder = PermissionDBBuilder::try_from((request_context, builder)).unwrap();
    let mut builder = PermissionDBBuilder::try_from((role_db, builder)).unwrap();

    let permission_entity_type = match permission_type.on_entity_type().starts_with("s") {
        true => PermissionEntityType::try_from("s").unwrap(),
        false => PermissionEntityType::try_from("r").unwrap(),
    };
    builder.entity_type(permission_entity_type);
    builder.entity_id(entity_id);
    let permission_db = builder.build().unwrap();

    let queries = DaoQueries::default();
    queries
        .insert(&permission_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    permission_db
}

pub async fn get_permission<E: SqlEntity>(db: &DbPool, by: &E) -> Result<PermissionDB, TdError> {
    let queries = DaoQueries::default();
    queries
        .select_by::<PermissionDB>(by)?
        .build_query_as()
        .fetch_one(db)
        .await
        .map_err(handle_sql_err)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::test_utils::seed_permission::seed_permission;
    use crate::test_utils::seed_role::seed_role;
    use crate::types::basic::{Description, RoleName};

    #[tokio::test]
    async fn test_seed_permission() {
        let db = td_database::test_utils::db().await.unwrap();
        let role = seed_role(
            &db,
            RoleName::try_from("joaquin").unwrap(),
            Description::try_from("super user").unwrap(),
        )
        .await;
        let permission = seed_permission(
            &db,
            PermissionType::try_from("sa").unwrap(),
            None,
            None,
            &role,
        )
        .await;

        let found = get_permission(&db, permission.id()).await.unwrap();
        assert_eq!(permission.id(), found.id());
        assert_eq!(permission.role_id(), found.role_id());
        assert_eq!(permission.permission_type(), found.permission_type());
        assert_eq!(permission.entity_type(), found.entity_type());
        assert_eq!(permission.entity_id(), found.entity_id());
        assert_eq!(permission.granted_by_id(), found.granted_by_id());
        assert_eq!(permission.granted_on(), found.granted_on());
        assert_eq!(permission.fixed(), found.fixed());
    }
}
