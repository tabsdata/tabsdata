//
// Copyright 2025. Tabs Data Inc.
//

use crate::Provider;
use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{handle_sql_err, ListParams};
use td_objects::sql::{DaoQueries, ListBy};
use td_objects::tower_service::authz::{AuthzEntity, Permission};
use td_objects::types::basic::{CollectionId, PermissionType, RoleId};
use td_objects::types::permission::PermissionDB;

/// Provider that gets role-permissions mapping from the database on every `get` call.
pub struct SqlRolePermissionsProvider {
    db_pool: DbPool,
}

impl SqlRolePermissionsProvider {
    pub fn new(db_pool: impl Into<DbPool>) -> Self {
        Self {
            db_pool: db_pool.into(),
        }
    }
}

//TODO: This try_to_permission should be converted into a `TryFrom<&PermissionDB> for Permission`
fn try_to_permission(perm_db: &PermissionDB) -> Result<Permission, TdError> {
    fn authz_entity(perm_db: &PermissionDB) -> AuthzEntity<CollectionId> {
        if let Some(entity_id) = perm_db.entity_id() {
            AuthzEntity::On(CollectionId::try_from(entity_id).unwrap())
        } else {
            AuthzEntity::All
        }
    }

    let perm = match perm_db.permission_type().as_str() {
        PermissionType::SA => Permission::SysAdmin,
        PermissionType::SS => Permission::SecAdmin,
        PermissionType::CA => Permission::CollectionAdmin(authz_entity(&perm_db)),
        PermissionType::CD => Permission::CollectionDev(authz_entity(&perm_db)),
        PermissionType::CX => Permission::CollectionExec(authz_entity(&perm_db)),
        PermissionType::CR => Permission::CollectionRead(authz_entity(&perm_db)),
        PermissionType::CR_ALL => Permission::CollectionReadAll(authz_entity(&perm_db)),
        argh => panic!("TODO, proper error ARGH - '{}'", argh),
    };
    Ok(perm)
}

#[async_trait]
impl Provider<HashMap<RoleId, Arc<Vec<Permission>>>> for SqlRolePermissionsProvider {
    async fn get(&self) -> Result<Arc<HashMap<RoleId, Arc<Vec<Permission>>>>, TdError> {
        let permissions: Vec<PermissionDB> = DaoQueries::default()
            .list_by::<PermissionDB>(&ListParams::all(), &())?
            .build_query_as()
            .fetch_all(&self.db_pool)
            .await
            .map_err(handle_sql_err)?;
        let role_permissions_map = permissions
            .iter()
            .map(|p| (p.role_id().clone(), try_to_permission(p).unwrap())) //TODO change the try_to_permission
            .into_group_map()
            .into_iter()
            .map(|(role, perms)| (role, Arc::new(perms)).into())
            .collect();
        Ok(Arc::new(role_permissions_map))
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::SqlRolePermissionsProvider;
    use td_common::provider::Provider;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::types::basic::RoleId;
    use td_security::{ENCODED_ID_ROLE_SEC_ADMIN, ENCODED_ID_ROLE_SYS_ADMIN, ENCODED_ID_ROLE_USER};

    #[td_test::test(sqlx(migrator = td_schema::schema()))]
    async fn test_sql_role_permissions_provider(db: DbPool) -> Result<(), TdError> {
        let provider = SqlRolePermissionsProvider::new(db);
        let permissions = provider.get().await.unwrap();
        assert_eq!(permissions.len(), 3);
        assert_eq!(
            permissions
                .get(&RoleId::try_from(ENCODED_ID_ROLE_SYS_ADMIN).unwrap())
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            permissions
                .get(&RoleId::try_from(ENCODED_ID_ROLE_SEC_ADMIN).unwrap())
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            permissions
                .get(&RoleId::try_from(ENCODED_ID_ROLE_USER).unwrap())
                .unwrap()
                .len(),
            4
        );
        Ok(())
    }
}
