//
// Copyright 2025. Tabs Data Inc.
//

use crate::{AuthzData, Provider};
use async_trait::async_trait;
use itertools::Itertools;
use sqlx::SqliteConnection;
use std::collections::HashMap;
use std::sync::Arc;
use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::sql::{DaoQueries, SelectBy};
use td_objects::tower_service::authz::{AuthzEntity, Permission};
use td_objects::types::basic::{CollectionId, PermissionType, RoleId, ToCollectionId};
use td_objects::types::permission::{InterCollectionPermissionDB, PermissionDB};

/// Provider that gets permissions and inter-permissions mapping from the database on every `get` call.
pub struct SqlAuthzDataProvider;

impl SqlAuthzDataProvider {
    //TODO: This try_to_permission should be converted into a `TryFrom<&PermissionDB> for Permission`
    fn try_to_permission(perm_db: &PermissionDB) -> Result<Permission, TdError> {
        fn authz_entity(perm_db: &PermissionDB) -> Result<AuthzEntity<CollectionId>, TdError> {
            if perm_db.entity_id().is_all_entities() {
                Ok(AuthzEntity::All)
            } else {
                Ok(AuthzEntity::On(CollectionId::try_from(
                    perm_db.entity_id(),
                )?))
            }
        }

        let perm = match perm_db.permission_type() {
            PermissionType::SysAdmin => Permission::SysAdmin,
            PermissionType::SecAdmin => Permission::SecAdmin,
            PermissionType::CollectionAdmin => Permission::CollectionAdmin(authz_entity(perm_db)?),
            PermissionType::CollectionDev => Permission::CollectionDev(authz_entity(perm_db)?),
            PermissionType::CollectionExec => Permission::CollectionExec(authz_entity(perm_db)?),
            PermissionType::CollectionRead => Permission::CollectionRead(authz_entity(perm_db)?),
        };
        Ok(perm)
    }

    async fn get_permissions<'a>(
        &'a self,
        conn: &'a mut SqliteConnection,
    ) -> Result<HashMap<RoleId, Arc<Vec<Permission>>>, TdError> {
        let permissions: Vec<PermissionDB> = DaoQueries::default()
            .select_by::<PermissionDB>(&())?
            .build_query_as()
            .fetch_all(conn)
            .await
            .map_err(handle_sql_err)?;
        let role_permissions_map = permissions
            .iter()
            .map(|p| (*p.role_id(), Self::try_to_permission(p).unwrap()))
            .into_group_map()
            .into_iter()
            .map(|(role, perms)| (role, Arc::new(perms)))
            .collect();
        Ok(role_permissions_map)
    }

    async fn get_inter_collection_permissions<'a>(
        &'a self,
        conn: &'a mut SqliteConnection,
    ) -> Result<HashMap<CollectionId, Arc<Vec<ToCollectionId>>>, TdError> {
        let permissions: Vec<InterCollectionPermissionDB> = DaoQueries::default()
            .select_by::<InterCollectionPermissionDB>(&())?
            .build_query_as()
            .fetch_all(conn)
            .await
            .map_err(handle_sql_err)?;
        let permissions_map = permissions
            .iter()
            .map(|p| (*p.from_collection_id(), *p.to_collection_id()))
            .into_group_map()
            .into_iter()
            .map(|(role, perms)| (role, Arc::new(perms)))
            .collect();
        Ok(permissions_map)
    }
}

#[async_trait]
impl<'a> Provider<'a, AuthzData, &'a mut SqliteConnection> for SqlAuthzDataProvider {
    async fn get(&'a self, context: &'a mut SqliteConnection) -> Result<Arc<AuthzData>, TdError> {
        let permissions = self.get_permissions(context).await?;
        let inter_collections_permissions_value_can_read_key =
            self.get_inter_collection_permissions(context).await?;
        let inter_collections_permissions_key_can_read_value =
            inter_collections_permissions_value_can_read_key
                .iter()
                .flat_map(|(from_collection, to_collections)| {
                    to_collections
                        .iter()
                        .map(move |to_collection| (*to_collection, *from_collection))
                })
                .into_group_map()
                .into_iter()
                .map(|(to_collection, from_collections)| {
                    (to_collection, Arc::new(from_collections))
                })
                .collect();

        let authz_data = AuthzData {
            permissions,
            inter_collections_permissions_value_can_read_key,
            inter_collections_permissions_key_can_read_value,
        };
        Ok(Arc::new(authz_data))
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::SqlAuthzDataProvider;
    use td_common::provider::Provider;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
    use td_objects::types::basic::{CollectionName, RoleId, ToCollectionId, UserId};

    #[td_test::test(sqlx(migrator = td_schema::schema()))]
    async fn test_get_permissions(db: DbPool) -> Result<(), TdError> {
        let provider = SqlAuthzDataProvider;
        let permissions = provider
            .get_permissions(&mut db.acquire().await.unwrap())
            .await?;
        assert_eq!(permissions.len(), 3);
        assert_eq!(permissions.get(&RoleId::sys_admin()).unwrap().len(), 6);
        assert_eq!(permissions.get(&RoleId::sec_admin()).unwrap().len(), 2);
        assert_eq!(permissions.get(&RoleId::user()).unwrap().len(), 3);
        Ok(())
    }

    #[td_test::test(sqlx(migrator = td_schema::schema()))]
    async fn test_get_inter_collection_permissions(db: DbPool) -> Result<(), TdError> {
        let c0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let c1 = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;
        let c2 = seed_collection(&db, &CollectionName::try_from("c2")?, &UserId::admin()).await;
        seed_inter_collection_permission(&db, c0.id(), &ToCollectionId::try_from(c1.id())?).await;
        seed_inter_collection_permission(&db, c0.id(), &ToCollectionId::try_from(c2.id())?).await;
        seed_inter_collection_permission(&db, c1.id(), &ToCollectionId::try_from(c2.id())?).await;

        let provider = SqlAuthzDataProvider;
        let permissions = provider
            .get_inter_collection_permissions(&mut db.acquire().await.unwrap())
            .await?;
        assert_eq!(permissions.len(), 2);
        assert_eq!(permissions.get(c0.id()).unwrap().len(), 2);
        assert_eq!(permissions.get(c1.id()).unwrap().len(), 1);
        assert!(!permissions.contains_key(c2.id()));
        Ok(())
    }

    #[td_test::test(sqlx(migrator = td_schema::schema()))]
    async fn test_provider_get(db: DbPool) -> Result<(), TdError> {
        let c0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let c1 = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;
        let c2 = seed_collection(&db, &CollectionName::try_from("c2")?, &UserId::admin()).await;
        seed_inter_collection_permission(&db, c0.id(), &ToCollectionId::try_from(c1.id())?).await;
        seed_inter_collection_permission(&db, c0.id(), &ToCollectionId::try_from(c2.id())?).await;
        seed_inter_collection_permission(&db, c1.id(), &ToCollectionId::try_from(c2.id())?).await;

        let provider = SqlAuthzDataProvider;

        let authz_data = provider.get(&mut db.acquire().await.unwrap()).await?;
        assert_eq!(authz_data.permissions.len(), 3);
        assert_eq!(
            authz_data
                .inter_collections_permissions_value_can_read_key
                .len(),
            2
        );

        Ok(())
    }
}
