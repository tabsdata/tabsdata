//
// Copyright 2025. Tabs Data Inc.
//

use crate::tower::{AuthzEntity, Permission};
use crate::{AuthzData, Provider};
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::inter_collection_permission::InterCollectionPermissionDB;
use td_objects::dxo::permission::PermissionDB;
use td_objects::types::basic::{CollectionId, PermissionType, RoleId, ToCollectionId};
use td_queries::LiveEntry;
use tracing::{debug, error, instrument};

/// Provider that gets permissions and inter-permissions mapping from the database on every `get` call.
pub struct SqlAuthzDataProvider;

impl SqlAuthzDataProvider {
    //TODO: This try_to_permission should be converted into a `TryFrom<&PermissionDB> for Permission`
    fn try_to_permission(perm_db: &PermissionDB) -> Result<Permission, TdError> {
        fn authz_entity(perm_db: &PermissionDB) -> Result<AuthzEntity<CollectionId>, TdError> {
            if perm_db.entity_id.is_all_entities() {
                Ok(AuthzEntity::All)
            } else {
                Ok(AuthzEntity::On(CollectionId::try_from(&perm_db.entity_id)?))
            }
        }

        let perm = match &perm_db.permission_type {
            PermissionType::SysAdmin => Permission::SysAdmin,
            PermissionType::SecAdmin => Permission::SecAdmin,
            PermissionType::CollectionAdmin => Permission::CollectionAdmin(authz_entity(perm_db)?),
            PermissionType::CollectionDev => Permission::CollectionDev(authz_entity(perm_db)?),
            PermissionType::CollectionExec => Permission::CollectionExec(authz_entity(perm_db)?),
            PermissionType::CollectionRead => Permission::CollectionRead(authz_entity(perm_db)?),
        };
        Ok(perm)
    }

    #[instrument(
        name = "authz.provider.get_permissions",
        skip_all,
        level = "debug",
        err(Debug)
    )]
    async fn get_permissions(
        &self,
        conn: &mut DynamicConnection<'_>,
    ) -> Result<HashMap<RoleId, Arc<Vec<Permission>>>, TdError> {
        let permissions = conn.live().fetch_all::<PermissionDB>(()).await?;
        let total_perms = permissions.len();
        let role_permissions_map: HashMap<RoleId, Arc<Vec<Permission>>> = permissions
            .iter()
            .filter_map(|p| match Self::try_to_permission(p) {
                Ok(perm) => Some((p.role_id, perm)),
                Err(e) => {
                    error!(
                        role_id = %p.role_id,
                        permission_id = %p.id,
                        "authz: invalid PermissionDB row; skipping: {e:#?}"
                    );
                    None
                }
            })
            .into_group_map()
            .into_iter()
            .map(|(role, perms)| (role, Arc::new(perms)))
            .collect();
        debug!(
            role_count = role_permissions_map.len(),
            permission_count = total_perms,
            "authz.provider.get_permissions: loaded {} roles / {} permissions",
            role_permissions_map.len(),
            total_perms
        );
        Ok(role_permissions_map)
    }

    #[instrument(
        name = "authz.provider.get_inter_collection",
        skip_all,
        level = "debug",
        err(Debug)
    )]
    async fn get_inter_collection_permissions(
        &self,
        conn: &mut DynamicConnection<'_>,
    ) -> Result<HashMap<CollectionId, Arc<Vec<ToCollectionId>>>, TdError> {
        let permissions = conn
            .live()
            .fetch_all::<InterCollectionPermissionDB>(())
            .await?;
        let edge_count = permissions.len();
        let permissions_map: HashMap<CollectionId, Arc<Vec<ToCollectionId>>> = permissions
            .iter()
            .map(|p| (p.from_collection_id, p.to_collection_id))
            .into_group_map()
            .into_iter()
            .map(|(role, perms)| (role, Arc::new(perms)))
            .collect();
        debug!(
            edge_count = edge_count,
            "authz.provider.get_inter_collection: loaded {} entries", edge_count
        );
        Ok(permissions_map)
    }
}

impl<'a, 'c> Provider<AuthzData, &'a mut DynamicConnection<'c>> for SqlAuthzDataProvider {
    async fn get(&self, context: &'a mut DynamicConnection<'c>) -> Result<Arc<AuthzData>, TdError> {
        let permissions = self.get_permissions(&mut *context).await?;
        let inter_collections_permissions_value_can_read_key =
            self.get_inter_collection_permissions(&mut *context).await?;
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

    async fn purge(&self, _context: &'a mut DynamicConnection<'c>) -> Result<(), TdError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::authz_provider::SqlAuthzDataProvider;
    use td_common::provider::Provider;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::types::basic::{CollectionName, ProjectId, RoleId, ToCollectionId, UserId};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_get_permissions(db: TdPool) -> Result<(), TdError> {
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

    #[td_test::test(db)]
    async fn test_get_inter_collection_permissions(db: TdPool) -> Result<(), TdError> {
        let scope = db.scoped(ProjectId::sandbox());
        let c0 = scope
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let c1 = scope
            .seed()
            .collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;
        let c2 = scope
            .seed()
            .collection(&CollectionName::try_from("c2")?, &UserId::admin())
            .await?;
        scope
            .seed()
            .inter_collection_permission(&c0.id, &ToCollectionId::try_from(c1.id)?)
            .await?;
        scope
            .seed()
            .inter_collection_permission(&c0.id, &ToCollectionId::try_from(c2.id)?)
            .await?;
        scope
            .seed()
            .inter_collection_permission(&c1.id, &ToCollectionId::try_from(c2.id)?)
            .await?;

        let provider = SqlAuthzDataProvider;
        let permissions = provider
            .get_inter_collection_permissions(&mut db.acquire().await.unwrap())
            .await?;
        assert_eq!(permissions.len(), 2);
        assert_eq!(permissions.get(&c0.id).unwrap().len(), 2);
        assert_eq!(permissions.get(&c1.id).unwrap().len(), 1);
        assert!(!permissions.contains_key(&c2.id));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_provider_get(db: TdPool) -> Result<(), TdError> {
        let scope = db.scoped(ProjectId::sandbox());
        let c0 = scope
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let c1 = scope
            .seed()
            .collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;
        let c2 = scope
            .seed()
            .collection(&CollectionName::try_from("c2")?, &UserId::admin())
            .await?;
        scope
            .seed()
            .inter_collection_permission(&c0.id, &ToCollectionId::try_from(&c1.id)?)
            .await?;
        scope
            .seed()
            .inter_collection_permission(&c0.id, &ToCollectionId::try_from(&c2.id)?)
            .await?;
        scope
            .seed()
            .inter_collection_permission(&c1.id, &ToCollectionId::try_from(&c2.id)?)
            .await?;

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
