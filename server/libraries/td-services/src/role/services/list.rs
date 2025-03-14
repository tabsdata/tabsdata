//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::from::{TryMapListService, With};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::role::{Role, RoleBuilder, RoleDBWithNames};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListRoleService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<Role>, TdError>,
}

impl ListRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(RoleQueries::new());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<RoleQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),

                ConnectionProvider::new(db),
                from_fn(By::<()>::list::<(), RoleQueries, RoleDBWithNames>),

                from_fn(With::<RoleDBWithNames>::try_map_list::<(), RoleBuilder, Role, _>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ListRequest<()>, ListResponse<Role>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_role::{get_role, seed_role};
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{Description, RoleName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(RoleQueries::new());
        let provider = ListRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<()>, ListResponse<Role>>(&[
            type_of_val(&By::<()>::list::<(), RoleQueries, RoleDBWithNames>),
            type_of_val(&With::<RoleDBWithNames>::try_map_list::<(), RoleBuilder, Role, _>),
        ]);
    }

    #[tokio::test]
    async fn test_list_role() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let _role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .list((), ListParams::default());

        let service = ListRoleService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(*response.len(), 4); // 3 default roles + 1
        let response = response.data();
        assert_eq!(*response[0].name(), RoleName::try_from("sys_admin")?);
        assert_eq!(*response[1].name(), RoleName::try_from("sec_admin")?);
        assert_eq!(*response[2].name(), RoleName::try_from("user")?);

        let found = get_role(&db, &RoleName::try_from("joaquin").unwrap()).await?;
        let role = response.get(3).unwrap();
        assert_eq!(role.id(), found.id());
        assert_eq!(role.name(), found.name());
        assert_eq!(role.description(), found.description());
        assert_eq!(role.created_on(), found.created_on());
        assert_eq!(role.created_by_id(), found.created_by_id());
        assert_eq!(role.modified_on(), found.modified_on());
        assert_eq!(role.modified_by_id(), found.modified_by_id());
        assert_eq!(role.fixed(), found.fixed());
        Ok(())
    }
}
