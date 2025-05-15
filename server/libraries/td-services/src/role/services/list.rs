//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::role::Role;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListRoleService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<Role>, TdError>,
}

impl ListRoleService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                ConnectionProvider::new(db),
                SrvCtxProvider::new(authz_context),
                from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                from_fn(By::<()>::list::<(), DaoQueries, Role>),
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
    use td_objects::types::basic::{AccessTokenId, Description, RoleId, RoleName, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_list_role(db: DbPool) {
        use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ListRoleService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<()>, ListResponse<Role>>(&[
            type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
            type_of_val(&By::<()>::list::<(), DaoQueries, Role>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_list_role(db: DbPool) -> Result<(), TdError> {
        let _role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .list((), ListParams::default());

        let service = ListRoleService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
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
