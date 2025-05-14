//
// Copyright 2024 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{ExtractService, TryMapListService, With};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::user::{UserDBWithNames, UserRead, UserReadBuilder};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListUsersService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<UserRead>, TdError>,
}

impl ListUsersService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        ListUsersService {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                ConnectionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin>::check),
                from_fn(By::<()>::list::<(), DaoQueries, UserDBWithNames>),
                from_fn(With::<UserDBWithNames>::try_map_list::<(), UserReadBuilder, UserRead, _>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ListRequest<()>, ListResponse<UserRead>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserEnabled, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_list_provider(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ListUsersService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<UserRead>>(&[
            type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin>::check),
            type_of_val(&By::<()>::list::<(), DaoQueries, UserDBWithNames>),
            type_of_val(&With::<UserDBWithNames>::try_map_list::<(), UserReadBuilder, UserRead, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_list_users(db: DbPool) {
        let _ = seed_user(
            &db,
            &UserName::try_from("u0").unwrap(),
            &UserEnabled::from(true),
        )
        .await;
        let user1 = seed_user(
            &db,
            &UserName::try_from("u1").unwrap(),
            &UserEnabled::from(true),
        )
        .await;

        let service = ListUsersService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            user1.id(),
            RoleId::sec_admin(),
            true,
        )
        .list((), ListParams::default());
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        assert_eq!(*list.len(), 3);
        let users = list
            .data()
            .iter()
            .map(|u| u.name().clone())
            .collect::<HashSet<_>>();
        let expected = HashSet::from([
            UserName::admin(),
            UserName::try_from("u0").unwrap(),
            UserName::try_from("u1").unwrap(),
        ]);
        assert_eq!(expected, users);
    }
}
