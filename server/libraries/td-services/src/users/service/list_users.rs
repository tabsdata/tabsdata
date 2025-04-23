//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::layers::list_users_sql_select;
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::extractor::extract_req_context;
use td_objects::tower_service::mapper::map_list;
use td_objects::users::dao::UserWithNames;
use td_objects::users::dto::{UserList, UserRead};
use td_tower::default_services::{
    ConnectionProvider, ServiceEntry, ServiceReturn, Share, SrvCtxProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct ListUsersService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<UserList>, TdError>,
}

impl ListUsersService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        ListUsersService {
            provider: Self::provider(db, authz_context),
        }
    }

    fn provider<Req: Share, Res: Share>(
        db: DbPool,
        authz_context: Arc<AuthzContext>,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(SrvCtxProvider::new(authz_context))
            .layer(from_fn(extract_req_context::<ListRequest<()>>))
            .layer(from_fn(AuthzOn::<System>::set))
            .layer(from_fn(Authz::<SecAdmin>::check))
            .layer(from_fn(list_users_sql_select))
            .layer(from_fn(map_list::<(), UserWithNames, UserRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(&self) -> TdBoxService<ListRequest<()>, ListResponse<UserList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::users::service::list_users::ListUsersService;
    use std::collections::HashSet;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId};
    use td_objects::users::dto::UserList;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_provider() {
        use crate::users::layers::list_users_sql_select;
        use crate::users::service::list_users::ListUsersService;
        use td_authz::Authz;
        use td_objects::crudl::{ListRequest, ListResponse};
        use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
        use td_objects::tower_service::extractor::extract_req_context;
        use td_objects::tower_service::mapper::map_list;
        use td_objects::users::dao::UserWithNames;
        use td_objects::users::dto::UserList;
        use td_objects::users::dto::UserRead;
        use td_tower::metadata::*;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListUsersService::provider(db, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<UserList>>(&[
            type_of_val(&extract_req_context::<ListRequest<()>>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin>::check),
            type_of_val(&list_users_sql_select), //*
            type_of_val(&map_list::<(), UserWithNames, UserRead>),
        ]);
    }

    #[tokio::test]
    async fn test_list_users() {
        let db = td_database::test_utils::db().await.unwrap();
        let _ = seed_user(&db, None, "u0", true).await;
        let user_id1 = seed_user(&db, None, "u1", true).await;

        let service = ListUsersService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            user_id1,
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
            .map(UserList::name)
            .map(String::to_string)
            .collect::<HashSet<_>>();
        let expected = HashSet::from(["admin".to_string(), "u0".to_string(), "u1".to_string()]);
        assert_eq!(expected, users);
    }
}
