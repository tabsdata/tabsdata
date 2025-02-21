//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::layers::{list_users_authorize, list_users_sql_select};
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::tower_service::extractor::extract_req_is_admin;
use td_objects::tower_service::mapper::map_list;
use td_objects::users::dao::UserWithNames;
use td_objects::users::dto::{UserList, UserRead};
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct ListUsersService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<UserList>, TdError>,
}

impl ListUsersService {
    pub fn new(db: DbPool) -> Self {
        ListUsersService {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(extract_req_is_admin::<ListRequest<()>>))
            .layer(from_fn(list_users_authorize))
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
    use crate::logic::users::service::list_users::ListUsersService;
    use std::collections::HashSet;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::users::dto::UserList;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_provider() {
        use crate::logic::users::layers::list_users_authorize;
        use crate::logic::users::layers::list_users_sql_select;
        use crate::logic::users::service::list_users::ListUsersService;
        use td_objects::crudl::{ListRequest, ListResponse};
        use td_objects::tower_service::extractor::extract_req_is_admin;
        use td_objects::tower_service::mapper::map_list;
        use td_objects::users::dao::UserWithNames;
        use td_objects::users::dto::UserList;
        use td_objects::users::dto::UserRead;
        use td_tower::metadata::*;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListUsersService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<UserList>>(&[
            type_of_val(&extract_req_is_admin::<ListRequest<()>>),
            type_of_val(&list_users_authorize),
            type_of_val(&list_users_sql_select), //*
            type_of_val(&map_list::<(), UserWithNames, UserRead>),
        ]);
    }

    #[tokio::test]
    async fn test_list_users() {
        let db = td_database::test_utils::db().await.unwrap();
        let _ = seed_user(&db, None, "u0", true).await;
        let user_id1 = seed_user(&db, None, "u1", true).await;

        let service = ListUsersService::new(db.clone()).service().await;

        let request = RequestContext::with(&user_id1.to_string(), "r", true)
            .await
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
