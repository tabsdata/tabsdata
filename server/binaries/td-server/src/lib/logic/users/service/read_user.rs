//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::users::layers::read_user_authorize;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::ReadRequest;
use td_objects::dlo::UserName;
use td_objects::tower_service::extractor::{
    extract_name, extract_req_is_admin, extract_req_user_id, extract_user_id,
};
use td_objects::tower_service::finder::find_by_name;
use td_objects::tower_service::mapper::map;
use td_objects::users::dao::UserWithNames;
use td_objects::users::dto::UserRead;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::util::BoxService;
use tower::ServiceBuilder;

pub struct ReadUserService {
    provider: ServiceProvider<ReadRequest<String>, UserRead, TdError>,
}

impl ReadUserService {
    pub fn new(db: DbPool) -> Self {
        ReadUserService {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(
                extract_name::<ReadRequest<String>, String, UserName>,
            ))
            .layer(from_fn(find_by_name::<UserName, UserWithNames>))
            .layer(from_fn(extract_user_id::<UserWithNames>))
            .layer(from_fn(extract_req_is_admin::<ReadRequest<String>>))
            .layer(from_fn(extract_req_user_id::<ReadRequest<String>>))
            .layer(from_fn(read_user_authorize))
            .layer(from_fn(map::<UserWithNames, UserRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(&self) -> BoxService<ReadRequest<String>, UserRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::users::service::read_user::ReadUserService;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_user::seed_user;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_provider() {
        use crate::logic::users::layers::read_user_authorize;
        use crate::logic::users::service::read_user::ReadUserService;
        use td_objects::crudl::ReadRequest;
        use td_objects::dlo::UserName;
        use td_objects::tower_service::extractor::extract_name;
        use td_objects::tower_service::extractor::extract_user_id;
        use td_objects::tower_service::extractor::{extract_req_is_admin, extract_req_user_id};
        use td_objects::tower_service::finder::find_by_name;
        use td_objects::tower_service::mapper::map;
        use td_objects::users::dao::UserWithNames;
        use td_objects::users::dto::UserRead;
        use td_tower::metadata::*;
        use tower::ServiceExt;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ReadUserService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<String>, UserRead>(&[
            type_of_val(&extract_name::<ReadRequest<String>, String, UserName>),
            type_of_val(&find_by_name::<UserName, UserWithNames>),
            type_of_val(&extract_user_id::<UserWithNames>),
            type_of_val(&extract_req_is_admin::<ReadRequest<String>>),
            type_of_val(&extract_req_user_id::<ReadRequest<String>>),
            type_of_val(&read_user_authorize),
            type_of_val(&map::<UserWithNames, UserRead>),
        ]);
    }

    #[tokio::test]
    async fn test_read_user() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;

        let service = ReadUserService::new(db.clone()).service().await;

        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .read("u0");
        let response = service.oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(created.id(), &user_id.to_string());
        assert_eq!(created.name(), "u0");
    }
}
