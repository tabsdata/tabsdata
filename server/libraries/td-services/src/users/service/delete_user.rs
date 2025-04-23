//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::layers::{delete_user_sql_delete, delete_user_validate};
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::DeleteRequest;
use td_objects::dlo::UserName;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::extractor::{
    extract_name, extract_req_context, extract_req_user_id, extract_user_id,
};
use td_objects::tower_service::finder::find_by_name;
use td_objects::users::dao::User;
use td_tower::default_services::{
    ServiceEntry, ServiceReturn, Share, SrvCtxProvider, TransactionProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct DeleteUserService {
    provider: ServiceProvider<DeleteRequest<String>, (), TdError>,
}

impl DeleteUserService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        DeleteUserService {
            provider: Self::provider(db, authz_context),
        }
    }

    fn provider<Req: Share, Res: Share>(
        db: DbPool,
        authz_context: Arc<AuthzContext>,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(SrvCtxProvider::new(authz_context))
            .layer(from_fn(extract_req_context::<DeleteRequest<String>>))
            .layer(from_fn(AuthzOn::<System>::set))
            .layer(from_fn(Authz::<SecAdmin>::check))
            .layer(from_fn(extract_req_user_id::<DeleteRequest<String>>))
            .layer(from_fn(
                extract_name::<DeleteRequest<String>, String, UserName>,
            ))
            .layer(from_fn(find_by_name::<UserName, User>))
            .layer(from_fn(extract_user_id::<User>))
            .layer(from_fn(delete_user_validate))
            // TODO delete user from user roles
            .layer(from_fn(delete_user_sql_delete))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<String>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::users::service::delete_user::DeleteUserService;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_objects::crudl::RequestContext;
    use td_objects::entity_finder::users::UserWithNamesFinder;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_delete_provider() {
        use crate::users::layers::delete_user_sql_delete;
        use crate::users::layers::delete_user_validate;
        use crate::users::service::delete_user::DeleteUserService;
        use td_authz::Authz;
        use td_objects::crudl::DeleteRequest;
        use td_objects::dlo::UserName;
        use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
        use td_objects::tower_service::extractor::extract_name;
        use td_objects::tower_service::extractor::extract_req_context;
        use td_objects::tower_service::extractor::extract_req_user_id;
        use td_objects::tower_service::extractor::extract_user_id;
        use td_objects::tower_service::finder::find_by_name;
        use td_objects::users::dao::User;
        use td_tower::metadata::*;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = DeleteUserService::provider(db, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<DeleteRequest<String>, ()>(&[
            type_of_val(&extract_req_context::<DeleteRequest<String>>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin>::check),
            type_of_val(&extract_req_user_id::<DeleteRequest<String>>),
            type_of_val(&extract_name::<DeleteRequest<String>, String, UserName>),
            type_of_val(&find_by_name::<UserName, User>),
            type_of_val(&extract_user_id::<User>),
            type_of_val(&delete_user_validate),   //*
            type_of_val(&delete_user_sql_delete), //*
        ]);
    }

    #[tokio::test]
    async fn test_delete_user() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id0 = seed_user(&db, None, "u0", true).await;

        let service = DeleteUserService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            false,
        )
        .delete("u0");
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let res = UserWithNamesFinder::default()
            .find_by_id(&mut db.acquire().await.unwrap(), &user_id0.to_string())
            .await;
        assert!(res.is_err());

        const SELECT: &str = "SELECT count(*) FROM users WHERE name = ?1";

        let found: i64 = sqlx::query_scalar(SELECT)
            .bind("u0".to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(found, 0);
    }
}
