//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::layers::{delete_collection_contents, delete_collection_sql_delete};
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::collections::dao::Collection;
use td_objects::crudl::DeleteRequest;
use td_objects::dlo::CollectionName;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::authz::{AuthzOn, SysAdmin, System};
use td_objects::tower_service::extractor::{
    extract_collection_id, extract_name, extract_req_context,
};
use td_objects::tower_service::finder::find_by_name;
use td_tower::default_services::{
    ServiceEntry, ServiceReturn, Share, SrvCtxProvider, TransactionProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct DeleteCollectionService {
    provider: ServiceProvider<DeleteRequest<CollectionParam>, (), TdError>,
}

impl DeleteCollectionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        DeleteCollectionService {
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
            .layer(from_fn(
                extract_req_context::<DeleteRequest<CollectionParam>>,
            ))
            .layer(from_fn(AuthzOn::<System>::set))
            .layer(from_fn(Authz::<SysAdmin>::check))
            .layer(from_fn(
                extract_name::<DeleteRequest<CollectionParam>, CollectionParam, CollectionName>,
            ))
            .layer(from_fn(find_by_name::<CollectionName, Collection>))
            .layer(from_fn(extract_collection_id::<Collection>))
            // TODO delete permissions with this collection
            .layer(from_fn(delete_collection_contents))
            .layer(from_fn(delete_collection_sql_delete))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<CollectionParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::collections::service::delete_collection::DeleteCollectionService;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_delete_service() {
        use crate::logic::collections::layers::{
            delete_collection_contents, delete_collection_sql_delete,
        };
        use crate::logic::collections::service::delete_collection::DeleteCollectionService;
        use td_authz::Authz;
        use td_objects::collections::dao::Collection;
        use td_objects::crudl::DeleteRequest;
        use td_objects::dlo::CollectionName;
        use td_objects::tower_service::authz::{AuthzOn, SysAdmin, System};
        use td_objects::tower_service::extractor::extract_req_context;
        use td_objects::tower_service::extractor::{extract_collection_id, extract_name};
        use td_objects::tower_service::finder::find_by_name;
        use td_tower::metadata::{type_of_val, Metadata};
        let db = td_database::test_utils::db().await.unwrap();
        let provider = DeleteCollectionService::provider(db, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<CollectionParam>, ()>(&[
            type_of_val(&extract_req_context::<DeleteRequest<CollectionParam>>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SysAdmin>::check),
            type_of_val(
                &extract_name::<DeleteRequest<CollectionParam>, CollectionParam, CollectionName>,
            ),
            type_of_val(&find_by_name::<CollectionName, Collection>),
            type_of_val(&extract_collection_id::<Collection>),
            type_of_val(&delete_collection_contents),
            type_of_val(&delete_collection_sql_delete),
        ]);
    }

    #[tokio::test]
    async fn test_delete_collection() {
        let db = td_database::test_utils::db().await.unwrap();
        seed_collection(&db, None, "ds0").await;

        let service = DeleteCollectionService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            false,
        )
        .delete(CollectionParam::new("ds0"));

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        const SELECT: &str = "SELECT count(*) FROM collections WHERE name = ?1";

        let found: i64 = sqlx::query_scalar(SELECT)
            .bind("ds0".to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(found, 0);
    }
}
