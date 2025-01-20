//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::list_transactions_sql::list_transactions_sql;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::datasets::dao::DsTransaction;
use td_objects::datasets::dto::TransactionList;
use td_objects::tower_service::mapper::map_list;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::util::BoxService;
use tower::ServiceBuilder;

pub struct ListTransactionsService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<TransactionList>, TdError>,
}

impl ListTransactionsService {
    /// Creates a new instance of [`ListTransactionsService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(list_transactions_sql))
            .layer(from_fn(map_list::<(), DsTransaction, TransactionList>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> BoxService<ListRequest<()>, ListResponse<TransactionList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::datasets::service::list_transactions::ListTransactionsService;
    use td_common::execution_status::TransactionStatus;
    use td_objects::crudl::{ListParams, ListResponse, RequestContext};
    use td_objects::datasets::dto::TransactionList;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_execution_plan::seed_execution_plan;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::test_utils::seed_user::seed_user;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_transactions() {
        use super::ListTransactionsService;
        use crate::logic::datasets::layer::list_transactions_sql::list_transactions_sql;
        use td_objects::crudl::ListRequest;
        use td_objects::crudl::ListResponse;
        use td_objects::datasets::dao::DsTransaction;
        use td_objects::datasets::dto::TransactionList;
        use td_objects::tower_service::mapper::map_list;
        use td_tower::metadata::{type_of_val, Metadata};
        use tower::ServiceExt;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListTransactionsService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<TransactionList>>(&[
            type_of_val(&list_transactions_sql),
            type_of_val(&map_list::<(), DsTransaction, TransactionList>),
        ]);
    }

    #[tokio::test]
    async fn test_list() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let e0 = seed_execution_plan(&db, "exec_plan_0", &collection_id, &d0, &f0, None).await;
        let t0 = seed_transaction(&db, &e0, None, TransactionStatus::Scheduled).await;
        let e1 = seed_execution_plan(&db, "exec_plan_1", &collection_id, &d0, &f0, None).await;
        let t1 = seed_transaction(&db, &e1, None, TransactionStatus::Scheduled).await;

        let service = ListTransactionsService::new(db).service().await;

        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .list((), ListParams::default());
        let response: ListResponse<TransactionList> = service.oneshot(request).await.unwrap();
        assert_eq!(
            response
                .data()
                .iter()
                .map(|t| t.id().to_string())
                .collect::<Vec<_>>(),
            vec![t1.to_string(), t0.to_string()]
        );
    }
}
