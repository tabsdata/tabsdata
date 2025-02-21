//
// Copyright 2025 Tabs Data Inc.
//

use crate::logic::datasets::layer::list_ds_worker_messages::list_ds_worker_messages;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::datasets::dao::DsWorkerMessageWithNames;
use td_objects::datasets::dto::WorkerMessageList;
use td_objects::rest_urls::{By, WorkerMessageListParam};
use td_objects::tower_service::extractor::extract_name;
use td_objects::tower_service::mapper::map_list;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListWorkerMessagesService {
    provider: ServiceProvider<
        ListRequest<WorkerMessageListParam>,
        ListResponse<WorkerMessageList>,
        TdError,
    >,
}

impl ListWorkerMessagesService {
    /// Creates a new instance of [`ListWorkerMessagesService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db.clone()),
        }
    }

    p! {
        provider(db: DbPool) -> TdError {
            service_provider!(layers!(
                ConnectionProvider::new(db),
                from_fn(extract_name::<ListRequest<WorkerMessageListParam>, WorkerMessageListParam, By>),
                from_fn(list_ds_worker_messages),
                from_fn(map_list::<WorkerMessageListParam, DsWorkerMessageWithNames, WorkerMessageList>)
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<WorkerMessageListParam>, ListResponse<WorkerMessageList>, TdError>
    {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_common::execution_status::TransactionStatus;
    use td_common::time::UniqueUtc;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::ByParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version_full;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_execution_plan::seed_execution_plan;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_worker_message::seed_worker_message;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_worker_message_service() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListWorkerMessagesService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata
            .assert_service::<ListRequest<WorkerMessageListParam>, ListResponse<WorkerMessageList>>(
                &[
                    type_of_val(
                        &extract_name::<
                            ListRequest<WorkerMessageListParam>,
                            WorkerMessageListParam,
                            By,
                        >,
                    ),
                    type_of_val(&list_ds_worker_messages),
                    type_of_val(
                        &map_list::<
                            WorkerMessageListParam,
                            DsWorkerMessageWithNames,
                            WorkerMessageList,
                        >,
                    ),
                ],
            );
    }

    enum ByParamTest {
        Function,
        Transaction,
        ExecutionPlan,
        DataVersion,
    }

    async fn test_list_by(by_param: ByParamTest) {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "collection").await;

        let (dataset_id, function_id) = seed_dataset(
            &db,
            None,
            &collection_id,
            "dataset",
            &["table"],
            &[],
            &[],
            "hash",
        )
        .await;

        let execution_plan_id = seed_execution_plan(
            &db,
            "exec_plan_0",
            &collection_id,
            &dataset_id,
            &function_id,
            None,
        )
        .await;

        let transaction_id =
            seed_transaction(&db, &execution_plan_id, None, TransactionStatus::Scheduled).await;

        let triggered_on = UniqueUtc::now_millis().await;
        let started_on = UniqueUtc::now_millis().await;
        let data_version_id = seed_data_version_full(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &transaction_id,
            None,
            &execution_plan_id,
            "M",
            &triggered_on,
            Some(&started_on),
            None,
            None,
            "R",
        )
        .await;

        let message_id = seed_worker_message(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &transaction_id,
            &execution_plan_id,
            &data_version_id,
        )
        .await;

        let by_param = match by_param {
            ByParamTest::Function => ByParam::function_id(function_id),
            ByParamTest::Transaction => ByParam::transaction_id(transaction_id),
            ByParamTest::ExecutionPlan => ByParam::execution_plan_id(execution_plan_id),
            ByParamTest::DataVersion => ByParam::data_version_id(data_version_id),
        };

        let service = ListWorkerMessagesService::new(db.clone()).service().await;
        let request: ListRequest<WorkerMessageListParam> =
            RequestContext::with(&user_id.to_string(), "r", false)
                .await
                .list(
                    WorkerMessageListParam::new(&by_param).unwrap(),
                    ListParams::default(),
                );

        let response: ListResponse<WorkerMessageList> = service.raw_oneshot(request).await.unwrap();

        assert_eq!(*response.len(), 1);
        assert_eq!(response.data()[0].id(), &message_id.to_string());
        assert_eq!(
            response.data()[0].collection_id(),
            &collection_id.to_string()
        );
        assert_eq!(response.data()[0].dataset_id(), &dataset_id.to_string());
        assert_eq!(response.data()[0].function_id(), &function_id.to_string());
        assert_eq!(
            response.data()[0].transaction_id(),
            &transaction_id.to_string()
        );
        assert_eq!(
            response.data()[0].execution_plan_id(),
            &execution_plan_id.to_string()
        );
        assert_eq!(
            response.data()[0].data_version_id(),
            &data_version_id.to_string()
        );
        assert_eq!(
            response.data()[0].started_on(),
            &started_on.timestamp_millis()
        );
    }

    #[tokio::test]
    async fn test_list_by_function_id() {
        test_list_by(ByParamTest::Function).await;
    }

    #[tokio::test]
    async fn test_list_by_transaction_id() {
        test_list_by(ByParamTest::Transaction).await;
    }

    #[tokio::test]
    async fn test_list_by_execution_plan_id() {
        test_list_by(ByParamTest::ExecutionPlan).await;
    }

    #[tokio::test]
    async fn test_list_by_data_version_id() {
        test_list_by(ByParamTest::DataVersion).await;
    }
}
