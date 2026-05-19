//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::worker::Worker;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = WorkerListService,
    request = ListRequest<()>,
    response = ListResponse<Worker>,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(
        // No need for authz for this service.

        // List all Workers.
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Worker>, _>),
        from_fn(By::<()>::list::<NoFilter, Worker>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, RoleId,
        TransactionKey, UserId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_workers(db: TdPool) {
        use td_tower::metadata::type_of_val;

        WorkerListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<Worker>>(&[
                // List all Workers.
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Worker>, _>),
                type_of_val(&By::<()>::list::<NoFilter, Worker>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_workers(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = db.seed().function(&collection, &create).await?;
        let execution = db.seed().execution(&function_version).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db.seed().transaction(&execution, &transaction_key).await?;

        let physical_function_run = db
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        let workers = [
            db.seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.seed()
                .worker(
                    &collection,
                    &function_version,
                    &WorkerType::CronTrigger,
                    &WorkerStatus::Requested,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
        ];

        let service = WorkerListService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default());

        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.len, workers.len());
        assert_eq!(response.data[0].id, workers[0].id);
        assert_eq!(response.data[0].message_status, workers[0].message_status);
        assert_eq!(response.data[0].worker_type, workers[0].worker_type);
        assert_eq!(response.data[0].worker_type, WorkerType::FunctionRunner);
        assert_eq!(response.data[1].id, workers[1].id);
        assert_eq!(response.data[1].message_status, workers[1].message_status);
        assert_eq!(response.data[1].worker_type, workers[1].worker_type);
        assert_eq!(response.data[2].id, workers[2].id);
        assert_eq!(response.data[2].message_status, workers[2].message_status);
        assert_eq!(response.data[2].worker_type, workers[2].worker_type);
        assert_eq!(response.data[3].id, workers[3].id);
        assert_eq!(response.data[3].message_status, workers[3].message_status);
        assert_eq!(response.data[3].worker_type, workers[3].worker_type);
        assert_eq!(response.data[4].worker_type, workers[4].worker_type);
        assert_eq!(response.data[4].id, workers[4].id);
        assert_eq!(response.data[4].message_status, workers[4].message_status);
        assert_eq!(response.data[4].worker_type, workers[4].worker_type);
        assert_eq!(response.data[4].worker_type, WorkerType::CronTrigger);
        Ok(())
    }
}
