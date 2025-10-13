//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::execution::Worker;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = WorkerListService,
    request = ListRequest<()>,
    response = ListResponse<Worker>,
    connection = ConnectionProvider,
    context = DaoQueries,
)]
fn service() {
    layers!(
        // No need for authz for this service.

        // List all Workers.
        from_fn(By::<()>::list::<(), NoListFilter, Worker>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::test_utils::seed_worker::seed_worker;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId,
        TransactionKey, UserId,
    };
    use td_objects::types::execution::WorkerMessageStatus;
    use td_objects::types::function::FunctionRegister;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_workers(db: DbPool) {
        use td_tower::metadata::type_of_val;

        WorkerListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<Worker>>(&[
                // List all Workers.
                type_of_val(&By::<()>::list::<(), NoListFilter, Worker>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_workers(db: DbPool) -> Result<(), TdError> {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

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
        let function_version = seed_function(&db, &collection, &create).await;
        let execution = seed_execution(&db, &function_version).await;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        let function_run = seed_function_run(
            &db,
            &collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        let workers = [
            seed_worker(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Unlocked,
            )
            .await,
        ];

        let service = WorkerListService::with_defaults(db.clone()).service().await;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default());

        let response = service.raw_oneshot(request).await?;
        assert_eq!(*response.len(), workers.len());
        assert_eq!(response.data()[0].id(), workers[0].id());
        assert_eq!(
            response.data()[0].message_status(),
            workers[0].message_status()
        );
        assert_eq!(response.data()[1].id(), workers[1].id());
        assert_eq!(
            response.data()[1].message_status(),
            workers[1].message_status()
        );
        assert_eq!(response.data()[2].id(), workers[2].id());
        assert_eq!(
            response.data()[2].message_status(),
            workers[2].message_status()
        );
        assert_eq!(response.data()[3].id(), workers[3].id());
        assert_eq!(
            response.data()[3].message_status(),
            workers[3].message_status()
        );
        assert_eq!(response.data()[4].id(), workers[4].id());
        assert_eq!(
            response.data()[4].message_status(),
            workers[4].message_status()
        );
        Ok(())
    }
}
