//
// Copyright 2025 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::execution::FunctionRun;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = FunctionRunListService,
    request = ListRequest<()>,
    response = ListResponse<FunctionRun>,
    connection = ConnectionProvider,
    context = DaoQueries,
)]
fn provider() {
    layers!(
        // No need for authz for this service.

        // List all function runs in the system.
        from_fn(By::<()>::list::<(), DaoQueries, FunctionRun>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, RoleId, TableNameDto, TransactionKey,
        UserId,
    };
    use td_objects::types::execution::FunctionRunStatus;
    use td_objects::types::function::FunctionRegister;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_list_function_run(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = FunctionRunListService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<()>, ListResponse<FunctionRun>>(&[type_of_val(
            &By::<()>::list::<(), DaoQueries, FunctionRun>,
        )]);
    }

    #[td_test::test(sqlx)]
    async fn test_list_function_run(db: DbPool) -> Result<(), TdError> {
        let queries = Arc::new(DaoQueries::default());

        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

        let dependencies = None;
        let triggers = None;
        let tables = vec![TableNameDto::try_from("table_version")?];
        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables.clone())
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = seed_function(&db, &collection, &create).await;
        let transaction_key = TransactionKey::try_from("ANY")?;

        let execution = seed_execution(&db, &function_version).await;

        let function_runs = [
            seed_function_run(
                &db,
                &collection,
                &function_version,
                &execution,
                &seed_transaction(&db, &execution, &transaction_key).await,
                &FunctionRunStatus::Done,
            )
            .await,
            seed_function_run(
                &db,
                &collection,
                &function_version,
                &execution,
                &seed_transaction(&db, &execution, &transaction_key).await,
                &FunctionRunStatus::Done,
            )
            .await,
            seed_function_run(
                &db,
                &collection,
                &function_version,
                &execution,
                &seed_transaction(&db, &execution, &transaction_key).await,
                &FunctionRunStatus::Done,
            )
            .await,
            seed_function_run(
                &db,
                &collection,
                &function_version,
                &execution,
                &seed_transaction(&db, &execution, &transaction_key).await,
                &FunctionRunStatus::Done,
            )
            .await,
            seed_function_run(
                &db,
                &collection,
                &function_version,
                &execution,
                &seed_transaction(&db, &execution, &transaction_key).await,
                &FunctionRunStatus::Done,
            )
            .await,
        ];

        // Test
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .list((), ListParams::default());

        let service = FunctionRunListService::new(db.clone(), queries)
            .service()
            .await;
        let response = service.raw_oneshot(request).await?;
        assert_eq!(*response.len(), function_runs.len());
        assert_eq!(response.data()[0].id(), function_runs[0].id());
        assert_eq!(
            response.data()[0].triggered_on(),
            function_runs[0].triggered_on()
        );
        assert_eq!(response.data()[1].id(), function_runs[1].id());
        assert_eq!(
            response.data()[1].triggered_on(),
            function_runs[1].triggered_on()
        );
        assert_eq!(response.data()[2].id(), function_runs[2].id());
        assert_eq!(
            response.data()[2].triggered_on(),
            function_runs[2].triggered_on()
        );
        assert_eq!(response.data()[3].id(), function_runs[3].id());
        assert_eq!(
            response.data()[3].triggered_on(),
            function_runs[3].triggered_on()
        );
        assert_eq!(response.data()[4].id(), function_runs[4].id());
        assert_eq!(
            response.data()[4].triggered_on(),
            function_runs[4].triggered_on()
        );
        Ok(())
    }
}
