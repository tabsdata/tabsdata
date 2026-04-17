//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::list::list_executions_by_tdv;
use ta_services::factory::service_factory;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::TableDataVersionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::TableDataVersionId;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionListByTableDataVersionService,
    request = ListRequest<TableDataVersionParam>,
    response = ListResponse<Execution>,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(
        // Extract TableDataVersionId from request.
        from_fn(With::<ListRequest<TableDataVersionParam>>::extract_name::<TableDataVersionParam>),
        from_fn(With::<TableDataVersionParam>::extract::<TableDataVersionId>),
        // Wrap into Option to match the nullable column.
        from_fn(With::<TableDataVersionId>::some),
        // List executions with pagination (single JOIN query).
        from_fn(With::<ListRequest<TableDataVersionParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
        from_fn(list_executions_by_tdv),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::and;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId,
        TableNameDto, TransactionKey, UserId, VersionPos,
    };
    use td_queries::BasicQueries;
    use td_queries::test_utils::SeedQueries;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_executions_by_table_data_version(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ExecutionListByTableDataVersionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<TableDataVersionParam>, ListResponse<Execution>>(&[
                // Extract TableDataVersionId from request.
                type_of_val(
                    &With::<ListRequest<TableDataVersionParam>>::extract_name::<
                        TableDataVersionParam,
                    >,
                ),
                type_of_val(&With::<TableDataVersionParam>::extract::<TableDataVersionId>),
                // Wrap into Option to match the nullable column.
                type_of_val(&With::<TableDataVersionId>::some),
                // List executions with pagination (single JOIN query).
                type_of_val(&With::<ListRequest<TableDataVersionParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
                type_of_val(&list_executions_by_tdv),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_executions_by_table_data_version(db: TdPool) -> Result<(), TdError> {
        // Create collection and function with an output table.
        let collection = db
            .seed_collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![TableNameDto::try_from("output_table")?];
        let create = FunctionRegister::builder()
            .try_name("producer_fn")?
            .try_description("produces data")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables.clone())
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = db.seed_function(&collection, &create).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        // Create an execution that produces a table data version.
        let execution_1 = db.seed_execution(&function_version).await?;
        let transaction_1 = db.seed_transaction(&execution_1, &transaction_key).await?;
        let function_run_1 = db
            .seed_function_run(
                &collection,
                &function_version,
                &execution_1,
                &transaction_1,
                &FunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db
            .fetch_one(and!(
                &collection.id,
                &td_objects::types::basic::TableName::try_from(tables[0].clone())?
            ))
            .await?;

        let tdv = db
            .seed_table_data_version(
                &collection,
                &execution_1,
                &transaction_1,
                &function_run_1,
                &table,
            )
            .await?;

        // Create a second function that consumes the TDV.
        let create_consumer = FunctionRegister::builder()
            .try_name("consumer_fn")?
            .try_description("consumes data")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let consumer_function_version = db.seed_function(&collection, &create_consumer).await?;

        // Create a second execution with a function requirement pointing to the TDV.
        let execution_2 = db.seed_execution(&consumer_function_version).await?;
        let transaction_2 = db.seed_transaction(&execution_2, &transaction_key).await?;
        let function_run_2 = db
            .seed_function_run(
                &collection,
                &consumer_function_version,
                &execution_2,
                &transaction_2,
                &FunctionRunStatus::Scheduled,
            )
            .await?;

        // Seed a function requirement that references the TDV as input.
        db.seed_function_requirement(
            &collection,
            &execution_2,
            &transaction_2,
            &function_run_2,
            &table,
            Some(&function_run_1),
            Some(&tdv),
            None,
            &VersionPos::try_from(0)?,
        )
        .await?;

        // Create a third execution that does NOT reference the TDV.
        let _execution_3 = db.seed_execution(&function_version).await?;

        // Call the service.
        let service = ExecutionListByTableDataVersionService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                TableDataVersionParam::builder()
                    .table_data_version(tdv.id.clone())
                    .build()?,
                ListParams::default(),
            );
        let response = service.raw_oneshot(request).await?;

        // Only execution_2 should appear (it has the function requirement referencing the TDV).
        assert_eq!(response.len, 1);
        assert_eq!(response.data[0].id, execution_2.id);

        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_executions_by_table_data_version_empty(db: TdPool) -> Result<(), TdError> {
        // Create a TDV with no function requirements pointing to it.
        let collection = db
            .seed_collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![TableNameDto::try_from("some_table")?];
        let create = FunctionRegister::builder()
            .try_name("some_fn")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables.clone())
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = db.seed_function(&collection, &create).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        let execution = db.seed_execution(&function_version).await?;
        let transaction = db.seed_transaction(&execution, &transaction_key).await?;
        let function_run = db
            .seed_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db
            .fetch_one(and!(
                &collection.id,
                &td_objects::types::basic::TableName::try_from(tables[0].clone())?
            ))
            .await?;

        let tdv = db
            .seed_table_data_version(&collection, &execution, &transaction, &function_run, &table)
            .await?;

        // No function requirements reference this TDV.
        let service = ExecutionListByTableDataVersionService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                TableDataVersionParam::builder()
                    .table_data_version(tdv.id.clone())
                    .build()?,
                ListParams::default(),
            );
        let response = service.raw_oneshot(request).await?;

        assert_eq!(response.len, 0);

        Ok(())
    }
}
