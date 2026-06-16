//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::list::list_executions_by_tdv;
use ta_services::factory::service_factory;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::PhysicalTableDataVersionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::PhysicalTableDataVersionId;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionListByPhysicalTableDataVersionService,
    request = ListRequest<PhysicalTableDataVersionParam>,
    response = ListResponse<Execution>,
    connection = ConnectionProvider,
    scope = "Project",
)]
fn service() {
    layers!(
        // Extract PhysicalTableDataVersionId from request.
        from_fn(
            With::<ListRequest<PhysicalTableDataVersionParam>>::extract_name::<
                PhysicalTableDataVersionParam,
            >
        ),
        from_fn(With::<PhysicalTableDataVersionParam>::extract::<PhysicalTableDataVersionId>),
        // Wrap into Option to match the nullable column.
        from_fn(With::<PhysicalTableDataVersionId>::some),
        // List executions
        from_fn(With::<ListRequest<PhysicalTableDataVersionParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
        from_fn(list_executions_by_tdv),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::and;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, ProjectId,
        RoleId, TableNameDto, TransactionKey, UserId, VersionPos,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_executions_by_table_data_version(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        ExecutionListByPhysicalTableDataVersionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<PhysicalTableDataVersionParam>, ProjectId>, ListResponse<Execution>>(&[
                // Extract PhysicalTableDataVersionId from request.
                type_of_val(
                    &With::<ListRequest<PhysicalTableDataVersionParam>>::extract_name::<
                        PhysicalTableDataVersionParam,
                    >,
                ),
                type_of_val(&With::<PhysicalTableDataVersionParam>::extract::<PhysicalTableDataVersionId>),
                // Wrap into Option to match the nullable column.
                type_of_val(&With::<PhysicalTableDataVersionId>::some),
                // List executions with pagination (single JOIN query).
                type_of_val(&With::<ListRequest<PhysicalTableDataVersionParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Execution>, _>),
                type_of_val(&list_executions_by_tdv),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_executions_by_table_data_version(db: TdPool) -> Result<(), TdError> {
        // Create collection and function with an output table.
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
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
        let function_version = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        // Create an execution that produces a table data version.
        let execution_1 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version)
            .await?;
        let transaction_1 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution_1, &transaction_key)
            .await?;
        let function_run_1 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution_1,
                &transaction_1,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db
            .live()
            .fetch_one(and!(
                &collection.id,
                &td_objects::types::basic::TableName::try_from(tables[0].clone())?
            ))
            .await?;

        let tdv = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_table_data_version(
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
        let consumer_function_version = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create_consumer)
            .await?;

        // Create a second execution with a function requirement pointing to the TDV.
        let execution_2 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&consumer_function_version)
            .await?;
        let transaction_2 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution_2, &transaction_key)
            .await?;
        let function_run_2 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &collection,
                &consumer_function_version,
                &execution_2,
                &transaction_2,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        // Seed a function requirement that references the TDV as input.
        db.scoped(ProjectId::sandbox())
            .seed()
            .logical_input(
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
        let _execution_3 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version)
            .await?;

        // Call the service.
        let service =
            ExecutionListByPhysicalTableDataVersionService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list(
                    PhysicalTableDataVersionParam::builder()
                        .table_data_version(tdv.id.clone())
                        .build()?,
                    ListParams::default(),
                )
                .in_project(ProjectId::sandbox());
        let response = service.raw_oneshot(request).await?;

        // Only execution_2 should appear (it has the function requirement referencing the TDV).
        assert_eq!(response.len, 1);
        assert_eq!(response.data[0].id, execution_2.id);

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_executions_by_table_data_version_empty(db: TdPool) -> Result<(), TdError> {
        // Create a TDV with no function requirements pointing to it.
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
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
        let function_version = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version)
            .await?;
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db
            .live()
            .fetch_one(and!(
                &collection.id,
                &td_objects::types::basic::TableName::try_from(tables[0].clone())?
            ))
            .await?;

        let tdv = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        // No function requirements reference this TDV.
        let service =
            ExecutionListByPhysicalTableDataVersionService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list(
                    PhysicalTableDataVersionParam::builder()
                        .table_data_version(tdv.id.clone())
                        .build()?,
                    ListParams::default(),
                )
                .in_project(ProjectId::sandbox());
        let response = service.raw_oneshot(request).await?;

        assert_eq!(response.len, 0);

        Ok(())
    }
}
