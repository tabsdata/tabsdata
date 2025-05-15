//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::plan::{
    build_execution_plan, build_function_requirements, build_function_runs, build_response,
    build_table_data_versions, build_transaction_map, build_transactions,
};
use crate::execution::layers::template::{build_execution_template, version_graph};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    combine, BuildService, ExtractDataService, ExtractNameService, ExtractService, TryIntoService,
    UpdateService, With,
};
use td_objects::tower_service::sql::{insert, insert_vec, By, SqlSelectIdOrNameService};
use td_objects::types::basic::{AtTime, CollectionIdName, FunctionId, FunctionIdName};
use td_objects::types::execution::{
    ExecutionDB, ExecutionDBBuilder, ExecutionRequest, ExecutionResponse, FunctionRequirementDB,
    FunctionRunDB, FunctionRunDBBuilder, TableDataVersionDB, TransactionDB, TransactionDBBuilder,
};
use td_objects::types::function::FunctionVersionDBWithNames;
use td_objects::types::trigger::TriggerVersionDBWithNames;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};
use te_execution::transaction::TransactionBy;

pub struct ExecuteFunctionService {
    provider:
        ServiceProvider<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse, TdError>,
}

impl ExecuteFunctionService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        let transaction_by = Arc::new(TransactionBy::Function);
        Self {
            provider: Self::provider(db, queries, transaction_by),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, transaction_by: Arc<TransactionBy>) {
            service_provider!(layers!(
                // Set context
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(transaction_by),

                // Extract from request.
                from_fn(With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract::<RequestContext>),
                from_fn(With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_name::<FunctionParam>),
                from_fn(With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_data::<ExecutionRequest>),
                from_fn(With::<RequestContext>::extract::<AtTime>),

                from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
                from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
                from_fn(combine::<CollectionIdName, FunctionIdName>),

                // DB Transaction start.
                TransactionProvider::new(db),

                // Select trigger function.
                from_fn(By::<(CollectionIdName, FunctionIdName)>::select::<DaoQueries, FunctionVersionDBWithNames>),
                from_fn(With::<FunctionVersionDBWithNames>::extract::<FunctionId>),

                // Create execution template.
                // Find trigger graph
                from_fn(version_graph::<DaoQueries, TriggerVersionDBWithNames>),
                // Create execution template
                from_fn(build_execution_template::<DaoQueries>),

                // Create execution plan.
                // Build execution
                from_fn(With::<FunctionVersionDBWithNames>::convert_to::<ExecutionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<ExecutionDBBuilder, _>),
                from_fn(With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
                from_fn(With::<ExecutionDBBuilder>::build::<ExecutionDB, _>),
                from_fn(insert::<DaoQueries, ExecutionDB>),

                // Build transactions
                from_fn(build_transaction_map),
                from_fn(With::<ExecutionDB>::convert_to::<TransactionDBBuilder, _>),
                from_fn(build_transactions),
                from_fn(insert_vec::<DaoQueries, TransactionDB>),

                // Build new function runs
                from_fn(With::<ExecutionDB>::convert_to::<FunctionRunDBBuilder, _>),
                from_fn(build_function_runs),
                from_fn(insert_vec::<DaoQueries, FunctionRunDB>),

                // Build new table data versions
                from_fn(build_table_data_versions),
                from_fn(insert_vec::<DaoQueries, TableDataVersionDB>),

                // Create execution plan
                from_fn(build_execution_plan::<DaoQueries>),

                // Create steps
                from_fn(build_function_requirements),
                from_fn(insert_vec::<DaoQueries, FunctionRequirementDB>),

                // Execution plan response
                from_fn(build_response),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse, TdError>
    {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{handle_sql_err, RequestContext};
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_function2::seed_function;
    use td_objects::types::basic::AccessTokenId;
    use td_objects::types::basic::RoleId;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, ExecutionName, FunctionName, FunctionRuntimeValues,
        TableDependency, TableName, TableTrigger, TriggeredOn, UserId,
    };
    use td_objects::types::execution::{
        ExecutionDBWithStatus, ExecutionStatus, FunctionRequirementDBWithNames, FunctionRunStatus,
        TableDataVersionDBWithStatus, TransactionDBWithStatus, TransactionStatus,
    };
    use td_objects::types::function::{FunctionDBWithNames, FunctionRegister};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_execute(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let transaction_by = Arc::new(TransactionBy::default());
        let provider = ExecuteFunctionService::provider(db, queries, transaction_by);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata
            .assert_service::<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse>(
                &[
                    // Extract from request.
                    type_of_val(
                        &With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract::<
                            RequestContext,
                        >,
                    ),
                    type_of_val(
                        &With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_name::<
                            FunctionParam,
                        >,
                    ),
                    type_of_val(
                        &With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_data::<
                            ExecutionRequest,
                        >,
                    ),
                    type_of_val(&With::<RequestContext>::extract::<AtTime>),
                    type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                    type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                    type_of_val(&combine::<CollectionIdName, FunctionIdName>),
                    // Select trigger function.
                    type_of_val(
                        &By::<(CollectionIdName, FunctionIdName)>::select::<
                            DaoQueries,
                            FunctionVersionDBWithNames,
                        >,
                    ),
                    type_of_val(&With::<FunctionVersionDBWithNames>::extract::<FunctionId>),
                    // Create execution template.
                    // Find trigger graph
                    type_of_val(&version_graph::<DaoQueries, TriggerVersionDBWithNames>),
                    // Create execution template
                    type_of_val(&build_execution_template::<DaoQueries>),
                    // Create execution plan.
                    // Build execution
                    type_of_val(
                        &With::<FunctionVersionDBWithNames>::convert_to::<ExecutionDBBuilder, _>,
                    ),
                    type_of_val(&With::<RequestContext>::update::<ExecutionDBBuilder, _>),
                    type_of_val(&With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
                    type_of_val(&With::<ExecutionDBBuilder>::build::<ExecutionDB, _>),
                    type_of_val(&insert::<DaoQueries, ExecutionDB>),
                    // Build transactions
                    type_of_val(&build_transaction_map),
                    type_of_val(&With::<ExecutionDB>::convert_to::<TransactionDBBuilder, _>),
                    type_of_val(&build_transactions),
                    type_of_val(&insert_vec::<DaoQueries, TransactionDB>),
                    // Build new function runs
                    type_of_val(&With::<ExecutionDB>::convert_to::<FunctionRunDBBuilder, _>),
                    type_of_val(&build_function_runs),
                    type_of_val(&insert_vec::<DaoQueries, FunctionRunDB>),
                    // Build new table data versions
                    type_of_val(&build_table_data_versions),
                    type_of_val(&insert_vec::<DaoQueries, TableDataVersionDB>),
                    // Create execution plan
                    type_of_val(&build_execution_plan::<DaoQueries>),
                    // Create steps
                    type_of_val(&build_function_requirements),
                    type_of_val(&insert_vec::<DaoQueries, FunctionRequirementDB>),
                    // Execution plan response
                    type_of_val(&build_response),
                ],
            );
    }

    #[td_test::test(sqlx)]
    async fn test_execute(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        let create = FunctionRegister::builder()
            .try_name("function_2")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![
                TableDependency::try_from("table_1")?,
                TableDependency::try_from("table_2")?,
                TableDependency::try_from("table_3")?,
            ])
            .triggers(vec![TableTrigger::try_from("table_1")?])
            .tables(vec![
                TableName::try_from("table_3")?,
                TableName::try_from("table_4")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .create(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("function_1")?
                .build()?,
            ExecutionRequest::builder()
                .name(Some(ExecutionName::try_from("test_execution")?))
                .build()?,
        );

        let service = ExecuteFunctionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        // Check the response
        assert_eq!(
            *response.name(),
            Some(ExecutionName::try_from("test_execution")?)
        );
        assert!(*response.triggered_on() < TriggeredOn::now().await);

        let mut all_functions: Vec<_> = response.all_functions().iter().map(|t| t.name()).collect();
        all_functions.sort();
        assert_eq!(
            all_functions,
            vec![
                &FunctionName::try_from("function_1")?,
                &FunctionName::try_from("function_2")?,
            ]
        );
        let triggered_functions: Vec<_> = response
            .triggered_functions()
            .iter()
            .map(|t| t.name())
            .collect();
        // it can vary depending on the execution
        assert!(triggered_functions.is_empty() || triggered_functions.len() == 1);
        let manual_trigger = response.manual_trigger();
        assert_eq!(
            manual_trigger.name(),
            &FunctionName::try_from("function_1")?
        );

        let mut all_tables: Vec<_> = response.all_tables().iter().map(|t| t.name()).collect();
        all_tables.sort();
        assert_eq!(
            all_tables,
            vec![
                &TableName::try_from("table_1")?,
                &TableName::try_from("table_2")?,
                &TableName::try_from("table_3")?,
                &TableName::try_from("table_4")?
            ]
        );
        let created_tables: Vec<_> = response.created_tables().iter().map(|t| t.name()).collect();
        // it can vary depending on the execution
        assert!(created_tables.len() == 2 || created_tables.len() == 4);

        // Asser db
        let queries = DaoQueries::default();

        // Execution
        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].id(), response.id());
        assert_eq!(executions[0].name(), response.name());
        assert_eq!(executions[0].collection_id(), collection.id());
        assert_eq!(*executions[0].status(), ExecutionStatus::Scheduled);

        // Transaction
        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert!(transactions.len() == 1 || transactions.len() == 2);
        for transaction in transactions {
            assert_eq!(transaction.execution_id(), response.id());
            assert_eq!(*transaction.status(), TransactionStatus::Scheduled);
        }

        // FunctionRun
        let function: FunctionDBWithNames = queries
            .select_by::<FunctionDBWithNames>(&(&FunctionName::try_from("function_1")?))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 1);
        assert_eq!(function_runs[0].collection_id(), collection.id());
        assert_eq!(function_runs[0].execution_id(), response.id());
        assert_eq!(*function_runs[0].status(), FunctionRunStatus::Scheduled);

        let function: FunctionDBWithNames = queries
            .select_by::<FunctionDBWithNames>(&(&FunctionName::try_from("function_2")?))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        // it can vary depending on the execution
        for function_run in function_runs {
            assert_eq!(function_run.collection_id(), collection.id());
            assert_eq!(function_run.execution_id(), response.id());
            assert_eq!(*function_run.status(), FunctionRunStatus::Scheduled);
        }

        // TableDataVersion
        let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
            .select_by::<TableDataVersionDBWithStatus>(&())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        // it can vary depending on the execution
        assert!(table_data_versions.len() == 2 || table_data_versions.len() == 4);
        for table_data_version in table_data_versions {
            assert_eq!(table_data_version.collection_id(), collection.id());
            assert_eq!(table_data_version.execution_id(), response.id());
            assert_eq!(*table_data_version.has_data(), None);
            assert_eq!(*table_data_version.status(), FunctionRunStatus::Scheduled);
        }

        // FunctionCondition
        let function_requirements: Vec<FunctionRequirementDBWithNames> = queries
            .select_by::<FunctionRequirementDBWithNames>(&())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        // it can vary depending on the execution
        assert!(function_requirements.is_empty() || function_requirements.len() == 4);

        let mut function_idxs = HashSet::new();
        for function_condition in &function_requirements {
            assert_eq!(function_condition.collection_id(), collection.id());
            assert_eq!(function_condition.execution_id(), response.id());
            if *function_condition.requirement_table() == TableName::try_from("table_3")? {
                // Self dependency on fist execution, version does not exist, requirement done.
                assert_eq!(*function_condition.status(), FunctionRunStatus::Done);
            } else {
                assert_eq!(*function_condition.status(), FunctionRunStatus::Scheduled);
            }

            if let Some(dependency_pos) = function_condition.requirement_dependency_pos() {
                // In the test, table order matches table name
                assert!(function_condition
                    .requirement_table()
                    .as_str()
                    .contains(&(**dependency_pos + 1).to_string()));
            }

            if let Some(version_idx) = function_condition.requirement_version_idx() {
                function_idxs.insert(**version_idx as usize);
            }
        }
        assert_eq!(
            function_idxs,
            (0..function_requirements
                .iter()
                .filter(|f| f.requirement_version_idx().is_some())
                .collect::<Vec<_>>()
                .len())
                .collect::<HashSet<_>>()
        );

        Ok(())
    }
}
