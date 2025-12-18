//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::DO_AUTHZ;
use crate::execution::layers::execution::{
    assert_function_status, build_execution_plan, build_function_requirements, build_function_runs,
    build_response, build_table_data_versions, build_transaction_map, build_transactions,
    resolve_execution_plan_versions, update_initial_function_run_status, validate_execution_plan,
    with_recursion_options,
};
use crate::graph::layers::{FunctionOrTable, GraphConfig, generate_td_graph};
use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_execution::TransactionBy;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::execution::{
    ExecutionDB, ExecutionDBBuilder, ExecutionRequest, ExecutionResponse,
};
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::function_requirement::FunctionRequirementDB;
use td_objects::dxo::function_run::{FunctionRunDB, FunctionRunDBBuilder};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::table_data_version::TableDataVersionDB;
use td_objects::dxo::transaction::{TransactionDB, TransactionDBBuilder};
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollExec};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractNameService, ExtractService, IntoService, SetService,
    TryIntoService, UpdateService, With, combine,
};
use td_objects::tower_service::sql::{By, SqlSelectService, insert, insert_vec};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, FunctionId, FunctionIdName, FunctionOnly,
};
use td_objects::types::recursion::RecursionOptions;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

pub struct GraphOpts;
impl
    GraphConfig<
        { TriggerDBWithNames::Active },
        { DependencyDBWithNames::Active },
        { FunctionDB::Executable },
        { TableDB::Available },
    > for GraphOpts
{
    type Authz = DO_AUTHZ;

    fn include_dependency_functions(&self) -> bool {
        false
    }
}

#[service_factory(
    name = ExecuteFunctionService,
    request = CreateRequest<FunctionParam, ExecutionRequest>,
    response = ExecutionResponse,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
    context = TransactionBy,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract::<RequestContext>),
        from_fn(
            With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_name::<FunctionParam>
        ),
        from_fn(
            With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_data::<ExecutionRequest>
        ),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        from_fn(combine::<CollectionIdName, FunctionIdName>),
        // Select trigger function.
        from_fn(
            By::<(CollectionIdName, FunctionIdName)>::select_version::<
                { FunctionDBWithNames::All },
                FunctionDBWithNames,
            >
        ),
        from_fn(assert_function_status),
        // check requester is coll_admin or coll_exec for the function's collection
        from_fn(With::<FunctionDBWithNames>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollExec>::check),
        // Get recursion options
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionId>::convert_to::<FunctionOrTable>),
        from_fn(With::<ExecutionRequest>::extract::<FunctionOnly>),
        from_fn(with_recursion_options),
        // Find td graph
        generate_td_graph(GraphOpts),
        // Build execution plan
        from_fn(build_execution_plan),
        from_fn(validate_execution_plan),
        // Build execution
        from_fn(With::<FunctionDBWithNames>::try_convert_to::<ExecutionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<ExecutionDBBuilder, _>),
        from_fn(With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
        from_fn(With::<RecursionOptions>::set::<ExecutionDBBuilder>),
        from_fn(With::<ExecutionDBBuilder>::build::<ExecutionDB, _>),
        from_fn(insert::<ExecutionDB>),
        // Build transactions
        from_fn(build_transaction_map),
        from_fn(With::<ExecutionDB>::try_convert_to::<TransactionDBBuilder, _>),
        from_fn(build_transactions),
        from_fn(insert_vec::<TransactionDB>),
        // Build new function runs
        from_fn(With::<ExecutionDB>::try_convert_to::<FunctionRunDBBuilder, _>),
        from_fn(build_function_runs),
        from_fn(insert_vec::<FunctionRunDB>),
        // Build new table data versions
        from_fn(build_table_data_versions),
        from_fn(insert_vec::<TableDataVersionDB>),
        // Create execution plan
        from_fn(resolve_execution_plan_versions),
        // Create steps
        from_fn(build_function_requirements),
        from_fn(insert_vec::<FunctionRequirementDB>),
        // Update function run initial status if needed
        from_fn(update_initial_function_run_status),
        // Execution plan response
        from_fn(build_response),
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::LazyLock;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_execution::EXECUTION_GRAPH_OPTIONS;
    use td_objects::dxo::crudl::handle_sql_err;
    use td_objects::dxo::execution::ExecutionDBWithStatus;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::function_requirement::FunctionRequirementDBWithNames;
    use td_objects::dxo::function_run::FunctionRunDBWithNames;
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::table_data_version::TableDataVersionDBWithNames;
    use td_objects::dxo::transaction::TransactionDBWithStatus;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
    use td_objects::test_utils::seed_table_data_version::seed_table_data_version;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Depth, ExecutionName, ExecutionStatus,
        FunctionName, FunctionRunStatus, FunctionRuntimeValues, RoleId, TableName, TableNameDto,
        ToCollectionId, TransactionKey, TransactionStatus, TriggeredOn, UserId,
    };
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_execute(db: DbPool) {
        use crate::graph::layers::build::GraphBuilder;
        use td_objects::dxo::dependency::DependencyDBWithNames;
        use td_objects::dxo::inter_collection_access::{
            InterCollectionAccess, InterCollectionAccessBuilder,
        };
        use td_objects::dxo::trigger::TriggerDBWithNames;
        use td_objects::tower_service::authz::InterColl;
        use td_objects::tower_service::from::{ConvertIntoMapService, VecBuildService};
        use td_tower::metadata::type_of_val;

        ExecuteFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<
            FunctionParam,
            ExecutionRequest,
        >, ExecutionResponse>(&[
            // Extract from request.
            type_of_val(
                &With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract::<RequestContext>,
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
                &By::<(CollectionIdName, FunctionIdName)>::select_version::<
                    { FunctionDBWithNames::All },
                    FunctionDBWithNames,
                >,
            ),
            type_of_val(&assert_function_status),
            // check requester is coll_admin or coll_exec for the function's collection
            type_of_val(&With::<FunctionDBWithNames>::extract::<CollectionId>),
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollExec>::check),
            // Get recursion options
            type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
            type_of_val(&With::<FunctionId>::convert_to::<FunctionOrTable>),
            type_of_val(&With::<ExecutionRequest>::extract::<FunctionOnly>),
            type_of_val(&with_recursion_options),
            // Find td graph
            // Find triggers
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_recursive_trigger_graph),
            // inter collection authz check
            type_of_val(
                &With::<TriggerDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>,
            ),
            type_of_val(
                &With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>,
            ),
            type_of_val(&Authz::<InterColl>::check_inter_collection),
            // Find dependencies. For clarity, we always show the function, even if
            // generating a table centered graph, the same way we show the tables of
            // a function centered graph.
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::main_function_id),
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::unique_triggered_function_ids),
            type_of_val(
                &GraphBuilder::<GraphOpts, _, _, _, _>::find_triggered_functions_dependency_graph,
            ),
            // inter collection authz check
            type_of_val(
                &With::<DependencyDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>,
            ),
            type_of_val(
                &With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>,
            ),
            type_of_val(&Authz::<InterColl>::check_inter_collection),
            // Find output tables
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_functions_output_tables),
            // Find all tables and functions
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_all_tables_map),
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_all_functions_map),
            // Create td graph and validate it
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::build_td_graph),
            // Build execution plan
            type_of_val(&build_execution_plan),
            type_of_val(&validate_execution_plan),
            // Build execution
            type_of_val(&With::<FunctionDBWithNames>::try_convert_to::<ExecutionDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<ExecutionDBBuilder, _>),
            type_of_val(&With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
            type_of_val(&With::<RecursionOptions>::set::<ExecutionDBBuilder>),
            type_of_val(&With::<ExecutionDBBuilder>::build::<ExecutionDB, _>),
            type_of_val(&insert::<ExecutionDB>),
            // Build transactions
            type_of_val(&build_transaction_map),
            type_of_val(&With::<ExecutionDB>::try_convert_to::<TransactionDBBuilder, _>),
            type_of_val(&build_transactions),
            type_of_val(&insert_vec::<TransactionDB>),
            // Build new function runs
            type_of_val(&With::<ExecutionDB>::try_convert_to::<FunctionRunDBBuilder, _>),
            type_of_val(&build_function_runs),
            type_of_val(&insert_vec::<FunctionRunDB>),
            // Build new table data versions
            type_of_val(&build_table_data_versions),
            type_of_val(&insert_vec::<TableDataVersionDB>),
            // Create execution plan
            type_of_val(&resolve_execution_plan_versions),
            // Create steps
            type_of_val(&build_function_requirements),
            type_of_val(&insert_vec::<FunctionRequirementDB>),
            // Update function run initial status if needed
            type_of_val(&update_initial_function_run_status),
            // Execution plan response
            type_of_val(&build_response),
        ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_dependencies_different_collections_permissions_ok(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = test_execute(db, true, false, true, false).await?;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_dependencies_different_collections_permissions_forbidden(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = test_execute(db, true, false, false, false).await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_trigger_different_collections_permissions_ok(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = test_execute(db, false, true, true, false).await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_trigger_different_collections_permissions_forbidden(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = test_execute(db, false, true, false, false).await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_single_function(db: DbPool) -> Result<(), TdError> {
        let _ = test_execute(db, true, false, true, true).await?;
        Ok(())
    }

    static COLLECTION_NAMES: LazyLock<Vec<CollectionName>> = LazyLock::new(|| {
        vec![
            CollectionName::try_from("collection_0").unwrap(),
            CollectionName::try_from("collection_1").unwrap(),
            CollectionName::try_from("collection_2").unwrap(),
        ]
    });

    static FUNCTION_NAMES: LazyLock<Vec<FunctionName>> = LazyLock::new(|| {
        vec![
            FunctionName::try_from("function_0").unwrap(),
            FunctionName::try_from("function_1").unwrap(),
            FunctionName::try_from("function_2").unwrap(),
            FunctionName::try_from("function_3").unwrap(),
        ]
    });

    pub(crate) async fn test_execute(
        db: DbPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
        with_permission: bool,
        single_function: bool,
    ) -> Result<ExecutionResponse, TdError> {
        let collection_0 = seed_collection(&db, &COLLECTION_NAMES[0], &UserId::admin()).await;
        let collection_1 = seed_collection(&db, &COLLECTION_NAMES[1], &UserId::admin()).await;
        let collection_2 = seed_collection(&db, &COLLECTION_NAMES[2], &UserId::admin()).await;

        seed_inter_collection_permission(
            &db,
            &collection_0.id,
            &ToCollectionId::try_from(&collection_1.id)?,
        )
        .await;

        seed_inter_collection_permission(
            &db,
            &collection_0.id,
            &ToCollectionId::try_from(&collection_2.id)?,
        )
        .await;

        if with_permission {
            seed_inter_collection_permission(
                &db,
                &collection_1.id,
                &ToCollectionId::try_from(&collection_2.id)?,
            )
            .await;
        }

        let graph_depth = if single_function {
            Some(Depth::ZERO)
        } else {
            EXECUTION_GRAPH_OPTIONS.max_downstream_depth()
        };

        // Trigger function without permission restrictions (collection_0), which triggers function_1
        // and function_2. These will trigger function_3, depending on the test.
        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_0")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection_0, &create).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[1].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(vec![TableTriggerDto::try_from(format!(
                "{}/table_0",
                COLLECTION_NAMES[0],
            ))?])
            .tables(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection_1, &create).await;

        let bundle_id = BundleId::default();
        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[2].clone())
            .try_description("function_3 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_3 snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(vec![TableTriggerDto::try_from(format!(
                "{}/table_0",
                COLLECTION_NAMES[0],
            ))?])
            .tables(Some(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection_2, &create).await;

        let deps_collection = if deps_diff_collection {
            &COLLECTION_NAMES[1]
        } else {
            &COLLECTION_NAMES[2]
        };
        let triggers_collection = if triggers_diff_collection {
            &COLLECTION_NAMES[1]
        } else {
            &COLLECTION_NAMES[2]
        };

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[3].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![
                TableDependencyDto::try_from(format!("{deps_collection}/table_1"))?,
                TableDependencyDto::try_from(format!("{deps_collection}/table_2"))?,
                TableDependencyDto::try_from("table_3")?,
            ])
            .triggers(vec![TableTriggerDto::try_from(format!(
                "{triggers_collection}/table_1"
            ))?])
            .tables(vec![
                TableNameDto::try_from("table_3")?,
                TableNameDto::try_from("table_4")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection_2, &create).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionParam::builder()
                    .try_collection(collection_0.name.to_string())?
                    .try_function(FUNCTION_NAMES[0].to_string())?
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .function_only(single_function)
                    .build()?,
            );

        let service = ExecuteFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;

        // Test assertions
        let expected_all_functions = match graph_depth {
            Some(depth) => FUNCTION_NAMES
                .iter()
                .take(*depth as usize + 1)
                .collect::<Vec<_>>(),
            None => FUNCTION_NAMES.iter().collect::<Vec<_>>(),
        };

        // with 0 or 1 functions, there is not going to be an inter collection permission issue
        if with_permission || expected_all_functions.len() <= 1 {
            let response = response?;

            // Check the response
            assert_eq!(
                response.name,
                Some(ExecutionName::try_from("test_execution")?)
            );
            assert!(response.triggered_on < TriggeredOn::now());

            let mut all_functions: Vec<_> =
                response.all_functions.values().map(|t| &t.name).collect();
            all_functions.sort();
            assert_eq!(expected_all_functions, all_functions);

            let expected_triggered_functions = match graph_depth {
                Some(depth) => FUNCTION_NAMES
                    .iter()
                    .take(*depth as usize + 1)
                    .collect::<Vec<_>>(),
                None => FUNCTION_NAMES.iter().collect::<Vec<_>>(),
            };
            let mut triggered_functions: Vec<_> = response
                .triggered_functions
                .iter()
                .map(|t| &response.all_functions[t].name)
                .collect();
            triggered_functions.sort();
            assert_eq!(expected_triggered_functions, triggered_functions);

            let manual_trigger = &response.all_functions[&response.manual_trigger];
            assert_eq!(manual_trigger.name, FUNCTION_NAMES[0]);

            let all_tables_with_collection = vec![
                TableName::try_from("collection_0/table_0")?,
                TableName::try_from("collection_1/table_1")?,
                TableName::try_from("collection_1/table_2")?,
                TableName::try_from("collection_2/table_1")?,
                TableName::try_from("collection_2/table_2")?,
                TableName::try_from("collection_2/table_3")?,
                TableName::try_from("collection_2/table_4")?,
            ];
            let expected_all_tables = match graph_depth {
                Some(depth) => all_tables_with_collection
                    .into_iter()
                    .take(*depth as usize + 1)
                    .collect::<Vec<_>>(),
                None => all_tables_with_collection.into_iter().collect::<Vec<_>>(),
            };

            let mut all_tables: Vec<_> = response
                .all_tables
                .values()
                .map(|t| TableName::try_from(format!("{}/{}", t.collection, t.name)).unwrap())
                .collect();
            all_tables.sort();
            assert_eq!(expected_all_tables, all_tables);

            // In this test, all tables are user tables.
            assert_eq!(response.user_tables.len(), all_tables.len());
            assert_eq!(response.system_tables.len(), 0);

            // In our test, created tables are all of them.
            let expected_created_tables = expected_all_tables;
            let mut created_tables: Vec<_> = response
                .created_tables
                .iter()
                .map(|t| {
                    TableName::try_from(format!(
                        "{}/{}",
                        &response.all_tables[t].collection, &response.all_tables[t].name
                    ))
                    .unwrap()
                })
                .collect();
            created_tables.sort();
            assert_eq!(expected_created_tables, created_tables);

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
            assert_eq!(executions[0].id, response.id);
            assert_eq!(executions[0].name, response.name);
            assert_eq!(executions[0].collection_id, collection_0.id);
            assert_eq!(executions[0].status, ExecutionStatus::Scheduled);

            // Transaction
            let transactions: Vec<TransactionDBWithStatus> = queries
                .select_by::<TransactionDBWithStatus>(&())?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            assert!(transactions.len() == 1 || transactions.len() == 3);
            for transaction in transactions {
                assert_eq!(&transaction.execution_id, &response.id);
                assert_eq!(transaction.status, TransactionStatus::Scheduled);
            }

            // FunctionRun
            let function: FunctionDBWithNames = queries
                .select_by::<FunctionDBWithNames>(&FUNCTION_NAMES[0])?
                .build_query_as()
                .fetch_one(&db)
                .await
                .map_err(handle_sql_err)?;
            let function_runs: Vec<FunctionRunDB> = queries
                .select_by::<FunctionRunDB>(&function.id)?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(function_runs.len(), 1);
            assert_eq!(function_runs[0].collection_id, collection_0.id);
            assert_eq!(function_runs[0].execution_id, response.id);
            assert_eq!(function_runs[0].status, FunctionRunStatus::Scheduled);

            let function: FunctionDBWithNames = queries
                .select_by::<FunctionDBWithNames>(&(&FunctionName::try_from("function_2")?))?
                .build_query_as()
                .fetch_one(&db)
                .await
                .map_err(handle_sql_err)?;
            let function_runs: Vec<FunctionRunDB> = queries
                .select_by::<FunctionRunDB>(&(&function.id))?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            // it can vary depending on the execution
            for function_run in function_runs {
                assert_eq!(&function_run.execution_id, &response.id);
                assert_eq!(function_run.status, FunctionRunStatus::Scheduled);
            }

            // TableDataVersion
            let table_data_versions: Vec<TableDataVersionDBWithNames> = queries
                .select_by::<TableDataVersionDBWithNames>(&())?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            // assert_eq!(table_data_versions.len(), expected_created_tables.len());
            for table_data_version in table_data_versions {
                assert_eq!(&table_data_version.execution_id, &response.id);
                assert_eq!(table_data_version.has_data, None);
                assert_eq!(table_data_version.status, FunctionRunStatus::Scheduled);
                // assert!(
                //     expected_created_tables.contains(&TableName::try_from(format!(
                //         "{}/{}",
                //         table_data_version.collection, table_data_version.name
                //     ))?)
                // );
            }

            // FunctionCondition
            let function_requirements: Vec<FunctionRequirementDBWithNames> = queries
                .select_by::<FunctionRequirementDBWithNames>(&())?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            let expected_num_function_requirements = if graph_depth.is_some() {
                0
            } else {
                6 // because of the 4 dependencies + 2 triggers
            };
            assert_eq!(
                expected_num_function_requirements,
                function_requirements.len()
            );

            let mut function_idxs = HashSet::new();
            for function_condition in &function_requirements {
                assert_eq!(function_condition.execution_id, response.id);
                if function_condition.requirement_table == TableName::try_from("table_3")? {
                    // Self dependency on fist execution, version does not exist, requirement done.
                    assert_eq!(function_condition.status, FunctionRunStatus::Committed);
                } else {
                    assert_eq!(function_condition.status, FunctionRunStatus::Scheduled);
                }

                if let Some(dependency_pos) = &function_condition.requirement_dependency_pos {
                    // In the test, table order matches table name
                    assert!(
                        function_condition
                            .requirement_table
                            .as_str()
                            .contains(&(**dependency_pos + 1).to_string())
                    );
                }

                if let Some(input_idx) = &function_condition.requirement_input_idx {
                    function_idxs.insert(**input_idx as usize);
                }
            }
            assert_eq!(
                function_idxs,
                (0..function_requirements
                    .iter()
                    .filter(|f| f.requirement_input_idx.is_some())
                    .collect::<Vec<_>>()
                    .len())
                    .collect::<HashSet<_>>()
            );
            Ok(response)
        } else {
            let err = response.err().unwrap();
            assert_eq!(
                std::mem::discriminant(&AuthzError::ForbiddenInterCollectionAccess("".to_string())),
                std::mem::discriminant(err.domain_err())
            );
            Err(err)
        }
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_initial_status_upstream_scheduled(db: DbPool) -> Result<(), TdError> {
        test_execute_upstream_initial_status(
            db,
            FunctionRunStatus::Scheduled,
            FunctionRunStatus::Scheduled,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_initial_status_upstream_on_hold(db: DbPool) -> Result<(), TdError> {
        test_execute_upstream_initial_status(
            db,
            FunctionRunStatus::OnHold,
            FunctionRunStatus::OnHold,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execute_initial_status_upstream_failed(db: DbPool) -> Result<(), TdError> {
        test_execute_upstream_initial_status(
            db,
            FunctionRunStatus::Failed,
            FunctionRunStatus::OnHold,
        )
        .await
    }

    async fn test_execute_upstream_initial_status(
        db: DbPool,
        upstream_status: FunctionRunStatus,
        expected_initial_status: FunctionRunStatus,
    ) -> Result<(), TdError> {
        let collection = seed_collection(&db, &COLLECTION_NAMES[0], &UserId::admin()).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_0")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_0 = seed_function(&db, &collection, &create).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[1].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_1 = seed_function(&db, &collection, &create).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[2].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![
                TableDependencyDto::try_from("table_0")?,
                TableDependencyDto::try_from("table_1")?,
            ])
            .triggers(vec![
                TableTriggerDto::try_from("table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ])
            .tables(vec![TableNameDto::try_from("table_2")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _function_2 = seed_function(&db, &collection, &create).await;

        // seed a function_run for function_0, with OnHold status
        let execution = seed_execution(&db, &function_0).await;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;
        let function_run = seed_function_run(
            &db,
            &collection,
            &function_0,
            &execution,
            &transaction,
            &upstream_status,
        )
        .await;
        let table_name = TableName::try_from("table_0")?;
        let table: TableDB = DaoQueries::default()
            .select_by::<TableDB>(&(&collection.id, &table_name))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        let _table_data_version = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;

        // seed a function_run for function_1, with Done status
        let execution = seed_execution(&db, &function_1).await;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;
        let function_run = seed_function_run(
            &db,
            &collection,
            &function_1,
            &execution,
            &transaction,
            &FunctionRunStatus::Done,
        )
        .await;
        let table_name = TableName::try_from("table_1")?;
        let table: TableDB = DaoQueries::default()
            .select_by::<TableDB>(&(&collection.id, &table_name))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        let _table_data_version = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;

        // And then run function_2
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionParam::builder()
                    .try_collection(collection.name.to_string())?
                    .try_function(FUNCTION_NAMES[2].to_string())?
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            );

        let service = ExecuteFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let _response = service.raw_oneshot(request).await?;

        // there should only be one function run for function_2
        let function_run: FunctionRunDBWithNames = DaoQueries::default()
            .select_by::<FunctionRunDBWithNames>(&FUNCTION_NAMES[2])?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();

        // and in OnHold status, because even tho function_1 is Done, function_0 is not ready
        assert_eq!(function_run.status, expected_initial_status);
        Ok(())
    }
}
