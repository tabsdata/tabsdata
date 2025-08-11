//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::plan::{
    build_execution_plan, build_function_requirements, build_function_runs, build_response,
    build_table_data_versions, build_transaction_map, build_transactions,
};
use crate::execution::layers::template::assert_active_status;
use crate::execution::layers::template::{
    build_execution_template, find_all_input_tables, find_trigger_graph,
};
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollExec, InterColl};
use td_objects::tower_service::from::{
    combine, BuildService, ConvertIntoMapService, ExtractDataService, ExtractNameService,
    ExtractService, TryIntoService, UpdateService, VecBuildService, With,
};
use td_objects::tower_service::sql::{insert, insert_vec, By, SqlSelectService};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, FunctionId, FunctionIdName, FunctionStatus,
};
use td_objects::types::dependency::DependencyDBWithNames;
use td_objects::types::execution::{
    ExecutionDB, ExecutionDBBuilder, ExecutionRequest, ExecutionResponse, FunctionRequirementDB,
    FunctionRunDB, FunctionRunDBBuilder, TableDataVersionDB, TransactionDB, TransactionDBBuilder,
};
use td_objects::types::function::FunctionDBWithNames;
use td_objects::types::permission::{InterCollectionAccess, InterCollectionAccessBuilder};
use td_objects::types::trigger::TriggerDBWithNames;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};
use te_execution::transaction::TransactionBy;

#[provider(
    name = ExecuteFunctionService,
    request = CreateRequest<FunctionParam, ExecutionRequest>,
    response = ExecutionResponse,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
    context = TransactionBy,
)]
fn provider() {
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
        from_fn(FunctionStatus::none),
        from_fn(By::<(CollectionIdName, FunctionIdName)>::select_version::<FunctionDBWithNames>),
        from_fn(assert_active_status),
        // check requester is coll_admin or coll_exec for the function's collection
        from_fn(With::<FunctionDBWithNames>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollExec>::check),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        // Find trigger graph
        from_fn(find_trigger_graph),
        // Find all input tables
        from_fn(find_all_input_tables),
        // inter collection authz check
        from_fn(With::<DependencyDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>),
        from_fn(With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
        from_fn(Authz::<InterColl>::check_inter_collection),
        // Create execution template
        from_fn(build_execution_template),
        // inter collection authz check
        from_fn(With::<TriggerDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>),
        from_fn(With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
        from_fn(Authz::<InterColl>::check_inter_collection),
        // Create execution plan.
        // Build execution
        from_fn(With::<FunctionDBWithNames>::convert_to::<ExecutionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<ExecutionDBBuilder, _>),
        from_fn(With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
        from_fn(With::<ExecutionDBBuilder>::build::<ExecutionDB, _>),
        from_fn(insert::<ExecutionDB>),
        // Build transactions
        from_fn(build_transaction_map),
        from_fn(With::<ExecutionDB>::convert_to::<TransactionDBBuilder, _>),
        from_fn(build_transactions),
        from_fn(insert_vec::<TransactionDB>),
        // Build new function runs
        from_fn(With::<ExecutionDB>::convert_to::<FunctionRunDBBuilder, _>),
        from_fn(build_function_runs),
        from_fn(insert_vec::<FunctionRunDB>),
        // Build new table data versions
        from_fn(build_table_data_versions),
        from_fn(insert_vec::<TableDataVersionDB>),
        // Create execution plan
        from_fn(build_execution_plan),
        // Create steps
        from_fn(build_function_requirements),
        from_fn(insert_vec::<FunctionRequirementDB>),
        // Execution plan response
        from_fn(build_response),
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashSet;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{handle_sql_err, RequestContext};
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::basic::{
        AccessTokenId, ExecutionStatus, FunctionRunStatus, TableDependencyDto, TableNameDto,
        TableTriggerDto, TransactionStatus,
    };
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, ExecutionName, FunctionName, FunctionRuntimeValues,
        TableName, TriggeredOn, UserId,
    };
    use td_objects::types::basic::{RoleId, ToCollectionId};
    use td_objects::types::execution::{ExecutionDBWithStatus, TransactionDBWithStatus};
    use td_objects::types::execution::{
        FunctionRequirementDBWithNames, TableDataVersionDBWithFunction,
    };
    use td_objects::types::function::{FunctionDBWithNames, FunctionRegister};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_execute(db: DbPool) {
        use td_objects::tower_service::authz::InterColl;
        use td_objects::tower_service::from::{ConvertIntoMapService, VecBuildService};
        use td_objects::types::dependency::DependencyDBWithNames;
        use td_objects::types::permission::{InterCollectionAccess, InterCollectionAccessBuilder};
        use td_tower::metadata::type_of_val;

        ExecuteFunctionService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse>(
                &[
                    // Extract from request.
                    type_of_val(&With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract::<RequestContext>),
                    type_of_val(&
                        With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_name::<FunctionParam>
                    ),
                    type_of_val(&
                        With::<CreateRequest<FunctionParam, ExecutionRequest>>::extract_data::<ExecutionRequest>
                    ),
                    type_of_val(&With::<RequestContext>::extract::<AtTime>),
                    type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                    type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                    type_of_val(&combine::<CollectionIdName, FunctionIdName>),
                    // Select trigger function.
                    type_of_val(&FunctionStatus::none),
                    type_of_val(&By::<(CollectionIdName, FunctionIdName)>::select_version::<FunctionDBWithNames>),
                    type_of_val(&assert_active_status),
                    // check requester is coll_admin or coll_exec for the function's collection
                    type_of_val(&With::<FunctionDBWithNames>::extract::<CollectionId>),
                    type_of_val(&AuthzOn::<CollectionId>::set),
                    type_of_val(&Authz::<CollAdmin, CollExec>::check),
                    type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                    // Find trigger graph
                    type_of_val(&find_trigger_graph),
                    // Find all input tables
                    type_of_val(&find_all_input_tables),
                    // inter collection authz check
                    type_of_val(&With::<DependencyDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                    type_of_val(&With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
                    type_of_val(&Authz::<InterColl>::check_inter_collection),
                    // Create execution template
                    type_of_val(&build_execution_template),
                    // inter collection authz check
                    type_of_val(&With::<TriggerDBWithNames>::vec_convert_to::<InterCollectionAccessBuilder, _>),
                    type_of_val(&With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
                    type_of_val(&Authz::<InterColl>::check_inter_collection),
                    // Create execution plan.
                    // Build execution
                    type_of_val(&With::<FunctionDBWithNames>::convert_to::<ExecutionDBBuilder, _>),
                    type_of_val(&With::<RequestContext>::update::<ExecutionDBBuilder, _>),
                    type_of_val(&With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
                    type_of_val(&With::<ExecutionDBBuilder>::build::<ExecutionDB, _>),
                    type_of_val(&insert::<ExecutionDB>),
                    // Build transactions
                    type_of_val(&build_transaction_map),
                    type_of_val(&With::<ExecutionDB>::convert_to::<TransactionDBBuilder, _>),
                    type_of_val(&build_transactions),
                    type_of_val(&insert_vec::<TransactionDB>),
                    // Build new function runs
                    type_of_val(&With::<ExecutionDB>::convert_to::<FunctionRunDBBuilder, _>),
                    type_of_val(&build_function_runs),
                    type_of_val(&insert_vec::<FunctionRunDB>),
                    // Build new table data versions
                    type_of_val(&build_table_data_versions),
                    type_of_val(&insert_vec::<TableDataVersionDB>),
                    // Create execution plan
                    type_of_val(&build_execution_plan),
                    // Create steps
                    type_of_val(&build_function_requirements),
                    type_of_val(&insert_vec::<FunctionRequirementDB>),
                    // Execution plan response
                    type_of_val(&build_response),
                ],
            );
    }

    #[td_test::test(sqlx)]
    async fn test_execute_dependencies_different_collections_permissions_ok(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = test_execute(db, true, false, true).await?;
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_execute_dependencies_different_collections_permissions_forbidden(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = test_execute(db, true, false, false).await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_execute_trigger_different_collections_permissions_ok(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = test_execute(db, false, true, true).await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_execute_trigger_different_collections_permissions_forbidden(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = test_execute(db, false, true, false).await;
        Ok(())
    }

    pub(crate) async fn test_execute(
        db: DbPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
        with_permission: bool,
    ) -> Result<ExecutionResponse, TdError> {
        let collection_name_0 = CollectionName::try_from("collection_0")?;
        let collection_0 = seed_collection(&db, &collection_name_0, &UserId::admin()).await;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = seed_collection(&db, &collection_name_1, &UserId::admin()).await;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = seed_collection(&db, &collection_name_2, &UserId::admin()).await;

        seed_inter_collection_permission(
            &db,
            collection_0.id(),
            &ToCollectionId::try_from(collection_1.id())?,
        )
        .await;

        seed_inter_collection_permission(
            &db,
            collection_0.id(),
            &ToCollectionId::try_from(collection_2.id())?,
        )
        .await;

        if with_permission {
            seed_inter_collection_permission(
                &db,
                collection_1.id(),
                &ToCollectionId::try_from(collection_2.id())?,
            )
            .await;
        }

        // Trigger function without permission restrictions (collection_0), which triggers function_1
        // and function_2. These will trigger function_3, depending on the test.
        let create = FunctionRegister::builder()
            .try_name("function_0")?
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
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(vec![TableTriggerDto::try_from("collection_0/table_0")?])
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
            .try_name("function_2")?
            .try_description("function_3 description")?
            .bundle_id(bundle_id)
            .try_snippet("function_3 snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(vec![TableTriggerDto::try_from("collection_0/table_0")?])
            .tables(Some(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection_2, &create).await;

        let deps_collection = if deps_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };
        let triggers_collection = if triggers_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };

        let create = FunctionRegister::builder()
            .try_name("function_3")?
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
                    .try_collection(format!("{}", collection_0.name()))?
                    .try_function("function_0")?
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            );

        let service = ExecuteFunctionService::with_defaults(db.clone())
            .await
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = if with_permission {
            let response = response?;

            // Check the response
            assert_eq!(
                *response.name(),
                Some(ExecutionName::try_from("test_execution")?)
            );
            assert!(*response.triggered_on() < TriggeredOn::now().await);

            let all_functions_map = response.all_functions();
            let mut all_functions: Vec<_> = response
                .all_functions()
                .values()
                .map(|t| t.name())
                .collect();
            all_functions.sort();
            assert_eq!(
                all_functions,
                vec![
                    &FunctionName::try_from("function_0")?,
                    &FunctionName::try_from("function_1")?,
                    &FunctionName::try_from("function_2")?,
                    &FunctionName::try_from("function_3")?,
                ]
            );
            let triggered_functions: Vec<_> = response
                .triggered_functions()
                .iter()
                .map(|t| &all_functions_map[t])
                .collect();
            // it can vary depending on the execution
            assert!(triggered_functions.is_empty() || triggered_functions.len() == 3);
            let manual_trigger = &all_functions_map[response.manual_trigger()];
            assert_eq!(
                manual_trigger.name(),
                &FunctionName::try_from("function_0")?
            );

            let all_tables_map = response.all_tables();
            let mut all_tables: Vec<_> = all_tables_map
                .values()
                .map(|t| TableName::try_from(format!("{}/{}", t.collection(), t.name())).unwrap())
                .collect();
            all_tables.sort();
            assert_eq!(
                all_tables,
                vec![
                    TableName::try_from("collection_0/table_0")?,
                    TableName::try_from("collection_1/table_1")?,
                    TableName::try_from("collection_1/table_2")?,
                    TableName::try_from("collection_2/table_1")?,
                    TableName::try_from("collection_2/table_2")?,
                    TableName::try_from("collection_2/table_3")?,
                    TableName::try_from("collection_2/table_4")?
                ]
            );
            // In this test, all tables are user tables.
            assert_eq!(response.user_tables().len(), all_tables.len());
            assert_eq!(response.system_tables().len(), 0);

            let created_tables: Vec<_> = response
                .created_tables()
                .iter()
                .map(|t| &all_tables_map[t])
                .collect();
            // it can vary depending on the execution
            assert!(created_tables.len() == 1 || created_tables.len() == 7);

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
            assert_eq!(executions[0].collection_id(), collection_0.id());
            assert_eq!(*executions[0].status(), ExecutionStatus::Scheduled);

            // Transaction
            let transactions: Vec<TransactionDBWithStatus> = queries
                .select_by::<TransactionDBWithStatus>(&())?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            assert!(transactions.len() == 1 || transactions.len() == 3);
            for transaction in transactions {
                assert_eq!(transaction.execution_id(), response.id());
                assert_eq!(*transaction.status(), TransactionStatus::Scheduled);
            }

            // FunctionRun
            let function: FunctionDBWithNames = queries
                .select_by::<FunctionDBWithNames>(&(&FunctionName::try_from("function_0")?))?
                .build_query_as()
                .fetch_one(&db)
                .await
                .map_err(handle_sql_err)?;
            let function_runs: Vec<FunctionRunDB> = queries
                .select_by::<FunctionRunDB>(&(function.id()))?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(function_runs.len(), 1);
            assert_eq!(function_runs[0].collection_id(), collection_0.id());
            assert_eq!(function_runs[0].execution_id(), response.id());
            assert_eq!(*function_runs[0].status(), FunctionRunStatus::Scheduled);

            let function: FunctionDBWithNames = queries
                .select_by::<FunctionDBWithNames>(&(&FunctionName::try_from("function_2")?))?
                .build_query_as()
                .fetch_one(&db)
                .await
                .map_err(handle_sql_err)?;
            let function_runs: Vec<FunctionRunDB> = queries
                .select_by::<FunctionRunDB>(&(function.id()))?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            // it can vary depending on the execution
            for function_run in function_runs {
                assert_eq!(function_run.execution_id(), response.id());
                assert_eq!(*function_run.status(), FunctionRunStatus::Scheduled);
            }

            // TableDataVersion
            let table_data_versions: Vec<TableDataVersionDBWithFunction> = queries
                .select_by::<TableDataVersionDBWithFunction>(&())?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            // it can vary depending on the execution
            assert!(table_data_versions.len() == 1 || table_data_versions.len() == 7);
            for table_data_version in table_data_versions {
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
            assert!(function_requirements.is_empty() || function_requirements.len() == 6);

            let mut function_idxs = HashSet::new();
            for function_condition in &function_requirements {
                assert_eq!(function_condition.execution_id(), response.id());
                if *function_condition.requirement_table() == TableName::try_from("table_3")? {
                    // Self dependency on fist execution, version does not exist, requirement done.
                    assert_eq!(*function_condition.status(), FunctionRunStatus::Committed);
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

                if let Some(input_idx) = function_condition.requirement_input_idx() {
                    function_idxs.insert(**input_idx as usize);
                }
            }
            assert_eq!(
                function_idxs,
                (0..function_requirements
                    .iter()
                    .filter(|f| f.requirement_input_idx().is_some())
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
        };
        response
    }
}
