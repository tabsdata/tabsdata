//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::SKIP_AUTHZ;
use crate::execution::layers::execution::{
    build_execution_plan, build_response, resolve_execution_plan_versions,
};
use crate::execution::layers::read::{
    build_existing_transaction_map, with_existing_recursion_options,
};
use crate::graph::layers::{FunctionOrTable, GraphConfig, generate_td_graph};
use ta_services::factory::service_factory;
use td_execution::TransactionBy;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::execution::{ExecutionDB, ExecutionResponse};
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::rest_urls::ExecutionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    ExtractNameService, ExtractService, IntoService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectService};
use td_objects::types::basic::{
    AtTime, ExecutionId, ExecutionIdName, FunctionId, FunctionVersionId, TriggeredOn,
};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

pub struct GraphOpts;
impl
    GraphConfig<
        { TriggerDBWithNames::Active },
        { DependencyDBWithNames::Active },
        { FunctionDB::Readable },
        { TableDB::Available },
    > for GraphOpts
{
    type Authz = SKIP_AUTHZ;

    fn include_dependency_functions(&self) -> bool {
        false
    }
}

// Very similar to trigger, but without triggering. It selects entities needed to build an execution plan.
#[service_factory(
    name = ExecutionReadService,
    request = ReadRequest<ExecutionParam>,
    response = ExecutionResponse,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = TransactionBy,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
        // Find Plan and its TriggeredOn (and convert to AtTime).
        from_fn(With::<ExecutionParam>::extract::<ExecutionIdName>),
        from_fn(By::<ExecutionIdName>::select::<ExecutionDB>),
        from_fn(With::<ExecutionDB>::extract::<TriggeredOn>),
        from_fn(With::<TriggeredOn>::try_convert_to::<AtTime, _>),
        // Find function that triggered the plan.
        from_fn(With::<ExecutionDB>::extract::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::select::<FunctionDBWithNames>),
        // Get recursion options
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionId>::convert_to::<FunctionOrTable>),
        from_fn(with_existing_recursion_options),
        // Find td graph
        generate_td_graph(GraphOpts),
        // Build execution plan
        from_fn(build_execution_plan),
        // Get transactions
        from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
        from_fn(By::<ExecutionId>::select_all::<TransactionDB>),
        from_fn(build_existing_transaction_map),
        // Create execution plan
        from_fn(resolve_execution_plan_versions),
        // Execution plan response
        from_fn(build_response),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::tests::test_execute;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_execution(db: DbPool) {
        use crate::graph::layers::build::GraphBuilder;
        use td_tower::metadata::type_of_val;

        ExecutionReadService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<ExecutionParam>, ExecutionResponse>(&[
            // Extract from request.
            type_of_val(&With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
            // Find Plan and its TriggeredOn (and convert to AtTime).
            type_of_val(&With::<ExecutionParam>::extract::<ExecutionIdName>),
            type_of_val(&By::<ExecutionIdName>::select::<ExecutionDB>),
            type_of_val(&With::<ExecutionDB>::extract::<TriggeredOn>),
            type_of_val(&With::<TriggeredOn>::try_convert_to::<AtTime, _>),
            // Find function that triggered the plan.
            type_of_val(&With::<ExecutionDB>::extract::<FunctionVersionId>),
            type_of_val(&By::<FunctionVersionId>::select::<FunctionDBWithNames>),
            // Get recursion options
            type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
            type_of_val(&With::<FunctionId>::convert_to::<FunctionOrTable>),
            type_of_val(&with_existing_recursion_options),
            // Find td graph
            // Find triggers
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_recursive_trigger_graph),
            // Find dependencies. For clarity, we always show the function, even if
            // generating a table centered graph, the same way we show the tables of
            // a function centered graph.
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::main_function_id),
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::unique_triggered_function_ids),
            type_of_val(
                &GraphBuilder::<GraphOpts, _, _, _, _>::find_triggered_functions_dependency_graph,
            ),
            // Find output tables
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_functions_output_tables),
            // Find all tables and functions
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_all_tables_map),
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_all_functions_map),
            // Create td graph and validate it
            type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::build_td_graph),
            // Build execution plan
            type_of_val(&build_execution_plan),
            // Get transactions
            type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
            type_of_val(&By::<ExecutionId>::select_all::<TransactionDB>),
            type_of_val(&build_existing_transaction_map),
            // Create execution plan
            type_of_val(&resolve_execution_plan_versions),
            // Execution plan response
            type_of_val(&build_response),
        ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read_execution(db: DbPool) -> Result<(), TdError> {
        let execution = test_execute(db.clone(), false, false, true, false).await?;

        let service = ExecutionReadService::with_defaults(db.clone())
            .service()
            .await;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                ExecutionParam::builder()
                    .try_execution(execution.id.to_string())?
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;
        assert_eq!(execution, response);
        Ok(())
    }
}
