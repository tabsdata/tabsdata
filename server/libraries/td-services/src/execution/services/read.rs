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
use td_objects::StateFilter;
use td_objects::States;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::execution::{ExecutionDB, ExecutionResponse};
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::transaction::TransactionDB;
use td_objects::rest_urls::ExecutionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, ExecutionId, ExecutionIdName, FunctionId, FunctionVersionId, TriggeredOn,
};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

pub struct GraphOpts<const FUNCTION_STATE: u8>;
impl<const FUNCTION_STATE: u8> GraphConfig for GraphOpts<FUNCTION_STATE>
where
    FunctionDB: States<FUNCTION_STATE>,
{
    type Authz = SKIP_AUTHZ;

    fn function_state() -> StateFilter<FunctionDB> {
        StateFilter::<FunctionDB>::from_states::<FUNCTION_STATE>()
    }

    fn include_dependency_functions() -> bool {
        false
    }

    /// Plan-reading rebuilds the graph as it existed at the plan's
    /// `TriggeredOn` — point-in-time SQL via ranked CTEs.
    fn historic() -> bool {
        true
    }
}

// Very similar to trigger, but without triggering. It selects entities needed to build an execution plan.
#[service_factory(
    name = ExecutionReadService,
    request = ReadRequest<ExecutionParam>,
    response = ExecutionResponse,
    connection = ConnectionProvider,
    context = TransactionBy,
)]
fn service<const FUNCTION_STATE: u8>()
where
    FunctionDB: States<FUNCTION_STATE>,
{
    layers!(
        // Extract from request.
        from_fn(With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
        // TODO: when ExecutionParam carries an AtParam, thread it as the
        // pointer-read AtTime so logical bindings render at request time.
        // Find Plan and its TriggeredOn (and convert to AtTime).
        from_fn(With::<ExecutionParam>::extract::<ExecutionIdName>),
        from_fn(By::<ExecutionIdName>::fetch_one::<ExecutionDB>),
        from_fn(With::<ExecutionDB>::extract::<TriggeredOn>),
        from_fn(With::<TriggeredOn>::try_convert_to::<AtTime, _>),
        // Find function that triggered the plan.
        from_fn(With::<ExecutionDB>::extract::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
        // Get recursion options
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionId>::convert_to::<FunctionOrTable>),
        from_fn(with_existing_recursion_options),
        // Find td graph
        generate_td_graph(GraphOpts::<FUNCTION_STATE>),
        // Build execution plan
        from_fn(build_execution_plan),
        // Get transactions
        from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
        from_fn(By::<ExecutionId>::fetch_all::<TransactionDB>),
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
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_read_execution(db: TdPool) {
        use crate::graph::layers::build::GraphBuilder;
        use td_tower::metadata::type_of_val;

        ExecutionReadService::<{ FunctionDB::Executable }>::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<ExecutionParam>, ExecutionResponse>(&[
                // Extract from request.
                type_of_val(&With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
                // Find Plan and its TriggeredOn (and convert to AtTime).
                type_of_val(&With::<ExecutionParam>::extract::<ExecutionIdName>),
                type_of_val(&By::<ExecutionIdName>::fetch_one::<ExecutionDB>),
                type_of_val(&With::<ExecutionDB>::extract::<TriggeredOn>),
                type_of_val(&With::<TriggeredOn>::try_convert_to::<AtTime, _>),
                // Find function that triggered the plan.
                type_of_val(&With::<ExecutionDB>::extract::<FunctionVersionId>),
                type_of_val(&By::<FunctionVersionId>::fetch_one::<FunctionDBWithNames>),
                // Get recursion options
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                type_of_val(&With::<FunctionId>::convert_to::<FunctionOrTable>),
                type_of_val(&with_existing_recursion_options),
                // Find td graph
                // Find triggers
                type_of_val(&GraphBuilder::<GraphOpts<{ FunctionDB::Executable }>>::find_recursive_trigger_graph),
                // Find dependencies. For clarity, we always show the function, even if
                // generating a table centered graph, the same way we show the tables of
                // a function centered graph.
                type_of_val(&GraphBuilder::<GraphOpts<{ FunctionDB::Executable }>>::main_function_id),
                type_of_val(&GraphBuilder::<GraphOpts<{ FunctionDB::Executable }>>::unique_triggered_function_ids),
                type_of_val(
                    &GraphBuilder::<GraphOpts<{ FunctionDB::Executable }>>::find_triggered_functions_dependency_graph,
                ),
                // Find output tables
                type_of_val(&GraphBuilder::<GraphOpts<{ FunctionDB::Executable }>>::find_functions_output_tables),
                // Find all tables and functions
                type_of_val(&GraphBuilder::<GraphOpts<{ FunctionDB::Executable }>>::find_all_tables_map),
                type_of_val(&GraphBuilder::<GraphOpts<{ FunctionDB::Executable }>>::find_all_functions_map),
                // Create td graph and validate it
                type_of_val(&GraphBuilder::<GraphOpts<{ FunctionDB::Executable }>>::build_td_graph),
                // Build execution plan
                type_of_val(&build_execution_plan),
                // Get transactions
                type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
                type_of_val(&By::<ExecutionId>::fetch_all::<TransactionDB>),
                type_of_val(&build_existing_transaction_map),
                // Create execution plan
                type_of_val(&resolve_execution_plan_versions),
                // Execution plan response
                type_of_val(&build_response),
            ]);
    }

    #[td_test::test(db)]
    async fn test_read_execution(db: TdPool) -> Result<(), TdError> {
        let execution = test_execute(db.clone(), false, false, true, false).await?;

        let service =
            ExecutionReadService::<{ FunctionDB::Executable }>::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                ExecutionParam::builder()
                    .execution(ExecutionIdName::from_str(&execution.id.to_string())?)
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;
        assert_eq!(execution.id, response.id);
        assert_eq!(execution.name, response.name);
        assert_eq!(execution.triggered_on, response.triggered_on);
        assert_eq!(execution.trigger_type, response.trigger_type);
        assert_eq!(execution.manual_trigger, response.manual_trigger);
        assert_eq!(execution.all_functions, response.all_functions);
        assert_eq!(execution.triggered_functions, response.triggered_functions);
        assert_eq!(execution.transactions, response.transactions);
        assert_eq!(execution.all_tables, response.all_tables);
        assert_eq!(execution.created_tables, response.created_tables);
        assert_eq!(execution.system_tables, response.system_tables);
        assert_eq!(execution.user_tables, response.user_tables);
        assert_eq!(execution.relations_info, response.relations_info);

        // `relations` is order-sensitive but ordered by graph walk, which
        // varies with HashMap iteration; compare as a multiset.
        use std::collections::HashSet;
        let lhs: HashSet<_> = execution.relations.iter().collect();
        let rhs: HashSet<_> = response.relations.iter().collect();
        assert_eq!(lhs, rhs);

        // `dot` uses petgraph node indices assigned in graph-walk order;
        // two equivalent graphs (in-memory build vs DB re-read) can produce
        // different index labelings of the same edges. Compare the
        // semantic edge set after resolving each index back to its label.
        assert_eq!(
            dot_edge_set(execution.dot.as_str()),
            dot_edge_set(response.dot.as_str()),
        );

        Ok(())
    }

    /// Parse a petgraph-emitted dot string into the set of labeled edges:
    /// `{(source_label, target_label, attrs)}`. Two dots describing the
    /// same graph compare equal regardless of node-index ordering.
    fn dot_edge_set(dot: &str) -> std::collections::HashSet<(String, String, String)> {
        let mut labels: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut edges = std::collections::HashSet::new();

        for raw in dot.lines() {
            let line = raw.trim();
            if line.is_empty() || line == "digraph {" || line == "}" {
                continue;
            }
            // Node line: `N [ label = "X" ... ]`
            if let Some((idx, rest)) = line.split_once(" [")
                && !rest.contains("->")
                && let Some(label_start) = rest.find("label = \"")
                && let Some(label_end) = rest[label_start + 9..].find('"')
            {
                let label = &rest[label_start + 9..label_start + 9 + label_end];
                labels.insert(idx.trim().to_string(), label.to_string());
                continue;
            }
            // Edge line: `N -> M [ attrs ]`
            if let Some((endpoints, attrs)) = line.split_once(" [")
                && let Some((src, tgt)) = endpoints.split_once(" -> ")
            {
                let src_l = labels
                    .get(src.trim())
                    .cloned()
                    .unwrap_or_else(|| src.trim().to_string());
                let tgt_l = labels
                    .get(tgt.trim())
                    .cloned()
                    .unwrap_or_else(|| tgt.trim().to_string());
                edges.insert((src_l, tgt_l, attrs.trim_end_matches(']').trim().to_string()));
            }
        }
        edges
    }
}
