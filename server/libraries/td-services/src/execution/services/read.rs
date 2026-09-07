//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::execution::{
    build_execution_plan, build_response, resolve_execution_plan_versions,
};
use crate::execution::read::{build_existing_transaction_map, with_existing_recursion_options};
use crate::graph::{FunctionOrTable, GraphConfig, generate_td_graph};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::ViewProject;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_execution::TransactionBy;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::execution::{ExecutionDB, ExecutionResponse};
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunInputDB};
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{AllowDisabled, AtTime};
use td_queries::LiveEntry;

// Very similar to trigger, but without triggering. It selects entities needed
// to build an execution plan.
#[service_factory]
async fn execution_read(
    authz: Ctx<AuthzContext>,
    transaction_by: Ctx<TransactionBy>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest<(ExecutionParam, AllowDisabled)>,
) -> Result<ExecutionResponse, TdError> {
    let (ctx, (param, allow_disabled)) = request.into_parts();
    let execution_id = param.execution;

    let conn = gated_conn.get(authz.none());
    let execution: ExecutionDB = conn.live().fetch_one(&execution_id).await?;
    let collection: CollectionDB = conn.live().fetch_one(&execution.collection_id).await?;
    let _grant = authz
        .require_in_project::<ViewProject>(conn, &ctx, &collection.base.project_id)
        .await?;
    let at = AtTime::try_from(&execution.triggered_on)?;

    let function: FunctionDBWithNames = conn
        .live()
        .fetch_one(&execution.function_version_id)
        .await?;
    let function_id = function.base.function_id;
    let entrypoint = FunctionOrTable::from(&function_id);
    let recursion_options = with_existing_recursion_options(&execution).await?;
    let function_state = if *allow_disabled {
        FunctionDB::active()
    } else {
        FunctionDB::executable()
    };
    // Plan-reading rebuilds the graph as it existed at the plan's
    // `TriggeredOn` — point-in-time SQL via ranked CTEs.
    let config = GraphConfig {
        historic: true,
        ..GraphConfig::defaults(function_state)
    };
    let graph = generate_td_graph(conn, config, &at, &entrypoint, &recursion_options).await?;

    let execution_plan = build_execution_plan(&graph, &function).await?;
    let transactions: Vec<TransactionDB> = conn.live().fetch_all(&execution_id).await?;
    let transaction_map = build_existing_transaction_map(&transaction_by, &transactions)?;
    let resolved_plan = resolve_execution_plan_versions(conn, &execution, &execution_plan).await?;

    // Persisted logical rows — the response renders `input_idx` from
    // them, so reads reproduce the plan-build assignment exactly.
    let lfrs: Vec<LogicalFunctionRunDB> = conn.live().fetch_all(&execution_id).await?;
    let inputs: Vec<LogicalFunctionRunInputDB> = conn.live().fetch_all(&execution_id).await?;

    build_response(
        &transaction_by,
        &transaction_map,
        &execution,
        &resolved_plan,
        &lfrs,
        &inputs,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::execution::services::execute::tests::test_execute;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_execution(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let execution = test_execute(db.clone(), p.id, false, false, false).await?;

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read((
            ExecutionParam {
                execution: execution.id.clone(),
            },
            AllowDisabled::FALSE,
        ));

        let response = service.execution_read(request).await?.data;
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
