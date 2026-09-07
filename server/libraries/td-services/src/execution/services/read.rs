//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::plan::{
    build_execution_plan, build_response, resolve_execution_plan_versions,
};
use crate::graph::{FunctionOrTable, GraphConfig, generate_td_graph};
use std::collections::HashMap;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_execution::EXECUTION_GRAPH_OPTIONS;
use td_execution::TransactionBy;
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::execution::{ExecutionDB, ExecutionResponse};
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunInputInsertDB,
    LogicalFunctionRunInsertDB,
};
use td_objects::dxo::transaction::TransactionValue;
use td_objects::dxo::transaction::{TransactionDB, TransactionInsertDB};
use td_objects::sql_condition::SqlCondition;
use td_objects::transaction::map::TransactionMap;
use td_objects::types::basic::{AllowDisabled, AtTime};
use td_objects::types::recursion::RecursionOptions;
use td_queries::Queries;

#[service_factory]
async fn execution_read(
    authz: Ctx<AuthzContext>,
    transaction_by: Ctx<TransactionBy>,
    db: DbPool<AcrossProjects>,
    request: ReadRequest<(ExecutionParam, AllowDisabled)>,
) -> Result<ExecutionResponse, TdError> {
    let (ctx, (param, allow_disabled)) = request.into_parts();
    let execution_id = param.execution;

    let mut trx = db.read().await?;
    let conn = trx.get(authz.none());
    let execution: ExecutionDB = conn.fetch_one(&execution_id).await?;
    let collection: ResolvedCollectionDB = conn.fetch_one(&execution.base.collection_id).await?;
    let project = collection.base.project_id;
    let mut trx = trx.enter_project(&project).await?;
    let grant = authz
        .require::<Project, ViewProject>(&mut trx, &ctx, &project)
        .await?;
    let conn = trx.get(grant);
    let at = AtTime::try_from(&execution.base.triggered_on)?;

    let function: FunctionDBWithNames = conn
        .timeline()
        .fetch_one(&execution.base.function_version_id)
        .await?;
    let function_id = function.base.base.function_id;
    let entrypoint = FunctionOrTable::from(&function_id);
    let recursion_options = with_existing_recursion_options(&execution)?;
    let function_state: Box<dyn SqlCondition<FunctionDB>> = if *allow_disabled {
        Box::new(FunctionDB::active())
    } else {
        Box::new(FunctionDB::executable())
    };
    // Plan-reading rebuilds the graph as it existed at the plan's
    // `TriggeredOn` — point-in-time SQL via visible-at interval scans.
    let config = GraphConfig {
        historic: true,
        ..GraphConfig::defaults(function_state)
    };
    let graph = generate_td_graph(conn, config, &at, &entrypoint, &recursion_options).await?;

    let execution_plan = build_execution_plan(&graph, &function)?;
    let transactions: Vec<TransactionInsertDB> = conn
        .fetch_all::<TransactionDB>(&execution_id)
        .await?
        .into_iter()
        .map(|transaction| transaction.base)
        .collect();
    let transaction_map = build_existing_transaction_map(&transaction_by, &transactions)?;
    let resolved_plan =
        resolve_execution_plan_versions(conn, &execution.base, &execution_plan).await?;

    // Persisted logical rows — the response renders `input_idx` from
    // them, so reads reproduce the plan-build assignment exactly.
    let lfrs: Vec<LogicalFunctionRunInsertDB> = conn
        .fetch_all::<LogicalFunctionRunDB>(&execution_id)
        .await?
        .into_iter()
        .map(|lfr| lfr.base)
        .collect();
    let inputs: Vec<LogicalFunctionRunInputInsertDB> = conn
        .fetch_all::<LogicalFunctionRunInputDB>(&execution_id)
        .await?
        .into_iter()
        .map(|input| input.base)
        .collect();

    build_response(
        &transaction_by,
        &transaction_map,
        &execution.base,
        &resolved_plan,
        &lfrs,
        &inputs,
    )
    .await
}

pub fn with_existing_recursion_options(
    execution: &ExecutionDB,
) -> Result<RecursionOptions, TdError> {
    // We are using the depth defined, if any, or defaulting to EXECUTION_GRAPH_DEPTH.
    let options = execution
        .base
        .recursion_options
        .as_deref()
        .cloned()
        .unwrap_or(EXECUTION_GRAPH_OPTIONS);
    Ok(options)
}

// Similar to build_transaction_map, but using already existing transactions instead of creating new ones.
pub fn build_existing_transaction_map(
    transaction_by: &TransactionBy,
    transactions: &[TransactionInsertDB],
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut mapped_transactions = HashMap::new();
    for transaction in transactions.iter() {
        if !mapped_transactions.contains_key(&transaction.transaction_key) {
            mapped_transactions.insert(
                transaction.transaction_key.clone(),
                TransactionValue::from(transaction),
            );
        }
    }

    // We should always have at least one transaction, and all transactions should have the same transaction_by.
    // In case it differs with the one configured in the system, we prioritize the one from the transaction.
    // This can happen when loading transactions from previous executions.
    let transaction_by_str = transactions.first().map(|t| t.transaction_by.clone());
    let transaction_by = if let Some(transaction_by_str) = transaction_by_str {
        transaction_by_str.parse().unwrap_or(transaction_by.clone())
    } else {
        transaction_by.clone()
    };
    let transaction_map = TransactionMap::from_map(mapped_transactions, transaction_by);
    Ok(transaction_map)
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

        // `dot` uses petgraph node indices assigned in graph-walk order, so
        // equivalent graphs can label differently; compare the edge set.
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
