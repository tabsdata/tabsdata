//
// Copyright 2025 Tabs Data Inc.
//

use td_error::TdError;
use td_execution::execution_planner::ExecutionPlanWithNames;
use td_execution::graphs::{DatasetGraph, ExecutionGraphWithNames};
use td_tower::extractors::{Input, SrvCtx};
use td_transaction::TransactionBy;

pub async fn execution_graph_with_names(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(execution_plan): Input<ExecutionPlanWithNames>,
) -> Result<ExecutionGraphWithNames, TdError> {
    let graph = DatasetGraph::executable_with_names(&execution_plan, &transaction_by)?;
    Ok(graph)
}
