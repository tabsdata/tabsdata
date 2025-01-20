//
// Copyright 2025 Tabs Data Inc.
//

use td_execution::execution_planner::ExecutionTemplate;
use td_execution::graphs::DatasetGraph;

/// Creates an `ExecutionTemplate` from a `DatasetGraph`. This is the only way of creating a new
/// `ExecutionPlan`, using `RelativeVersion`. After that, it can be transformed to any other
/// type implementing `VersionRef`. With the `ExecutionTemplate` it is possible to create
/// any concrete `ExecutionPlan` with any `VersionRef` needed.
pub trait FromDatasetGraph {
    fn from_graph(dgraph: DatasetGraph) -> ExecutionTemplate;
}
