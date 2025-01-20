//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_execution::dataset::Dataset;
use td_execution::execution_planner::ExecutionTemplate;
use td_execution::graphs::DatasetGraphBuilder;
use td_execution::link::{DataGraph, TriggerGraph};
use td_interceptor::execution::from_graph::ExecutionTemplateBuilder;
use td_interceptor_api::execution::from_graph::FromDatasetGraph;
use td_objects::dlo::{CollectionId, DatasetId};
use td_tower::extractors::{Context, Input};
use td_transaction::TransactionBy;

pub async fn generate_execution_template(
    Context(transaction_by): Context<TransactionBy>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_id): Input<DatasetId>,
    Input(data_graph): Input<DataGraph>,
    Input(trigger_graph): Input<TriggerGraph>,
) -> Result<ExecutionTemplate, TdError> {
    let trigger_dataset = Dataset::new(&collection_id, &dataset_id);
    let dataset_graph =
        DatasetGraphBuilder::new(&data_graph, &trigger_graph).build(trigger_dataset)?;
    dataset_graph.validate_dag()?;
    dataset_graph.validate_transaction(&transaction_by)?;
    let execution_template = ExecutionTemplateBuilder::from_graph(dataset_graph);
    Ok(execution_template)
}
