//
//  Copyright 2024 Tabs Data Inc.
//

use std::ops::Deref;
use td_common::dataset::DatasetRef;
use td_common::error::TdError;
use td_common::uri::TdUri;
use td_execution::execution_planner::ExecutionTemplate;
use td_execution::graphs::DatasetGraph;
use td_objects::datasets::dto::ExecutionTemplateRead;
use td_objects::dlo::{CollectionName, DatasetName};
use td_tower::extractors::Input;

pub async fn execution_template_to_api(
    Input(collection): Input<CollectionName>,
    Input(dataset): Input<DatasetName>,
    Input(execution_template): Input<ExecutionTemplate>,
) -> Result<ExecutionTemplateRead, TdError> {
    let graph = DatasetGraph::from_execution_planner(execution_template.deref())?;
    let dot = format!("{:?}", graph.template_dot());

    let triggered: Vec<_> = execution_template
        .triggers()
        .iter()
        .map(|dataset| {
            TdUri::new(dataset.collection(), dataset.dataset(), None, None)
                .unwrap()
                .to_string()
        })
        .collect();

    let response = ExecutionTemplateRead::builder()
        .collection_name(collection.as_str())
        .dataset_name(dataset.as_str())
        .triggered_datasets(triggered)
        .dot(dot)
        .build()
        .unwrap();
    Ok(response)
}
