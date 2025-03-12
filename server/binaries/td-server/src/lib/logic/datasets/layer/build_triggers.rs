//
// Copyright 2024 Tabs Data Inc.
//

use td_common::id;
use td_error::TdError;
use td_objects::datasets::dao::*;
use td_objects::datasets::dlo::*;
use td_objects::dlo::{CollectionId, DatasetId, FunctionId};
use td_tower::extractors::Input;

pub async fn build_triggers(
    Input(triggers): Input<FunctionTriggers>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_id): Input<DatasetId>,
    Input(function_id): Input<FunctionId>,
) -> Result<Vec<DsTrigger>, TdError> {
    let mut dataset_triggers = vec![];

    let trigger_deps: Vec<_> = triggers
        .triggers()
        .iter()
        .map(|uri| uri.with_ids())
        .map(|uri| {
            DsTrigger::builder()
                .id(id::id())
                .collection_id(&*collection_id)
                .dataset_id(&*dataset_id)
                .function_id(&*function_id)
                .trigger_collection_id(uri.collection())
                .trigger_dataset_id(uri.dataset())
                .build()
                .unwrap()
        })
        .collect();
    dataset_triggers.extend(trigger_deps);

    Ok(dataset_triggers)
}
