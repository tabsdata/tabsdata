//
// Copyright 2024 Tabs Data Inc.
//

use td_common::id;
use td_common::system_tables::INITIAL_VALUES;
use td_common::uri::Version::Head;
use td_common::uri::Versions;
use td_error::TdError;
use td_objects::datasets::dao::*;
use td_objects::datasets::dlo::*;
use td_objects::dlo::{CollectionId, DatasetId, FunctionId};
use td_tower::extractors::Input;

pub async fn build_dependencies(
    Input(deps): Input<FunctionDependencies>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_id): Input<DatasetId>,
    Input(function_id): Input<FunctionId>,
) -> Result<Vec<DsDependency>, TdError> {
    let mut dataset_deps = vec![];

    // Implicit dependency on previous initial values
    let initial_values = DsDependency::builder()
        .id(id::id())
        .collection_id(&*collection_id)
        .dataset_id(&*dataset_id)
        .function_id(&*function_id)
        .table_collection_id(&*collection_id)
        .table_dataset_id(&*dataset_id)
        .table_name(INITIAL_VALUES)
        .table_versions(Versions::Single(Head(0)).to_string())
        .pos(-1)
        .build()
        .unwrap();
    dataset_deps.push(initial_values);

    let data_deps: Vec<_> = deps
        .all()
        .iter()
        .map(|uri| uri.with_ids())
        .enumerate()
        .map(|(index, uri)| {
            DsDependency::builder()
                .id(id::id())
                .collection_id(&*collection_id)
                .dataset_id(&*dataset_id)
                .function_id(&*function_id)
                .table_collection_id(uri.collection())
                .table_dataset_id(uri.dataset())
                .table_name(uri.table().unwrap())
                .table_versions(uri.versions().to_string())
                .pos(index as i64)
                .build()
                .unwrap()
        })
        .collect();
    dataset_deps.extend(data_deps);

    Ok(dataset_deps)
}
