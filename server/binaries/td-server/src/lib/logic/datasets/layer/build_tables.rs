//
// Copyright 2024 Tabs Data Inc.
//

use td_common::id;
use td_common::system_tables::INITIAL_VALUES;
use td_error::TdError;
use td_objects::datasets::dao::*;
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionId, DatasetId, FunctionId};
use td_tower::extractors::Input;

pub async fn build_tables(
    Input(dataset_write): Input<DatasetWrite>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_id): Input<DatasetId>,
    Input(function_id): Input<FunctionId>,
) -> Result<Vec<DsTable>, TdError> {
    let mut tables = vec![];

    let initial_values = DsTable::builder()
        .id(id::id())
        .name(INITIAL_VALUES)
        .collection_id(&*collection_id)
        .dataset_id(&*dataset_id)
        .function_id(&*function_id)
        .pos(-1)
        .build()
        .unwrap();
    tables.push(initial_values);

    let data_tables: Vec<_> = dataset_write
        .tables()
        .iter()
        .enumerate()
        .map(|(index, table)| {
            DsTable::builder()
                .id(id::id())
                .name(table)
                .collection_id(&*collection_id)
                .dataset_id(&*dataset_id)
                .function_id(&*function_id)
                .pos(index as i64)
                .build()
                .unwrap()
        })
        .collect();
    tables.extend(data_tables);

    Ok(tables)
}
