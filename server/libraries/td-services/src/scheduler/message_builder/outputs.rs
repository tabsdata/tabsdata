//
// Copyright 2026 Tabsdata Inc.
//

use super::{build_external_location, partition_and_sort};
use std::collections::HashMap;
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunToExecute;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
use td_objects::dxo::yaml::function::{OutputTable, OutputTableVersion};
use td_storage::Storage;
use td_storage::location::StorageLocation;

/// Build the output tables of a function-runner message from the FR's
/// minted output PTDVs, partitioned into (system, user) by param position.
pub(crate) fn build_output_tables(
    storage: &Storage,
    // Output table paths use each PTDV's own stamped base_path, so the FR itself
    // is no longer needed here.
    _f: &PhysicalFunctionRunToExecute,
    tables: &[PhysicalTableDataVersionDBWithNames],
) -> Result<(Vec<OutputTable>, Vec<OutputTable>), TdError> {
    let mut output_tables_map = HashMap::new();
    for table in tables.iter() {
        let (path, _) = StorageLocation::current()
            .builder(&table.base_path)
            .collection(&table.collection_id)
            .data(&table.id)
            .table(&table.table_id, &table.table_version_id)
            .build()?;
        let (location, _) = build_external_location(storage, &path)?;

        let output_table = OutputTableVersion::builder()
            .name(table.name.clone())
            .collection_id(table.collection_id)
            .collection(table.collection.clone())
            .table_id(table.table_id)
            .table_version_id(table.table_version_id)
            .table_data_version_id(&table.id)
            .location(location)
            .table_pos(table.function_param_pos)
            .build()?;

        output_tables_map.insert(*table.function_param_pos, output_table);
    }
    Ok(partition_and_sort(output_tables_map, OutputTable::Table))
}
