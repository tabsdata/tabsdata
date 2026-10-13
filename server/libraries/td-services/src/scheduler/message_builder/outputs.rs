//
// Copyright 2026 Tabsdata Inc.
//

use super::{build_external_location, partition_and_sort};
use std::collections::HashMap;
use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunToExecute;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
use td_objects::dxo::worker_input::function::{OutputTable, OutputTableVersion};
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
            .builder(&table.base.base.base.base_path)
            .collection(&table.base.base.base.collection_id)
            .data(&table.base.base.base.id)
            .table(
                &table.base.base.base.table_id,
                &table.base.base.base.table_version_id,
            )
            .build()?;
        let (location, _) = build_external_location(storage, &path)?;

        let output_table = OutputTableVersion {
            name: table.base.base.base.name.clone(),
            collection_id: table.base.base.base.collection_id,
            collection: table.collection.clone(),
            table_id: table.base.base.base.table_id,
            table_version_id: table.base.base.base.table_version_id,
            table_data_version_id: table.base.base.base.id.clone(),
            location,
            table_pos: table.base.base.base.function_param_pos,
        };

        output_tables_map.insert(*table.base.base.base.function_param_pos, output_table);
    }
    Ok(partition_and_sort(output_tables_map, OutputTable::Table))
}
