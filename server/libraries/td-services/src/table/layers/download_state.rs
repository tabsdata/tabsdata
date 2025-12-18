//
// Copyright 2025 Tabs Data Inc.
//

use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::rest_urls::params::TableAtIdName;
use td_objects::system::TD_FN_STATE_PREFIX;
use td_objects::types::basic::{AtTime, CollectionIdName, IdOrName, TableIdName};
use td_tower::extractors::Input;

pub async fn get_table_at_id_name(
    Input(at_time): Input<AtTime>,
    Input(function): Input<FunctionDBWithNames>,
    Input(system_tables): Input<Vec<TableDBWithNames>>,
) -> Result<TableAtIdName, TdError> {
    let state_tables = system_tables
        .iter()
        .filter(|t| t.name.starts_with(TD_FN_STATE_PREFIX))
        .collect::<Vec<_>>();
    let state_table = match state_tables.len() {
        1 => state_tables[0],
        len => Err(api_error!(
            ApiError::InternalError,
            "Found '{len}' {TD_FN_STATE_PREFIX} tables, expected 1",
        ))?,
    };
    let table_at_id = TableAtIdName {
        collection: CollectionIdName::from_id(function.collection_id),
        table: TableIdName::from_id(state_table.table_id),
        at: *at_time,
    };
    Ok(table_at_id)
}
