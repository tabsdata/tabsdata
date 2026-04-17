//
// Copyright 2025 Tabs Data Inc.
//

use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::rest_urls::params::TableAtIdName;
use td_objects::system::TD_FN_STATE_PREFIX;
use td_objects::types::basic::{AtTime, CollectionIdName, TableIdName, UncommittedStatus};
use td_tower::extractors::Ref;

pub async fn get_table_at_id_name(
    at_time: Ref<AtTime>,
    function: Ref<FunctionDBWithNames>,
    system_tables: Ref<Vec<TableDBWithNames>>,
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
        collection: CollectionIdName::Id(function.collection_id),
        table: TableIdName::Id(state_table.table_id),
        at: *at_time,
        // Internal path: allow uncommitted versions for now.
        uncommitted: UncommittedStatus::TRUE,
    };
    Ok(table_at_id)
}
