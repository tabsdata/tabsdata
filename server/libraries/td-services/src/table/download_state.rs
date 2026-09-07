//
// Copyright 2025 Tabsdata Inc.
//

use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::AtTimeParam;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::{TableDBWithNames, TableParam, UncommittedParam};
use td_objects::system::TD_FN_STATE_PREFIX;
use td_objects::types::basic::{AtTime, CollectionIdName, TableIdName, UncommittedStatus};

pub async fn get_table_at_id_name(
    at_time: &AtTime,
    function: &FunctionDBWithNames,
    system_tables: &[TableDBWithNames],
) -> Result<(CollectionParam, TableParam, AtTimeParam, UncommittedParam), TdError> {
    let state_tables = system_tables
        .iter()
        .filter(|t| t.base.name.starts_with(TD_FN_STATE_PREFIX))
        .collect::<Vec<_>>();
    let state_table = match state_tables.len() {
        1 => state_tables[0],
        len => Err(api_error!(
            ApiError::InternalError,
            "Found '{len}' {TD_FN_STATE_PREFIX} tables, expected 1",
        ))?,
    };
    Ok((
        CollectionParam {
            collection: CollectionIdName::Id(function.base.collection_id),
        },
        TableParam {
            table: TableIdName::Id(state_table.base.table_id),
        },
        AtTimeParam { at: *at_time },
        // Internal path: allow uncommitted versions for now.
        UncommittedParam {
            uncommitted: UncommittedStatus::TRUE,
        },
    ))
}
