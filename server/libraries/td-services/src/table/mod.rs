//
// Copyright 2025 Tabsdata Inc.
//

pub mod services;

use crate::table::storage::resolve_table_location;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::types::basic::{
    AtTime, CollectionIdName, TableIdName, TriggeredOn, UncommittedStatus,
};
use td_queries::HistoricEntry;
use td_storage::SPath;

pub mod columns;
pub mod download;
pub mod download_state;
pub mod flow;
pub mod sample;
pub mod schema;
pub mod storage;

/// Resolve a `(collection, table, at, uncommitted)`-shaped parameter to the
/// table row and the storage location of its latest data version at that
/// time. Only committed transactions are considered unless the parameter
/// allows uncommitted versions; versions without data changes resolve to the
/// previous version's data.
pub async fn find_data_version_location_at(
    conn: &mut DomainConnection<Apiserver>,
    collection: &CollectionIdName,
    table: &TableIdName,
    at: &AtTime,
    uncommitted: &UncommittedStatus,
) -> Result<(TableDBWithNames, Option<SPath>), TdError> {
    let table_row: TableDBWithNames = conn
        .historic(at)
        .state(TableDBWithNames::available())
        .fetch_one(and!(collection, table))
        .await?;
    let table_id = table_row.base.table_id;
    let triggered_on = TriggeredOn::try_from(at)?;
    let location = resolve_table_location(conn, &triggered_on, &table_id, uncommitted).await?;
    Ok((table_row, location))
}
