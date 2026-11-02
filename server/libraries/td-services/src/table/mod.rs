//
// Copyright 2025 Tabsdata Inc.
//

pub mod services;

use crate::collection::resolve_collection_id;
use crate::table::storage::{TableLocation, resolve_table_location};
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::types::basic::{AtTime, CollectionIdName, TableIdName, UncommittedStatus};
use td_queries::Queries;

pub mod columns;
pub mod download;
pub mod flow;
pub mod sample;
pub mod storage;

/// Resolve a `(collection, table, at, uncommitted)`-shaped parameter to the
/// table row and the storage location of its latest data version at that
/// time. Only committed transactions are considered unless the parameter
/// allows uncommitted versions; versions without data changes resolve to the
/// previous version's data.
pub async fn find_data_version_location_at<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    collection: &CollectionIdName,
    table: &TableIdName,
    at: Option<&AtTime>,
    uncommitted: &UncommittedStatus,
) -> Result<(TableDBWithNames, Option<TableLocation>), TdError> {
    let collection_id = resolve_collection_id(conn, collection).await?;
    let condition = and!(&collection_id, table);
    let table_row: TableDBWithNames = match at {
        Some(at) => conn.scd2().as_of(at).fetch_one(condition).await?,
        None => conn.scd2().fetch_one(condition).await?,
    };
    let table_id = table_row.table_id;
    let location = resolve_table_location(conn, at, &table_id, uncommitted).await?;
    Ok((table_row, location))
}
