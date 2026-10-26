//
// Copyright 2026 Tabsdata Inc.
//

//! `list` surfaces — filtered/paginated reads mapped through a `ListDto`,
//! with no filter and default pagination, over the chain dataset.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::CollectionRead;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::function::Function;
use td_objects::dxo::table::Table;
use td_objects::list::params::ListQueryParams;
use td_queries::Queries;

use crate::harness::{record, seed};
use td_objects::dxo::topology::ProjectTopologyDB;

#[td_test::test(db)]
async fn load_default_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<CollectionRead>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db.scd2().list((), &params).await?;
    record("default::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_current_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db.scd2().list::<Table>((), &params).await?;
    record("latest::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_find_at_list_full(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let version = db
        .scd2()
        .fetch_one::<ProjectTopologyDB>(&h.project_id)
        .await?;
    let params = ListQueryParams::<Function>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db
        .scd2()
        .as_of(&version.interval.valid_from)
        .list::<Function>((), &params)
        .await?;
    record("as_of::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_last_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db.scd2().last().list::<Table>((), &params).await?;
    record("last::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}
