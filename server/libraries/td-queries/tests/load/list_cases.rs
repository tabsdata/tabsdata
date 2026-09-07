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
use td_objects::types::basic::AtTime;
use td_queries::{CurrentEntry, HistoricEntry, LiveEntry};

use crate::harness::{record, seed};

#[td_test::test(db)]
async fn load_live_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<CollectionRead>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = (&db).live().list((), &params).await?;
    record("live::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_current_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = (&db).current().list::<Table>((), &params).await?;
    record("current::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_historic_list_versions(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let params = ListQueryParams::<Function>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = (&db)
        .historic(&at)
        .list_versions::<Function>((), &params)
        .await?;
    record("historic::list_versions", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_historic_list_full(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let params = ListQueryParams::<Function>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = (&db)
        .historic(&at)
        .list_full::<Function>((), &params)
        .await?;
    record("historic::list_full", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}
