//
// Copyright 2026 Tabsdata Inc.
//

//! Runs `SELECT <columns> FROM <table> LIMIT 0` for every registered DAO, so
//! a projection the schema no longer matches fails here instead of at first
//! use. `LIMIT 0` validates column existence only: no row is decoded.

use sea_query::{Alias, Query};
use td_database::pool::TdPool;
use td_database::sea_query::builder::SeaQueryExt;
use td_error::TdError;
use td_objects::registered_daos;

/// Floor against registration silently breaking and leaving this test
/// vacuously green; 56 registered at the time of writing.
const MIN_EXPECTED_DAOS: usize = 45;

#[td_test::test(db)]
async fn every_dao_projection_resolves_against_schema(db: TdPool) -> Result<(), TdError> {
    let daos: Vec<_> = registered_daos().collect();
    assert!(
        daos.len() >= MIN_EXPECTED_DAOS,
        "only {} DAOs registered (expected >= {MIN_EXPECTED_DAOS}); DAO registration may be broken",
        daos.len(),
    );

    let mut failures = Vec::new();
    for dao in daos {
        let proj = (dao.describe)();
        let mut query = Query::select();
        for col in &proj.columns {
            query.column(Alias::new(col));
        }
        query.from(Alias::new(&proj.table)).limit(0);

        if let Err(e) = query.build_query().fetch_all(&db).await {
            failures.push(format!("`{}` (relation `{}`): {e}", proj.name, proj.table));
        }
    }
    assert!(
        failures.is_empty(),
        "DAO projections do not resolve against the schema:\n  {}",
        failures.join("\n  ")
    );

    let mut bogus = Query::select();
    bogus
        .column(Alias::new("definitely_not_a_column"))
        .from(Alias::new("workers"))
        .limit(0);
    assert!(
        bogus.build_query().fetch_all(&db).await.is_err(),
        "schema consistency check has no teeth: an unknown column did not error",
    );

    Ok(())
}
