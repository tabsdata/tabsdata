//
// Copyright 2026 Tabsdata Inc.
//

//! A read-only unit of work sees one snapshot for all its statements.

use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::show_setting;

/// A function read assembled from several statements (its version, then its
/// tables, dependencies and triggers), or a chunked read over more than 100
/// ids, must not mix two versions of the catalog when a commit lands between
/// two of them: an RO unit reads at one snapshot, whether it spans projects
/// or is scoped to one.
#[td_test::test(db)]
async fn test_read_only_units_read_at_one_snapshot(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);

    let mut across = pools.read().await?;
    let across_isolation = show_setting(
        across.get(Grant::asserted("isolation probe")),
        "transaction_isolation",
    )
    .await
    .expect("isolation probe");

    let mut scoped = DbPool::in_project(&pools, &p.id).read().await?;
    let scoped_isolation = show_setting(
        scoped.get(Grant::asserted("isolation probe")),
        "transaction_isolation",
    )
    .await
    .expect("isolation probe");

    for (unit, isolation) in [("across", across_isolation), ("scoped", scoped_isolation)] {
        assert!(
            isolation == "repeatable read" || isolation == "serializable",
            "an RO unit ({unit}) reads at per-statement snapshots: {isolation}"
        );
    }
    Ok(())
}
