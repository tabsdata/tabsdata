//
// Copyright 2026 Tabsdata Inc.
//

//! `list` surfaces — filtered/paginated reads mapped through a `ListDto`,
//! with no filter and default pagination, over the chain dataset.

use std::time::Instant;

use ta_objects::Typed;
use td_database::connection::PgConnectionExt;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::CollectionRead;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::function::Function;
use td_objects::dxo::logical::FunctionRun;
use td_objects::dxo::table::Table;
use td_objects::dxo::tag::TagFacetParams;
use td_objects::types::basic::TagKey;

use td_queries::{Queries, Unversioned, list_plan_sql};

use crate::harness::{analyze, record, seed};
use crate::profile::SizeProfile;
use td_objects::dxo::topology::ProjectTopologyDB;

#[td_test::test(db)]
async fn load_default_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListParams::default();
    let t = Instant::now();
    let rows = db.scd2().list::<CollectionRead>((), params).await?.data;
    record("default::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_current_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListParams::default();
    let t = Instant::now();
    let rows = db.scd2().list::<Table>((), params).await?.data;
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
    let params = ListParams::default();
    let t = Instant::now();
    let rows = db
        .scd2()
        .as_of(&version.interval.valid_from)
        .list::<Function>((), params)
        .await?
        .data;
    record("as_of::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_last_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListParams::default();
    let t = Instant::now();
    let rows = db.scd2().last().list::<Table>((), params).await?.data;
    record("last::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_tagged_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let team_a = team_a("table");
    let t = Instant::now();
    let rows = db.scd2().list::<Table>((), team_a).await?.data;
    record("tagged::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_tagged_collections_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let team_a = team_a("collection");
    let t = Instant::now();
    let rows = db.scd2().list::<CollectionRead>((), team_a).await?.data;
    record(
        "latest::tagged::collections",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_tag_keys(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let rows = db.tags().keys(&TagFacetParams::default()).await?;
    record("tags::keys", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_tag_values(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let team = TagKey::try_new("team")?;
    let t = Instant::now();
    let rows = db.tags().values(&team, &TagFacetParams::default()).await?;
    record("tags::values", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

fn team_a(entity: &str) -> ListParams {
    ListParams {
        filter: vec![format!("{entity}.team:eq:a")],
        ..ListParams::default()
    }
}

/// Runs listed by their function's tags at three selectivities. The tags
/// side must be index-served: a filter never scans the tags table.
#[td_test::test(db)]
async fn load_function_runs_by_function_tag(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    analyze(&db, "runs by function tag").await;
    for (case, filter) in [
        ("tagged::runs::common", "function.team:eq:a"),
        ("tagged::runs::rare", "function.team:eq:nobody"),
        ("tagged::runs::untagged", "function.team:is:null"),
    ] {
        let params = ListParams {
            filter: vec![filter.to_string()],
            ..ListParams::default()
        };
        let t = Instant::now();
        let rows = db.list::<FunctionRun>((), params.clone()).await?.data;
        record(case, profile, t, rows.len(), seeded);
        let mut conn = db.begin_ro().await.expect("read-only connection");
        let plan = conn
            .explain(&list_plan_sql::<FunctionRun, _>(&Unversioned, params)?)
            .await
            .expect("explain")
            .join("\n");
        eprintln!("[query-load] {case} plan:\n{plan}\n");
        if profile != SizeProfile::Smoke {
            assert!(
                !plan.contains("Seq Scan on tags"),
                "{case} scans the tags:\n{plan}"
            );
        }
    }
    Ok(())
}
