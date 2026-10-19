//
// Copyright 2026 Tabsdata Inc.
//

//! v13 → v14 keys every fact by an id minted at its `ts`, through the
//! delta's `td_mint_id`, which mints the application's `Id`.

use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use crate::components::ApiserverComponent;
use crate::generated_sql::generated;
use crate::test_utils::postgres::pool;
use td_common::id::{Id, id_time};
use td_schema::schema::archive;
use td_schema::schema::deltas::mint_id_function;

use super::{execute, scalars};

fn fact(ts: &str) -> String {
    format!(
        "INSERT INTO metrics_facts (ts, ts_hour_ms, ts_day_ms, ts_week_ms, ts_month_ms, project_id, \
           group_id, collection_id, execution_id, transaction_id, function_id, function_run_id, \
           attempt_id, attempt_outcome, run_status, replay, current, run_has_inputs, \
           run_has_outputs, side, private, has_data, row_count, data_bytes, edge_first, \
           edge_first_group) \
         VALUES ('{ts}', 0, 0, 0, 0, 'p0', 'g0', 'c0', 10, 20, 'f0', 30, 40, 'D', 'D', 0, true, \
           false, false, 'A', false, false, 0, 0, false, false)"
    )
}

fn parse(id: &str) -> Id {
    Id::try_from(id).unwrap_or_else(|e| panic!("{id} is not an Id: {e}"))
}

fn at_ms(ms: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(ms)
}

#[tokio::test]
async fn v13_to_v14_keys_facts_at_their_instant_postgres() {
    let db = pool().await;
    db.admin
        .upgrade_source(archive::source(13))
        .await
        .expect("installing archive v13");
    execute(&db.serving, fact("2026-01-01 08:00:00.123+00")).await;
    execute(&db.serving, fact("2026-01-01 08:00:00.123+00")).await;
    execute(&db.serving, fact("2026-01-01 09:00:00+00")).await;

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v13 -> current");

    let ids = scalars(&db.serving, "SELECT id FROM metrics_facts ORDER BY ts, id").await;
    assert_eq!(ids.len(), 3);
    assert!(
        scalars(
            &db.serving,
            "SELECT proname FROM pg_proc WHERE proname = 'td_mint_id'"
        )
        .await
        .is_empty(),
        "the delta drops td_mint_id"
    );
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), 3, "{ids:?}");
    let times: Vec<SystemTime> = ids.iter().map(|id| id_time(&parse(id))).collect();
    assert_eq!(
        times,
        [
            at_ms(1_767_254_400_123),
            at_ms(1_767_254_400_123),
            at_ms(1_767_258_000_000)
        ]
    );
}

#[tokio::test]
async fn td_mint_id_mints_application_ids_postgres() {
    let db = pool().await;
    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("installing current");
    sqlx::query(generated(mint_id_function()))
        .execute(&db.admin.pool)
        .await
        .expect("creating td_mint_id");

    let early = scalars(
        &db.serving,
        "SELECT td_mint_id('2026-01-01 08:00:00.123+00') FROM generate_series(1, 1000)",
    )
    .await;
    assert_eq!(early.iter().collect::<HashSet<_>>().len(), 1000);
    for minted in &early {
        let id = parse(minted);
        assert_eq!(&id.to_string(), minted, "{minted} must be canonical");
        assert_eq!(id[6] >> 4, 7, "{minted} must be a UUIDv7");
        assert_eq!(id[8] >> 6, 0b10, "{minted} must carry the RFC 9562 variant");
        assert_eq!(id_time(&id), at_ms(1_767_254_400_123), "{minted}");
    }

    let later = scalars(
        &db.serving,
        "SELECT td_mint_id('2026-01-01 08:00:00.124+00')",
    )
    .await;
    assert!(
        early.iter().all(|e| e < &later[0]),
        "a later instant must sort after"
    );
    assert!(
        early.iter().all(|e| parse(e) < parse(&later[0])),
        "and so must its Id"
    );
}
