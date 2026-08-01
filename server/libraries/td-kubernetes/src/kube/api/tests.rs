//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn non_ephemeral_selector_excludes_ephemeral_tier() {
    let rendered = tabsdata_non_ephemeral_selector().to_string();
    assert!(rendered.contains(&format!("{PART_OF_LABEL}={PART_OF_TABSDATA}")));
    assert!(rendered.contains(&format!("{TIER_LABEL} notin (ephemeral)")));
}

#[tokio::test]
async fn a_wait_inside_the_budget_returns_its_outcome() {
    let waiting = async { "settled" };
    let outcome = heartbeat(waiting, "probe", Instant::now(), Duration::from_secs(5)).await;
    assert_eq!(outcome, Some("settled"));
}

#[tokio::test]
async fn a_wait_beyond_the_budget_gives_up() {
    let waiting = async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        "never"
    };
    let outcome = heartbeat(waiting, "probe", Instant::now(), Duration::from_millis(50)).await;
    assert_eq!(outcome, None);
}

#[tokio::test]
async fn a_wait_with_no_budget_never_gives_up() {
    let waiting = async {
        tokio::time::sleep(Duration::from_millis(80)).await;
        "settled"
    };
    let outcome = heartbeat(waiting, "probe", Instant::now(), None).await;
    assert_eq!(outcome, Some("settled"));
}

#[test]
fn the_heartbeat_fits_inside_the_bounded_budget() {
    assert!(AWAIT_CONDITION_HEARTBEAT < AWAIT_CONDITION_TIMEOUT);
    assert!(!AWAIT_CONDITION_HEARTBEAT.is_zero());
}
