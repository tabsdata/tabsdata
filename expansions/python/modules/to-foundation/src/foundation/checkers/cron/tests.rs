//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use tm_workspace::workspace_root;

const SCHEDULES: &str = include_str!(concat!(
    workspace_root!(),
    "/server/libraries/td-common/resources/cron/schedules.jsonl"
));

#[test]
fn every_schedule_is_checked_as_kubernetes_checks_it() {
    let disagreements: Vec<String> = SCHEDULES
        .lines()
        .filter_map(|line| {
            let (schedule, valid): (String, bool) = serde_json::from_str(line).unwrap();
            (cron(&schedule).is_ok() != valid).then_some(line.to_owned())
        })
        .collect();
    assert_eq!(disagreements, Vec::<String>::new());
}

#[test]
fn a_descriptor_kubernetes_documents_is_accepted() {
    assert!(cron("@daily").is_ok());
}

#[test]
fn a_day_of_week_past_saturday_is_refused() {
    assert!(cron("0 0 * * 7").is_err());
}
