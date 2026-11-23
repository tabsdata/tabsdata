//
// Copyright 2026 Tabsdata Inc.
//

//! td-schema spells statuses as raw codes, being below td-objects; each code
//! set it keeps must equal the enum's.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use td_objects::types::basic::{
    LogicalFunctionRunStatus, PhysicalFunctionRunStatus, WorkerStatus, WorkerType,
};
use td_schema::schema::current::{
    ATTEMPT_TRANSITIONS, ATTEMPTS_IN_FLIGHT, RUN_DISCARDED, RUN_TERMINALS, RUN_TRANSITIONS,
    WORKER_TRANSITIONS,
};

fn codes<S: Display>(statuses: &[S]) -> BTreeSet<String> {
    statuses.iter().map(ToString::to_string).collect()
}

fn pairs<S: Display>(transitions: &[(S, S)]) -> BTreeSet<(String, String)> {
    transitions
        .iter()
        .map(|(from, to)| (from.to_string(), to.to_string()))
        .collect()
}

#[test]
fn run_terminals_match_the_enum() {
    assert_eq!(
        codes(RUN_TERMINALS),
        codes(LogicalFunctionRunStatus::TERMINAL)
    );
}

#[test]
fn run_discarded_match_the_enum() {
    assert_eq!(
        codes(RUN_DISCARDED),
        codes(LogicalFunctionRunStatus::DISCARDED)
    );
}

#[test]
fn run_transitions_match_the_enum() {
    assert_eq!(
        pairs(RUN_TRANSITIONS),
        pairs(LogicalFunctionRunStatus::TRANSITIONS)
    );
}

#[test]
fn attempts_in_flight_match_the_enum() {
    assert_eq!(
        codes(ATTEMPTS_IN_FLIGHT),
        codes(PhysicalFunctionRunStatus::IN_FLIGHT)
    );
}

#[test]
fn attempt_transitions_match_the_enum() {
    assert_eq!(
        pairs(ATTEMPT_TRANSITIONS),
        pairs(PhysicalFunctionRunStatus::TRANSITIONS)
    );
}

#[test]
fn worker_transitions_match_the_enum() {
    let schema: BTreeMap<String, BTreeSet<(String, String)>> = WORKER_TRANSITIONS
        .iter()
        .map(|(kind, transitions)| (kind.to_string(), pairs(transitions)))
        .collect();
    let objects: BTreeMap<String, BTreeSet<(String, String)>> = WorkerType::iter()
        .map(|kind| (kind.to_string(), pairs(WorkerStatus::transitions(&kind))))
        .collect();
    assert_eq!(schema, objects);
}
