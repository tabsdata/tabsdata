//
// Copyright 2026 Tabsdata Inc.
//

use td_objects::types::basic::{FunctionId, WorkerId};

use super::*;

const FUNCTION: &str = "00000000000000000000000f00";
const OTHER_FUNCTION: &str = "00000000000000000000000f01";

fn key(kind: WorkloadKind) -> Key {
    Key {
        function_id: FUNCTION.to_owned(),
        kind,
    }
}

fn submission(worker_id: &str) -> Submission<&'static str> {
    Submission {
        worker_id: worker_id.to_owned(),
        name: format!("pub-{worker_id}"),
        payload: "payload",
    }
}

fn workload(
    name: &str,
    function: &str,
    kind: WorkloadKind,
    phase: WorkloadPhase,
) -> ObservedWorkload {
    ObservedWorkload {
        worker_id: WorkerId::try_from("wkr20260101-0000000009").ok(),
        function_id: FunctionId::try_from(function).ok(),
        function: None,
        kind,
        name: name.to_owned(),
        phase,
        phase_since: None,
        finished_at: None,
        message: None,
        deleting: false,
    }
}

fn creates<P: Clone>(actions: &[Action<P>]) -> Vec<Submission<P>> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Create { submission, .. } => Some(submission.clone()),
            _ => None,
        })
        .collect()
}

fn deletes<P>(actions: &[Action<P>]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Delete { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn a_submission_with_nothing_in_the_way_is_created() {
    let mut state = State::default();
    state.submit(key(WorkloadKind::Function), submission("a"));
    let actions = state.plan(&[], Instant::now());
    assert_eq!(creates(&actions), vec![submission("a")]);
    assert!(state.is_idle());
}

#[test]
fn a_newer_submission_replaces_one_never_created() {
    let mut state = State::default();
    state.submit(key(WorkloadKind::Function), submission("a"));
    let replaced = state.submit(key(WorkloadKind::Function), submission("b"));
    assert_eq!(replaced, Some(submission("a")));
    assert_eq!(
        creates(&state.plan(&[], Instant::now())),
        vec![submission("b")]
    );
}

#[test]
fn withdrawing_a_queued_submission_cancels_it() {
    let mut state = State::default();
    state.submit(key(WorkloadKind::Function), submission("a"));
    state.withdraw(key(WorkloadKind::Function), "a", "pub-a");
    assert!(state.plan(&[], Instant::now()).is_empty());
    assert!(state.is_idle());
}

#[test]
fn withdrawing_a_created_workload_deletes_it_until_it_is_gone() {
    let mut state: State<&str> = State::default();
    state.withdraw(key(WorkloadKind::Function), "a", "pub-a");
    let running = workload(
        "pub-a",
        FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Running,
    );
    let actions = state.plan(std::slice::from_ref(&running), Instant::now());
    assert_eq!(deletes(&actions), vec!["pub-a".to_owned()]);
    assert!(state.plan(&[running], Instant::now()).is_empty());
    assert!(!state.is_idle());
    state.plan(&[], Instant::now());
    assert!(state.is_idle());
}

#[test]
fn another_live_workload_of_the_function_is_deleted_before_the_create() {
    let mut state = State::default();
    state.submit(key(WorkloadKind::Function), submission("b"));
    let old = workload(
        "pub-a",
        FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Running,
    );
    let actions = state.plan(std::slice::from_ref(&old), Instant::now());
    assert_eq!(deletes(&actions), vec!["pub-a".to_owned()]);
    assert!(creates(&actions).is_empty());
    let mut going = old;
    going.deleting = true;
    assert!(creates(&state.plan(&[going], Instant::now())).is_empty());
    assert_eq!(
        creates(&state.plan(&[], Instant::now())),
        vec![submission("b")]
    );
}

#[test]
fn finished_workloads_neither_block_nor_get_deleted() {
    let mut state = State::default();
    state.submit(key(WorkloadKind::Function), submission("b"));
    let done = workload(
        "pub-a",
        FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Succeeded,
    );
    let failed = workload(
        "pub-c",
        FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Failed,
    );
    let actions = state.plan(&[done, failed], Instant::now());
    assert!(deletes(&actions).is_empty());
    assert_eq!(creates(&actions), vec![submission("b")]);
}

#[test]
fn a_trigger_and_a_run_of_the_same_function_coexist() {
    let mut state = State::default();
    state.submit(key(WorkloadKind::Function), submission("b"));
    let trigger = workload(
        "crn-a",
        FUNCTION,
        WorkloadKind::CronTrigger,
        WorkloadPhase::Present,
    );
    let actions = state.plan(&[trigger], Instant::now());
    assert!(deletes(&actions).is_empty());
    assert_eq!(creates(&actions), vec![submission("b")]);
}

#[test]
fn workloads_of_other_functions_are_left_alone() {
    let mut state = State::default();
    state.submit(key(WorkloadKind::Function), submission("b"));
    let other = workload(
        "pub-z",
        OTHER_FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Running,
    );
    let actions = state.plan(&[other], Instant::now());
    assert!(deletes(&actions).is_empty());
    assert_eq!(creates(&actions), vec![submission("b")]);
}

#[test]
fn a_workload_already_being_deleted_is_adopted_not_deleted_again() {
    let mut state: State<&str> = State::default();
    let mut going = workload(
        "pub-a",
        FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Running,
    );
    going.deleting = true;
    let actions = state.plan(&[going], Instant::now());
    assert!(deletes(&actions).is_empty());
    assert!(!state.is_idle());
}

#[test]
fn a_slow_deletion_warns_then_forces() {
    let mut state: State<&str> = State::default();
    let start = Instant::now();
    state.withdraw(
        key(WorkloadKind::Function),
        "wkr20260101-0000000009",
        "pub-a",
    );
    let running = workload(
        "pub-a",
        FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Running,
    );
    state.plan(std::slice::from_ref(&running), start);
    assert!(
        state
            .plan(std::slice::from_ref(&running), start + WARN_AFTER / 2)
            .is_empty()
    );
    let warned = state.plan(std::slice::from_ref(&running), start + WARN_AFTER);
    assert!(matches!(warned.as_slice(), [Action::Warn { name, .. }] if name == "pub-a"));
    assert!(
        state
            .plan(std::slice::from_ref(&running), start + WARN_AFTER * 2)
            .is_empty()
    );
    let forced = state.plan(std::slice::from_ref(&running), start + FORCE_AFTER);
    assert!(matches!(
        forced.as_slice(),
        [Action::Force { name, worker_id }] if name == "pub-a" && worker_id == "wkr20260101-0000000009"
    ));
    assert!(state.plan(&[running], start + FORCE_AFTER * 2).is_empty());
}

#[test]
fn a_failed_delete_is_issued_again() {
    let mut state: State<&str> = State::default();
    state.withdraw(key(WorkloadKind::Function), "a", "pub-a");
    let running = workload(
        "pub-a",
        FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Running,
    );
    state.plan(std::slice::from_ref(&running), Instant::now());
    state.reissue("pub-a");
    assert_eq!(
        deletes(&state.plan(&[running], Instant::now())),
        vec!["pub-a".to_owned()]
    );
}

#[test]
fn a_failed_create_is_restored_unless_replaced_or_withdrawn() {
    let mut state = State::default();
    state.restore(key(WorkloadKind::Function), submission("a"));
    assert_eq!(
        creates(&state.plan(&[], Instant::now())),
        vec![submission("a")]
    );

    state.submit(key(WorkloadKind::Function), submission("b"));
    state.restore(key(WorkloadKind::Function), submission("a"));
    assert_eq!(
        creates(&state.plan(&[], Instant::now())),
        vec![submission("b")]
    );

    state.withdraw(key(WorkloadKind::Function), "a", "pub-a");
    state.restore(key(WorkloadKind::Function), submission("a"));
    assert!(creates(&state.plan(&[], Instant::now())).is_empty());
}

#[test]
fn a_submission_whose_workload_already_exists_is_dropped() {
    let mut state = State::default();
    state.submit(key(WorkloadKind::Function), submission("a"));
    let existing = workload(
        "pub-a",
        FUNCTION,
        WorkloadKind::Function,
        WorkloadPhase::Running,
    );
    assert!(state.plan(&[existing], Instant::now()).is_empty());
    assert!(state.is_idle());
}
