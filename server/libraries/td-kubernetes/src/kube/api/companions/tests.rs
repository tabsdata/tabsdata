//
// Copyright 2026 Tabsdata Inc.
//

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::base::error::kubernetes::{KubeJobError, KubernetesError};
use crate::kube::api::companions::as_one_unit;

const NAMESPACE: &str = "a-namespace";
const WORKLOAD: &str = "a-workload";

fn rejected() -> KubernetesError {
    KubeJobError::ApplyFailed {
        namespace: NAMESPACE.to_owned(),
        cause: "the api server rejected it".to_owned(),
    }
    .into()
}

#[derive(Default)]
struct Ledger {
    created: AtomicUsize,
    attached: AtomicUsize,
    removed: AtomicUsize,
}

impl Ledger {
    fn counts(&self) -> (usize, usize, usize) {
        (
            self.created.load(Ordering::SeqCst),
            self.attached.load(Ordering::SeqCst),
            self.removed.load(Ordering::SeqCst),
        )
    }
}

async fn run(
    ledger: Arc<Ledger>,
    attaching: Result<(), KubernetesError>,
    removing: Result<(), KubernetesError>,
    step: Duration,
) -> Result<u8, KubernetesError> {
    let creating = ledger.clone();
    let attached = ledger.clone();
    let removed = ledger.clone();
    as_one_unit(
        NAMESPACE.to_owned(),
        WORKLOAD.to_owned(),
        move || async move {
            tokio::time::sleep(step).await;
            creating.created.fetch_add(1, Ordering::SeqCst);
            Ok(7u8)
        },
        move |created: u8| async move {
            tokio::time::sleep(step).await;
            attached.attached.fetch_add(1, Ordering::SeqCst);
            (created, attaching)
        },
        move || async move {
            tokio::time::sleep(step).await;
            removed.removed.fetch_add(1, Ordering::SeqCst);
            removing
        },
    )
    .await
}

#[tokio::test]
async fn every_step_runs_when_nothing_fails() {
    let ledger = Arc::new(Ledger::default());
    let outcome = run(ledger.clone(), Ok(()), Ok(()), Duration::ZERO).await;
    assert_eq!(outcome.unwrap(), 7);
    assert_eq!(ledger.counts(), (1, 1, 0));
}

#[tokio::test]
async fn a_timeout_on_the_caller_does_not_cut_the_sequence() {
    let ledger = Arc::new(Ledger::default());
    let step = Duration::from_millis(100);
    let running = run(ledger.clone(), Ok(()), Ok(()), step);

    let waited = tokio::time::timeout(Duration::from_millis(150), running).await;
    assert!(waited.is_err(), "the caller is expected to give up waiting");
    assert_eq!(ledger.counts(), (1, 0, 0));

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(ledger.counts(), (1, 1, 0));
}

#[tokio::test]
async fn a_failure_to_attach_removes_the_workload() {
    let ledger = Arc::new(Ledger::default());
    let outcome = run(ledger.clone(), Err(rejected()), Ok(()), Duration::ZERO).await;
    assert!(outcome.is_err());
    assert_eq!(ledger.counts(), (1, 1, 1));
}

#[tokio::test]
async fn a_failure_to_attach_reports_what_went_wrong() {
    let ledger = Arc::new(Ledger::default());
    let outcome = run(ledger, Err(rejected()), Ok(()), Duration::ZERO).await;
    assert!(matches!(outcome, Err(KubernetesError::Job(_))));
}

#[tokio::test]
async fn a_removal_that_fails_is_reported_as_such() {
    let ledger = Arc::new(Ledger::default());
    let outcome = run(
        ledger.clone(),
        Err(rejected()),
        Err(rejected()),
        Duration::ZERO,
    )
    .await;
    assert!(matches!(outcome, Err(KubernetesError::Companions(_))));
    assert_eq!(ledger.counts(), (1, 1, 1));
}

#[tokio::test]
async fn a_workload_that_cannot_be_created_attaches_nothing() {
    let ledger = Arc::new(Ledger::default());
    let creating = ledger.clone();
    let attached = ledger.clone();
    let removed = ledger.clone();
    let outcome: Result<u8, KubernetesError> = as_one_unit(
        NAMESPACE.to_owned(),
        WORKLOAD.to_owned(),
        move || async move {
            creating.created.fetch_add(1, Ordering::SeqCst);
            Err(rejected())
        },
        move |created: u8| async move {
            attached.attached.fetch_add(1, Ordering::SeqCst);
            (created, Ok(()))
        },
        move || async move {
            removed.removed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;
    assert!(matches!(outcome, Err(KubernetesError::Job(_))));
    assert_eq!(ledger.counts(), (1, 0, 0));
}

#[tokio::test]
async fn a_panicking_sequence_is_reported_rather_than_lost() {
    let outcome: Result<u8, KubernetesError> = as_one_unit(
        NAMESPACE.to_owned(),
        WORKLOAD.to_owned(),
        || async { panic!("the sequence panicked") },
        |created: u8| async move { (created, Ok(())) },
        || async { Ok(()) },
    )
    .await;
    assert!(matches!(outcome, Err(KubernetesError::Companions(_))));
}
