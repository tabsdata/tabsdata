//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

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

fn failed_job() -> Job {
    Job {
        status: Some(k8s_openapi::api::batch::v1::JobStatus {
            conditions: Some(vec![k8s_openapi::api::batch::v1::JobCondition {
                type_: JOB_CONDITION_FAILED.to_owned(),
                status: JOB_CONDITION_STATUS_TRUE.to_owned(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn a_job_that_settled_as_failed_reports_failure() {
    assert!(
        failed_job().has_failed(),
        "a Failed condition is how a job reports a bad end; the wait itself \
         succeeds, so nothing else can tell the diagnostics to run"
    );
}

#[test]
fn a_completed_job_reports_no_failure() {
    let mut job = failed_job();
    job.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].type_ = "Complete".to_owned();
    assert!(!job.has_failed());
}

#[test]
fn a_job_without_status_reports_no_failure() {
    assert!(!Job::default().has_failed());
}

#[test]
fn a_pod_in_the_failed_phase_reports_failure() {
    let pod = Pod {
        status: Some(k8s_openapi::api::core::v1::PodStatus {
            phase: Some(POD_PHASE_FAILED.to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(pod.has_failed());
}

#[test]
fn a_running_pod_reports_no_failure() {
    let pod = Pod {
        status: Some(k8s_openapi::api::core::v1::PodStatus {
            phase: Some("Running".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(!pod.has_failed());
}

#[test]
fn resources_that_cannot_settle_badly_never_report_failure() {
    assert!(!Deployment::default().has_failed());
    assert!(!CronJob::default().has_failed());
}
