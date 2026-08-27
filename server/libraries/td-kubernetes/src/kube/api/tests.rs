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

#[test]
fn the_worker_selector_narrows_the_listing_to_observable_workloads() {
    let rendered = tabsdata_worker_selector().to_string();
    assert!(
        rendered.contains(&format!("{PART_OF_LABEL}={PART_OF_TABSDATA}")),
        "{rendered}"
    );
    assert!(rendered.contains(POD_YAML_WORKER_LABEL), "{rendered}");
    assert!(rendered.contains(LABEL_WORKER_ID), "{rendered}");
}

#[test]
fn the_worker_selector_is_stricter_than_the_plain_one() {
    let plain = tabsdata_selector(None).to_string();
    let worker = tabsdata_worker_selector().to_string();
    assert!(worker.len() > plain.len(), "{worker} vs {plain}");
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

fn api_error(code: u16) -> kube::Error {
    kube::Error::Api(ErrorResponse {
        status: "Failure".to_owned(),
        message: "denied".to_owned(),
        reason: "Forbidden".to_owned(),
        code,
    })
}

fn connect_error() -> kube::Error {
    kube::Error::Service("client error (Connect)".into())
}

#[test]
fn an_unreachable_cluster_is_told_apart_from_a_refusing_one() {
    assert!(is_connect_failure(&connect_error()));
    assert!(!is_connect_failure(&api_error(403)));
    assert!(!is_connect_failure(&api_error(404)));
}

#[tokio::test]
async fn a_reachable_cluster_is_never_retried() {
    let mut calls = 0;
    let outcome = retry_while_unreachable("reading", || {
        calls += 1;
        async { Ok::<_, kube::Error>("value") }
    })
    .await;
    assert_eq!(outcome.unwrap(), "value");
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn an_apiserver_that_answers_no_is_not_retried() {
    let mut calls = 0;
    let outcome = retry_while_unreachable("reading", || {
        calls += 1;
        async { Err::<(), _>(api_error(404)) }
    })
    .await;
    assert!(outcome.is_err());
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn a_cluster_that_becomes_reachable_is_awaited() {
    let mut calls = 0;
    let outcome = retry_while_unreachable("reading", || {
        calls += 1;
        let attempt = calls;
        async move {
            if attempt < 3 {
                Err(connect_error())
            } else {
                Ok("value")
            }
        }
    })
    .await;
    assert_eq!(outcome.unwrap(), "value");
    assert_eq!(calls, 3);
}

#[test]
fn the_connect_retry_polls_more_than_once_inside_its_budget() {
    assert!(CONNECT_RETRY_INTERVAL < CONNECT_RETRY_TIMEOUT);
    assert!(!CONNECT_RETRY_INTERVAL.is_zero());
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
