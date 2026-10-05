//
// Copyright 2026 Tabsdata Inc.
//

use kube::core::ErrorResponse;

use super::*;

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
