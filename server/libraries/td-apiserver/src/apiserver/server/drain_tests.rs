//
// Copyright 2026 Tabsdata Inc.
//

//! Shutdown drains: a request in flight when the signal arrives completes,
//! new connections are refused, and the server returns only once drained.

use crate::apiserver::server::tls_fixture::tls_folder;
use crate::apiserver::server::{Server, ServerBuilder, ServerError, serve};
use async_trait::async_trait;
use axum::Router;
use axum::routing::get;
use http::uri::Scheme;
use nonempty::nonempty;
use reqwest::Client;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use td_objects::types::addresses::NonEmptyAddresses;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct SlowRequest {
    entered: Notify,
    released: Notify,
}

/// `/slow` tells `entered` it started, then answers once `released`.
fn slow_router(slow: Arc<SlowRequest>) -> Router {
    Router::new().route(
        "/slow",
        get(move || {
            let slow = slow.clone();
            async move {
                slow.entered.notify_one();
                slow.released.notified().await;
                "done"
            }
        }),
    )
}

fn localhost() -> NonEmptyAddresses {
    NonEmptyAddresses::new(nonempty![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)])
}

fn client() -> Client {
    Client::builder()
        .tls_danger_accept_invalid_certs(true)
        .build()
        .unwrap()
}

async fn drains_a_request_in_flight(server: Box<dyn Server>, slow: Arc<SlowRequest>) {
    let addr = server.listeners()[0].local_addr().unwrap();
    let base = format!("{}://localhost:{}", server.scheme(), addr.port());
    let url = format!("{base}/slow");
    let shutdown = CancellationToken::new();
    let served = tokio::spawn(serve(server, shutdown.clone()));

    let in_flight = tokio::spawn({
        let url = url.clone();
        async move { client().get(url).send().await }
    });
    slow.entered.notified().await;
    shutdown.cancel();

    let refused = tokio::time::timeout(Duration::from_secs(5), async {
        while client().get(format!("{base}/probe")).send().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        refused.is_ok(),
        "new connections are still accepted after the signal"
    );
    assert!(!served.is_finished(), "the server returned before draining");
    slow.released.notify_one();

    let response = in_flight.await.unwrap().expect("the request in flight");
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "done");
    tokio::time::timeout(Duration::from_secs(5), served)
        .await
        .expect("the server returns once drained")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_plain_drains_a_request_in_flight() {
    let slow = Arc::new(SlowRequest::default());
    let server = ServerBuilder::new(localhost(), slow_router(slow.clone()))
        .build()
        .await
        .unwrap();
    assert_eq!(server.scheme(), Scheme::HTTP);
    drains_a_request_in_flight(server, slow).await;
}

#[tokio::test]
async fn test_tls_drains_a_request_in_flight() {
    let slow = Arc::new(SlowRequest::default());
    let server = ServerBuilder::new(localhost(), slow_router(slow.clone()))
        .tls(tls_folder().await)
        .build()
        .await
        .unwrap();
    assert_eq!(server.scheme(), Scheme::HTTPS);
    drains_a_request_in_flight(server, slow).await;
}

/// One listener that fails at once, one that runs until stopped.
#[derive(Debug)]
struct OneListenerFails {
    stopped: Arc<Notify>,
    listeners: Vec<TcpListener>,
}

#[async_trait]
impl Server for OneListenerFails {
    async fn handles(
        self: Box<Self>,
        shutdown: CancellationToken,
    ) -> Vec<JoinHandle<Result<(), ServerError>>> {
        let failing = tokio::spawn(async {
            Err(ServerError::Server(
                "failing".to_string(),
                std::io::Error::other("listener failed"),
            ))
        });
        let stopped = self.stopped.clone();
        let running = tokio::spawn(async move {
            shutdown.cancelled().await;
            stopped.notify_one();
            Ok(())
        });
        vec![failing, running]
    }

    fn listeners(&self) -> &Vec<TcpListener> {
        &self.listeners
    }

    fn scheme(&self) -> Scheme {
        Scheme::HTTP
    }
}

#[tokio::test]
async fn test_a_failing_listener_stops_the_others_and_fails_the_server() {
    let stopped = Arc::new(Notify::new());
    let server = Box::new(OneListenerFails {
        stopped: stopped.clone(),
        listeners: vec![],
    });
    let shutdown = CancellationToken::new();
    let served = tokio::time::timeout(Duration::from_secs(5), serve(server, shutdown.clone()))
        .await
        .expect("the server returns without the group's signal");
    assert!(served.is_err());
    assert!(
        !shutdown.is_cancelled(),
        "only the server's own listeners stop"
    );
    tokio::time::timeout(Duration::from_secs(1), stopped.notified())
        .await
        .expect("the running listener was stopped");
}
