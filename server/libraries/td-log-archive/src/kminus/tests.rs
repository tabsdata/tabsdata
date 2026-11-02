//
// Copyright 2026 Tabsdata Inc.
//

use futures::StreamExt;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const WORKER: &str = "wkr20261017-440";
const LOG_ID: &str = "pub-wkr20261017-440";
const PRCONFIG_FILE: &str = "vk-prconfig.yaml";
const POD_LOGS: &str = "========== POD STARTED ==========\n\
                        --- container 1000-function ---\n\
                        line one\n\
                        line two\n";

struct Pki {
    authority: String,
    client: String,
    client_key: String,
}

fn pki() -> Pki {
    let mut authority = CertificateParams::new(Vec::<String>::new()).unwrap();
    authority.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let authority = CertifiedIssuer::self_signed(authority, KeyPair::generate().unwrap()).unwrap();
    let client_key = KeyPair::generate().unwrap();
    let client = CertificateParams::new(Vec::<String>::new())
        .unwrap()
        .signed_by(&client_key, &*authority)
        .unwrap();
    Pki {
        authority: authority.pem(),
        client: client.pem(),
        client_key: client_key.serialize_pem(),
    }
}

fn prconfig_text(url: &str, authority: &str, pki: &Pki) -> String {
    format!(
        "url: \"{url}\"\n\
         nodeName: \"tabsdata\"\n\
         namespace: \"tabsdata\"\n\
         server:\n  \
           tls:\n    \
             certificate-authority-data: {authority}\n\
         client:\n  \
           id: \"vk\"\n  \
           certificate-data: {}\n  \
           certificate-key-data: {}\n\
         metricsUrl: \"http://127.0.0.1:2464\"\n",
        STANDARD.encode(&pki.client),
        STANDARD.encode(&pki.client_key),
    )
}

fn write_prconfig(folder: &TempDir, url: &str) -> PathBuf {
    let path = folder.path().join(PRCONFIG_FILE);
    let pki = pki();
    let authority = STANDARD.encode(&pki.authority);
    std::fs::write(&path, prconfig_text(url, &authority, &pki)).expect("prconfig written");
    path
}

fn request() -> ArchiveRequest<'static> {
    ArchiveRequest {
        worker_id: WORKER,
        log_id: LOG_ID,
        started_on_millis: None,
    }
}

async fn read(reader: &PodRunnerReader) -> String {
    let mut stream = reader.stream(request());
    let mut output = String::new();
    while let Some(item) = stream.next().await {
        output.push_str(std::str::from_utf8(&item.expect("no backend error")).expect("utf-8"));
    }
    output
}

async fn backend_error(reader: &PodRunnerReader) -> String {
    let items: Vec<_> = reader.stream(request()).collect().await;
    assert_eq!(items.len(), 1);
    match items.into_iter().next().unwrap() {
        Err(LogBridgeError::Backend { cause }) => cause,
        other => panic!("expected a backend error, got {other:?}"),
    }
}

async fn pod_logs(server: &MockServer, response: ResponseTemplate) {
    Mock::given(method("GET"))
        .and(path(format!("/{POD_LOGS_PATH}")))
        .and(query_param(LOG_POD_ID_PARAM, LOG_ID))
        .respond_with(response)
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn passes_the_pod_logs_through_unchanged() {
    let server = MockServer::start().await;
    pod_logs(
        &server,
        ResponseTemplate::new(200).set_body_string(POD_LOGS),
    )
    .await;
    let folder = TempDir::new().unwrap();
    let reader = PodRunnerReader::new(write_prconfig(&folder, &server.uri()));

    assert_eq!(read(&reader).await, POD_LOGS);
}

#[tokio::test]
async fn reports_no_logs_kept_when_the_pod_runner_has_none() {
    let server = MockServer::start().await;
    pod_logs(
        &server,
        ResponseTemplate::new(404).set_body_string("{\"error\":\"not found\"}"),
    )
    .await;
    let folder = TempDir::new().unwrap();
    let reader = PodRunnerReader::new(write_prconfig(&folder, &server.uri()));

    assert_eq!(
        read(&reader).await,
        format!("no logs kept for worker '{WORKER}' by the pod runner (they may have expired)\n")
    );
}

#[tokio::test]
async fn fails_with_the_body_of_other_statuses() {
    let server = MockServer::start().await;
    pod_logs(
        &server,
        ResponseTemplate::new(400).set_body_string("invalid logPodId\n"),
    )
    .await;
    let folder = TempDir::new().unwrap();
    let reader = PodRunnerReader::new(write_prconfig(&folder, &server.uri()));

    let cause = backend_error(&reader).await;
    assert!(cause.contains(LOG_ID), "{cause}");
    assert!(cause.contains("400"), "{cause}");
    assert!(cause.ends_with("invalid logPodId"), "{cause}");
}

#[tokio::test]
async fn fails_when_the_pod_runner_is_unreachable() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let folder = TempDir::new().unwrap();
    let reader = PodRunnerReader::new(write_prconfig(&folder, &url));

    let cause = backend_error(&reader).await;
    assert!(cause.contains(LOG_ID), "{cause}");
}

#[tokio::test]
async fn fails_when_the_prconfig_is_missing() {
    let folder = TempDir::new().unwrap();
    let missing = folder.path().join(PRCONFIG_FILE);
    let reader = PodRunnerReader::new(&missing);

    let cause = backend_error(&reader).await;
    assert!(cause.contains(&missing.display().to_string()), "{cause}");
}

#[tokio::test]
async fn fails_when_a_certificate_is_not_base64() {
    let folder = TempDir::new().unwrap();
    let prconfig = folder.path().join(PRCONFIG_FILE);
    std::fs::write(
        &prconfig,
        prconfig_text("http://127.0.0.1:1", "not base64!", &pki()),
    )
    .unwrap();
    let reader = PodRunnerReader::new(&prconfig);

    let cause = backend_error(&reader).await;
    assert!(cause.contains("certificate-authority-data"), "{cause}");
}

#[tokio::test]
async fn rereads_the_prconfig_on_every_call() {
    let first = MockServer::start().await;
    let second = MockServer::start().await;
    pod_logs(
        &first,
        ResponseTemplate::new(200).set_body_string("first\n"),
    )
    .await;
    pod_logs(
        &second,
        ResponseTemplate::new(200).set_body_string("second\n"),
    )
    .await;
    let folder = TempDir::new().unwrap();
    let reader = PodRunnerReader::new(write_prconfig(&folder, &first.uri()));

    assert_eq!(read(&reader).await, "first\n");
    write_prconfig(&folder, &second.uri());
    assert_eq!(read(&reader).await, "second\n");
}

#[tokio::test]
async fn reads_the_prconfig_the_pod_runner_writes() {
    let folder = TempDir::new().unwrap();
    let config = PodRunnerConfig::read(&write_prconfig(&folder, "https://127.0.0.1:2463/"))
        .await
        .expect("a valid prconfig");

    assert_eq!(config.pod_logs_url(), "https://127.0.0.1:2463/getPodLogs");
    config.client().expect("an mTLS client");
}
