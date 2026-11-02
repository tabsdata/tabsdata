//
// Copyright 2026 Tabsdata Inc.
//

use futures::StreamExt;
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const PROJECT: &str = "tabsdata-dev";
const CLUSTER: &str = "tabsdata-gke-dev";
const NAMESPACE: &str = "tabsdatashreyas";
const WORKER: &str = "wkr20261017-440";
const LOG_ID: &str = "pub-wkr20261017-440";
const SERVICE_ACCOUNT: &str = "td-tabsdatashreyas-apiserver@tabsdata-dev.iam.gserviceaccount.com";
const DEFAULT_VIEW: &str =
    "projects/tabsdata-dev/locations/global/buckets/_Default/views/tabsdata-tabsdatashreyas";
const LIST: &str = "/v2/entries:list";
const TOKEN: &str = "/computeMetadata/v1/instance/service-accounts/default/token";

fn endpoints(server: &MockServer) -> Endpoints {
    Endpoints {
        metadata: format!("{}/computeMetadata/v1/", server.uri()),
        logging: format!("{}/v2/", server.uri()),
    }
}

async fn metadata_value(server: &MockServer, route: &str, value: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/computeMetadata/v1/{route}")))
        .and(header(METADATA_FLAVOR_HEADER, METADATA_FLAVOR))
        .respond_with(ResponseTemplate::new(200).set_body_string(value))
        .mount(server)
        .await;
}

async fn gke_metadata(server: &MockServer, email: &str) {
    metadata_value(server, EMAIL_PATH, email).await;
    metadata_value(server, PROJECT_PATH, PROJECT).await;
    metadata_value(server, CLUSTER_NAME_PATH, CLUSTER).await;
}

fn token_response(expires_in: u64) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "access_token": "token",
        "expires_in": expires_in,
        "token_type": "Bearer",
    }))
}

async fn token(server: &MockServer, expires_in: u64, expected: u64) {
    Mock::given(method("GET"))
        .and(path(TOKEN))
        .and(header(METADATA_FLAVOR_HEADER, METADATA_FLAVOR))
        .respond_with(token_response(expires_in))
        .expect(expected)
        .mount(server)
        .await;
}

async fn reader(server: &MockServer, place: ViewPlace) -> CloudLoggingReader {
    gke_metadata(server, SERVICE_ACCOUNT).await;
    CloudLoggingReader::discover(
        endpoints(server),
        NAMESPACE,
        place,
        Duration::from_millis(1),
    )
    .await
    .expect("a Google service account turns Cloud Logging on")
}

fn entry(container: &str, payload: Value) -> Value {
    let mut entry = json!({
        "resource": {
            "type": "k8s_container",
            "labels": { "container_name": container, "namespace_name": NAMESPACE }
        }
    });
    match payload {
        Value::String(text) => entry["textPayload"] = Value::String(text),
        other => entry["jsonPayload"] = other,
    }
    entry
}

fn page(entries: Vec<Value>, next: Option<&str>) -> ResponseTemplate {
    let mut body = json!({ "entries": entries });
    if let Some(next) = next {
        body["nextPageToken"] = Value::String(next.to_owned());
    }
    ResponseTemplate::new(200).set_body_json(body)
}

async fn read(reader: &CloudLoggingReader, started_on_millis: Option<i64>) -> String {
    let mut stream = reader.stream(ArchiveRequest {
        worker_id: WORKER,
        log_id: LOG_ID,
        started_on_millis,
    });
    let mut output = String::new();
    while let Some(item) = stream.next().await {
        output.push_str(std::str::from_utf8(&item.expect("no backend error")).expect("utf-8"));
    }
    output
}

fn header_of(container: &str) -> String {
    String::from_utf8(container_header(container).to_vec()).expect("utf-8")
}

#[tokio::test]
async fn a_google_service_account_turns_cloud_logging_on() {
    let server = MockServer::start().await;
    let reader = reader(&server, ViewPlace::default()).await;
    assert_eq!(reader.inner.view, DEFAULT_VIEW);
}

#[tokio::test]
async fn a_workload_identity_pool_identity_leaves_it_off() {
    let server = MockServer::start().await;
    gke_metadata(&server, "tabsdata-dev.svc.id.goog").await;
    let reader = CloudLoggingReader::discover(
        endpoints(&server),
        NAMESPACE,
        ViewPlace::default(),
        Duration::from_millis(1),
    )
    .await;
    assert!(reader.is_none());
}

#[tokio::test]
async fn a_missing_cluster_name_leaves_it_off() {
    let server = MockServer::start().await;
    metadata_value(&server, EMAIL_PATH, SERVICE_ACCOUNT).await;
    metadata_value(&server, PROJECT_PATH, PROJECT).await;
    let reader = CloudLoggingReader::discover(
        endpoints(&server),
        NAMESPACE,
        ViewPlace::default(),
        Duration::from_millis(1),
    )
    .await;
    assert!(reader.is_none());
}

#[tokio::test]
async fn no_metadata_server_leaves_it_off() {
    let unreachable = Endpoints {
        metadata: "http://127.0.0.1:1/computeMetadata/v1/".to_owned(),
        logging: "http://127.0.0.1:1/v2/".to_owned(),
    };
    let reader = CloudLoggingReader::discover(
        unreachable,
        NAMESPACE,
        ViewPlace::default(),
        Duration::from_millis(1),
    )
    .await;
    assert!(reader.is_none());
}

#[test]
fn the_view_defaults_to_the_default_bucket_in_global() {
    assert_eq!(
        view_name(PROJECT, NAMESPACE, &ViewPlace::default()),
        DEFAULT_VIEW
    );
}

#[test]
fn the_view_comes_from_the_configured_bucket_and_location() {
    let place = ViewPlace {
        bucket: Some("tabsdata-logs".to_owned()),
        location: Some("europe-west1".to_owned()),
    };
    assert_eq!(
        view_name(PROJECT, NAMESPACE, &place),
        "projects/tabsdata-dev/locations/europe-west1/buckets/tabsdata-logs/views/tabsdata-tabsdatashreyas"
    );
}

#[test]
fn a_location_alone_keeps_the_default_bucket() {
    let place = ViewPlace {
        bucket: None,
        location: Some("europe-west1".to_owned()),
    };
    assert_eq!(
        view_name(PROJECT, NAMESPACE, &place),
        "projects/tabsdata-dev/locations/europe-west1/buckets/_Default/views/tabsdata-tabsdatashreyas"
    );
}

#[test]
fn pod_labels_become_gke_entry_labels() {
    assert_eq!(
        gke_pod_label("tabsdata.com/worker-id"),
        "k8s-pod/tabsdata_com/worker-id"
    );
    assert_eq!(
        gke_pod_label("app.kubernetes.io/part-of"),
        "k8s-pod/app_kubernetes_io/part-of"
    );
    assert_eq!(
        gke_pod_label("tabsdata.com/log-id"),
        "k8s-pod/tabsdata_com/log-id"
    );
}

#[test]
fn the_filter_selects_the_worker_by_its_log_id_from_shortly_before_its_start() {
    let started_on = DateTime::parse_from_rfc3339("2026-10-17T10:00:00Z")
        .expect("date")
        .timestamp_millis();
    let filter = entries_filter(CLUSTER, NAMESPACE, LOG_ID, Some(started_on));
    assert_eq!(
        filter,
        "(log_id(\"stdout\") OR log_id(\"stderr\")) \
         AND resource.type=\"k8s_container\" \
         AND resource.labels.cluster_name=\"tabsdata-gke-dev\" \
         AND resource.labels.namespace_name=\"tabsdatashreyas\" \
         AND labels.\"k8s-pod/tabsdata_com/log-id\"=\"pub-wkr20261017-440\" \
         AND timestamp>=\"2026-10-17T09:59:30.000Z\""
    );
}

#[test]
fn an_unknown_start_leaves_the_time_open() {
    let filter = entries_filter(CLUSTER, NAMESPACE, LOG_ID, None);
    assert!(!filter.contains("timestamp"));
}

#[tokio::test]
async fn the_request_reads_the_view_in_time_order() {
    let server = MockServer::start().await;
    token(&server, 3600, 1).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .and(header("authorization", "Bearer token"))
        .respond_with(page(vec![entry("0130-run-function", json!("done"))], None))
        .expect(1)
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    read(&reader, Some(0)).await;

    let requests = server.received_requests().await.expect("recorded");
    let list = requests
        .iter()
        .find(|request| request.url.path() == LIST)
        .expect("a list request");
    let body: Value = serde_json::from_slice(&list.body).expect("json body");
    assert_eq!(body["resourceNames"], json!([DEFAULT_VIEW]));
    assert_eq!(body["orderBy"], json!("timestamp asc"));
    assert_eq!(body["pageSize"], json!(PAGE_SIZE));
    assert!(body.get("pageToken").is_none());
    assert!(
        body["filter"]
            .as_str()
            .expect("filter")
            .contains("timestamp>=")
    );
}

#[tokio::test]
async fn pages_are_followed_and_a_header_opens_each_container() {
    let server = MockServer::start().await;
    token(&server, 3600, 1).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(page(vec![], Some("first")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .and(body_partial_json(json!({ "pageToken": "first" })))
        .respond_with(page(
            vec![
                entry("0010-venvloader", json!("installing")),
                entry("0010-venvloader", json!("ready\n")),
                entry("0130-run-function", json!("iteration 1")),
            ],
            Some("second"),
        ))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .and(body_partial_json(json!({ "pageToken": "second" })))
        .respond_with(page(
            vec![entry("0130-run-function", json!({ "event": "done" }))],
            None,
        ))
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    let output = read(&reader, None).await;

    let expected = format!(
        "{}installing\nready\n{}iteration 1\n{{\"event\":\"done\"}}\n",
        header_of("0010-venvloader"),
        header_of("0130-run-function"),
    );
    assert_eq!(output, expected);
}

#[tokio::test]
async fn no_entries_say_the_logs_were_not_found() {
    let server = MockServer::start().await;
    token(&server, 3600, 1).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(page(vec![], None))
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    let output = read(&reader, None).await;

    assert!(output.starts_with("no logs found for worker 'wkr20261017-440' in Cloud Logging"));
}

#[tokio::test]
async fn a_denied_read_names_the_view() {
    let server = MockServer::start().await;
    token(&server, 3600, 1).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "code": 403, "message": "Permission denied", "status": "PERMISSION_DENIED" }
        })))
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    let output = read(&reader, None).await;

    assert!(output.starts_with(&format!(
        "no access to the Cloud Logging view '{DEFAULT_VIEW}'"
    )));
    assert!(output.trim_end().ends_with("Permission denied"));
}

#[tokio::test]
async fn throttled_reads_are_retried() {
    let server = MockServer::start().await;
    token(&server, 3600, 1).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(page(vec![entry("0130-run-function", json!("done"))], None))
        .expect(1)
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    let output = read(&reader, None).await;

    assert!(output.ends_with("done\n"));
}

#[tokio::test]
async fn any_other_failure_is_a_backend_error() {
    let server = MockServer::start().await;
    token(&server, 3600, 1).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    let items: Vec<_> = reader
        .stream(ArchiveRequest {
            worker_id: WORKER,
            log_id: LOG_ID,
            started_on_millis: None,
        })
        .collect()
        .await;

    assert!(matches!(
        items.as_slice(),
        [Err(LogBridgeError::Backend { .. })]
    ));
}

#[tokio::test]
async fn the_token_is_reused_until_it_nears_expiry() {
    let server = MockServer::start().await;
    token(&server, 3600, 1).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(page(
            vec![entry("0130-run-function", json!("one"))],
            Some("next"),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(page(vec![entry("0130-run-function", json!("two"))], None))
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    read(&reader, None).await;
}

#[tokio::test]
async fn a_token_close_to_expiry_is_fetched_again() {
    let server = MockServer::start().await;
    token(&server, 30, 2).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(page(
            vec![entry("0130-run-function", json!("one"))],
            Some("next"),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(page(vec![entry("0130-run-function", json!("two"))], None))
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    read(&reader, None).await;
}

#[tokio::test]
async fn an_unauthorized_read_fetches_a_new_token_once() {
    let server = MockServer::start().await;
    token(&server, 3600, 2).await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(ResponseTemplate::new(401))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(LIST))
        .respond_with(page(vec![entry("0130-run-function", json!("done"))], None))
        .mount(&server)
        .await;
    let reader = reader(&server, ViewPlace::default()).await;

    let output = read(&reader, None).await;

    assert!(output.ends_with("done\n"));
}
