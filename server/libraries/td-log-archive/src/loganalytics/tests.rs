//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use futures::StreamExt;
use tempfile::TempDir;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const WORKER: &str = "wkr20261017-440";
const LOG_ID: &str = "pub-wkr20261017-440";
const NAMESPACE: &str = "tabsdatashreyas";
const WORKSPACE: &str = "0f9c2b8e-1111-4222-8333-944455556666";
const CLIENT_ID: &str = "7b1c2d3e-aaaa-4bbb-8ccc-0d1e2f3a4b5c";
const TENANT_ID: &str = "11111111-2222-4333-8444-555555555555";
const FEDERATED_TOKEN: &str = "federated-token";
const TOKEN: &str = "/11111111-2222-4333-8444-555555555555/oauth2/v2.0/token";
const QUERY: &str = "/v1/workspaces/0f9c2b8e-1111-4222-8333-944455556666/query";
const COLUMNS: [&str; 4] = ["TimeGenerated", "_ItemId", "ContainerName", "LogMessage"];

struct Fixture {
    server: MockServer,
    _folder: TempDir,
    token_file: PathBuf,
}

impl Fixture {
    async fn start() -> Self {
        let server = MockServer::start().await;
        let folder = TempDir::new().unwrap();
        let token_file = folder.path().join("azure-identity-token");
        std::fs::write(&token_file, format!("{FEDERATED_TOKEN}\n")).unwrap();
        Self {
            server,
            _folder: folder,
            token_file,
        }
    }

    fn reader(&self, page_size: usize) -> LogAnalyticsReader {
        LogAnalyticsReader::new(
            Endpoints {
                authority: format!("{}/", self.server.uri()),
                query: format!("{}/v1/", self.server.uri()),
            },
            Identity {
                client_id: CLIENT_ID.to_owned(),
                tenant_id: TENANT_ID.to_owned(),
                token_file: self.token_file.clone(),
                subject: format!("system:serviceaccount:{NAMESPACE}:aut-apiserver"),
            },
            NAMESPACE,
            WORKSPACE,
            page_size,
            Duration::from_millis(1),
        )
        .expect("the client builds")
    }

    async fn token(&self, expires_in: u64, expected: u64) {
        Mock::given(method("POST"))
            .and(path(TOKEN))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "token_type": "Bearer",
                "access_token": "token",
                "expires_in": expires_in,
            })))
            .expect(expected)
            .mount(&self.server)
            .await;
    }

    async fn query(&self, response: ResponseTemplate) {
        Mock::given(method("POST"))
            .and(path(QUERY))
            .respond_with(response)
            .mount(&self.server)
            .await;
    }
}

fn rows(rows: Vec<Vec<Value>>) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "tables": [{
            "name": "PrimaryResult",
            "columns": COLUMNS.map(|name| json!({ "name": name, "type": "string" })),
            "rows": rows,
        }]
    }))
}

fn row(time: &str, item: &str, container: &str, message: Value) -> Vec<Value> {
    vec![json!(time), json!(item), json!(container), message]
}

fn failure(status: u16, message: &str, inner: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_json(json!({
        "error": {
            "code": "BadArgumentError",
            "message": message,
            "innererror": { "code": "SemanticError", "message": inner },
        }
    }))
}

async fn items(reader: &LogAnalyticsReader) -> Vec<Result<Bytes, LogBridgeError>> {
    reader
        .stream(ArchiveRequest {
            worker_id: WORKER,
            log_id: LOG_ID,
            started_on_millis: None,
        })
        .collect()
        .await
}

async fn read(reader: &LogAnalyticsReader) -> String {
    items(reader)
        .await
        .into_iter()
        .map(|item| String::from_utf8(item.expect("no backend error").to_vec()).expect("utf-8"))
        .collect()
}

async fn backend_error(reader: &LogAnalyticsReader) -> String {
    let items = items(reader).await;
    match items.as_slice() {
        [Err(LogBridgeError::Backend { cause })] => cause.clone(),
        other => panic!("expected one backend error, got {other:?}"),
    }
}

fn header_of(container: &str) -> String {
    String::from_utf8(container_header(container).to_vec()).unwrap()
}

fn environment(values: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let values: BTreeMap<String, String> = values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    move |name| values.get(name).cloned()
}

const WORKLOAD_IDENTITY: [(&str, &str); 4] = [
    (
        "AZURE_FEDERATED_TOKEN_FILE",
        "/var/run/secrets/azure/tokens/azure-identity-token",
    ),
    ("AZURE_CLIENT_ID", CLIENT_ID),
    ("AZURE_TENANT_ID", TENANT_ID),
    ("TD_WORKER_LOGS_WORKSPACE", WORKSPACE),
];

#[test]
fn a_workload_identity_and_a_workspace_turn_log_analytics_on() {
    let mut values = WORKLOAD_IDENTITY.to_vec();
    values.push(("POD_SERVICE_ACCOUNT", "aut-apiserver"));
    let (identity, workspace) =
        settings(NAMESPACE, environment(&values)).expect("Log Analytics is on");
    assert_eq!(workspace, WORKSPACE);
    assert_eq!(identity.client_id, CLIENT_ID);
    assert_eq!(identity.tenant_id, TENANT_ID);
    assert_eq!(
        identity.token_file,
        PathBuf::from("/var/run/secrets/azure/tokens/azure-identity-token")
    );
    assert_eq!(
        identity.subject,
        "system:serviceaccount:tabsdatashreyas:aut-apiserver"
    );
}

#[test]
fn any_missing_setting_leaves_it_off() {
    for missing in WORKLOAD_IDENTITY.map(|(name, _)| name) {
        let values: Vec<_> = WORKLOAD_IDENTITY
            .into_iter()
            .filter(|(name, _)| *name != missing)
            .collect();
        assert!(
            settings(NAMESPACE, environment(&values)).is_none(),
            "{missing} missing"
        );
    }
}

#[test]
fn the_authority_host_comes_from_the_webhook_with_one_trailing_slash() {
    let endpoints = Endpoints::from_lookup(environment(&[(
        "AZURE_AUTHORITY_HOST",
        "https://login.microsoftonline.us",
    )]));
    assert_eq!(endpoints.authority, "https://login.microsoftonline.us/");
    let endpoints = Endpoints::from_lookup(environment(&[]));
    assert_eq!(endpoints.authority, "https://login.microsoftonline.com/");
    assert_eq!(endpoints.query, "https://api.loganalytics.io/v1/");
}

#[test]
fn the_query_selects_the_worker_by_its_log_id_from_shortly_before_its_start() {
    let started_on = DateTime::parse_from_rfc3339("2026-10-17T10:00:00Z")
        .expect("date")
        .timestamp_millis();
    assert_eq!(
        rows_query(NAMESPACE, LOG_ID, Some(started_on), None, 5000),
        "ContainerLogV2\n\
         | where TimeGenerated >= datetime(2026-10-17T09:59:30.000Z)\n\
         | where PodNamespace == \"tabsdatashreyas\"\n\
         | where tostring(KubernetesMetadata.podLabels[\"tabsdata.com/log-id\"]) == \"pub-wkr20261017-440\"\n\
         | project TimeGenerated, _ItemId, ContainerName, LogMessage\n\
         | order by TimeGenerated asc, _ItemId asc\n\
         | take 5000"
    );
}

#[test]
fn an_unknown_start_leaves_the_time_open() {
    let query = rows_query(NAMESPACE, LOG_ID, None, None, 5000);
    assert!(!query.contains("TimeGenerated >"), "{query}");
}

#[test]
fn a_later_page_starts_after_the_last_row() {
    let cursor = Cursor {
        time: "2026-10-17T10:00:01.1234567Z".to_owned(),
        item: "b".to_owned(),
    };
    let query = rows_query(NAMESPACE, LOG_ID, None, Some(&cursor), 5000);
    assert!(
        query.contains(
            "| where TimeGenerated > datetime(2026-10-17T10:00:01.1234567Z) or \
             (TimeGenerated == datetime(2026-10-17T10:00:01.1234567Z) and _ItemId > \"b\")\n"
        ),
        "{query}"
    );
}

#[test]
fn only_kubernetes_names_reach_the_query() {
    assert!(is_kube_name("pub-wkr20261017-440"));
    assert!(is_kube_name("tabsdata.shreyas"));
    for name in ["", "Pub", "a\"b", "a b", "a|b"] {
        assert!(!is_kube_name(name), "{name}");
    }
}

#[tokio::test]
async fn an_invalid_log_id_is_refused_without_a_query() {
    let fixture = Fixture::start().await;
    let reader = fixture.reader(5000);
    let items: Vec<_> = reader
        .stream(ArchiveRequest {
            worker_id: WORKER,
            log_id: "pub\" or true",
            started_on_millis: None,
        })
        .collect()
        .await;
    assert!(
        matches!(items.as_slice(), [Err(LogBridgeError::Backend { cause })] if cause.contains("log id")),
        "{items:?}"
    );
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn the_token_is_requested_with_the_federated_assertion() {
    let fixture = Fixture::start().await;
    Mock::given(method("POST"))
        .and(path(TOKEN))
        .and(body_string_contains("grant_type=client_credentials"))
        .and(body_string_contains(format!("client_id={CLIENT_ID}")))
        .and(body_string_contains(
            "scope=https%3A%2F%2Fapi.loganalytics.io%2F.default",
        ))
        .and(body_string_contains(
            "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer",
        ))
        .and(body_string_contains(format!("client_assertion={FEDERATED_TOKEN}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "token",
            "expires_in": 3600,
        })))
        .expect(1)
        .mount(&fixture.server)
        .await;
    Mock::given(method("POST"))
        .and(path(QUERY))
        .and(header("authorization", "Bearer token"))
        .respond_with(rows(vec![row("t1", "a", "1000-main", json!("hello"))]))
        .expect(1)
        .mount(&fixture.server)
        .await;

    let output = read(&fixture.reader(5000)).await;
    assert!(output.ends_with("hello\n"), "{output}");
}

#[tokio::test]
async fn rows_are_written_with_a_header_whenever_the_container_changes() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture
        .query(rows(vec![
            row("t1", "a", "0010-venvloader", json!("runtime ready")),
            row("t2", "b", "0130-run-function", json!("iteration 1\n")),
            row("t3", "c", "0130-run-function", json!("iteration 2")),
            row("t4", "d", "1000-report-completion", json!("finished")),
        ]))
        .await;

    assert_eq!(
        read(&fixture.reader(5000)).await,
        format!(
            "{}runtime ready\n{}iteration 1\niteration 2\n{}finished\n",
            header_of("0010-venvloader"),
            header_of("0130-run-function"),
            header_of("1000-report-completion"),
        )
    );
}

#[tokio::test]
async fn a_json_log_message_is_written_back_as_json() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture
        .query(rows(vec![row(
            "t1",
            "a",
            "1000-main",
            json!({ "msg": "json line", "n": 1 }),
        )]))
        .await;

    let output = read(&fixture.reader(5000)).await;
    assert!(
        output.ends_with("{\"msg\":\"json line\",\"n\":1}\n"),
        "{output}"
    );
}

#[tokio::test]
async fn pages_are_read_until_one_comes_back_short() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path(QUERY))
        .and(body_string_contains("_ItemId > \\\"b\\\""))
        .respond_with(rows(vec![row("t2", "c", "1000-main", json!("three"))]))
        .with_priority(1)
        .expect(1)
        .mount(&fixture.server)
        .await;
    Mock::given(method("POST"))
        .and(path(QUERY))
        .respond_with(rows(vec![
            row("t1", "a", "1000-main", json!("one")),
            row("t2", "b", "1000-main", json!("two")),
        ]))
        .expect(1)
        .mount(&fixture.server)
        .await;

    assert_eq!(
        read(&fixture.reader(2)).await,
        format!("{}one\ntwo\nthree\n", header_of("1000-main"))
    );
}

#[tokio::test]
async fn a_full_last_page_ends_on_the_next_empty_one() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path(QUERY))
        .and(body_string_contains("_ItemId > \\\"b\\\""))
        .respond_with(rows(vec![]))
        .with_priority(1)
        .expect(1)
        .mount(&fixture.server)
        .await;
    Mock::given(method("POST"))
        .and(path(QUERY))
        .respond_with(rows(vec![
            row("t1", "a", "1000-main", json!("one")),
            row("t2", "b", "1000-main", json!("two")),
        ]))
        .expect(1)
        .mount(&fixture.server)
        .await;

    assert_eq!(
        read(&fixture.reader(2)).await,
        format!("{}one\ntwo\n", header_of("1000-main"))
    );
}

#[tokio::test]
async fn no_rows_say_the_logs_were_not_found() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture.query(rows(vec![])).await;

    let output = read(&fixture.reader(5000)).await;
    assert!(
        output.starts_with("no logs found for worker 'wkr20261017-440'"),
        "{output}"
    );
}

#[tokio::test]
async fn a_denied_read_names_the_workspace() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture
        .query(failure(
            403,
            "The provided credentials have insufficient access",
            "",
        ))
        .await;

    let output = read(&fixture.reader(5000)).await;
    assert!(
        output.starts_with(&format!(
            "no access to the Log Analytics workspace '{WORKSPACE}'"
        )),
        "{output}"
    );
    assert!(output.contains("insufficient access"), "{output}");
}

#[tokio::test]
async fn an_unknown_workspace_is_named() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture
        .query(failure(404, "Workspace could not be found", ""))
        .await;

    let cause = backend_error(&fixture.reader(5000)).await;
    assert!(
        cause.contains(&format!("'{WORKSPACE}' was not found")),
        "{cause}"
    );
}

#[tokio::test]
async fn missing_kubernetes_metadata_points_at_container_insights() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture
        .query(failure(
            400,
            "The request had some invalid properties",
            "Failed to resolve scalar expression named 'KubernetesMetadata'",
        ))
        .await;

    let cause = backend_error(&fixture.reader(5000)).await;
    assert!(cause.contains("metadata collection"), "{cause}");
    assert!(
        cause.contains("Failed to resolve scalar expression"),
        "{cause}"
    );
}

#[tokio::test]
async fn any_other_failure_is_a_backend_error_with_azure_s_message() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture
        .query(failure(
            400,
            "The request had some invalid properties",
            "Bad condition",
        ))
        .await;

    let cause = backend_error(&fixture.reader(5000)).await;
    assert!(
        cause.contains("400 Bad Request: The request had some invalid properties: Bad condition"),
        "{cause}"
    );
}

#[tokio::test]
async fn a_partial_result_is_a_backend_error() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture
        .query(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [],
            "error": { "code": "PartialError", "message": "There were some errors" },
        })))
        .await;

    let cause = backend_error(&fixture.reader(5000)).await;
    assert!(
        cause.contains("partial result: There were some errors"),
        "{cause}"
    );
}

#[tokio::test]
async fn throttled_reads_are_retried() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path(QUERY))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(2)
        .with_priority(1)
        .mount(&fixture.server)
        .await;
    fixture
        .query(rows(vec![row("t1", "a", "1000-main", json!("hello"))]))
        .await;

    let output = read(&fixture.reader(5000)).await;
    assert!(output.ends_with("hello\n"), "{output}");
}

#[tokio::test]
async fn the_token_is_reused_until_it_nears_expiry() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 1).await;
    fixture.query(rows(vec![])).await;

    let reader = fixture.reader(5000);
    read(&reader).await;
    read(&reader).await;
}

#[tokio::test]
async fn a_token_close_to_expiry_is_fetched_again() {
    let fixture = Fixture::start().await;
    fixture.token(30, 2).await;
    fixture.query(rows(vec![])).await;

    let reader = fixture.reader(5000);
    read(&reader).await;
    read(&reader).await;
}

#[tokio::test]
async fn an_unauthorized_read_fetches_a_new_token_once() {
    let fixture = Fixture::start().await;
    fixture.token(3600, 2).await;
    Mock::given(method("POST"))
        .and(path(QUERY))
        .respond_with(ResponseTemplate::new(401))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&fixture.server)
        .await;
    fixture
        .query(rows(vec![row("t1", "a", "1000-main", json!("hello"))]))
        .await;

    let output = read(&fixture.reader(5000)).await;
    assert!(output.ends_with("hello\n"), "{output}");
}

#[tokio::test]
async fn a_refused_token_names_the_client_and_the_federated_subject() {
    let fixture = Fixture::start().await;
    Mock::given(method("POST"))
        .and(path(TOKEN))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "invalid_request",
            "error_description": "AADSTS70021: No matching federated identity record found",
        })))
        .mount(&fixture.server)
        .await;

    let cause = backend_error(&fixture.reader(5000)).await;
    assert!(cause.contains(CLIENT_ID), "{cause}");
    assert!(
        cause.contains("system:serviceaccount:tabsdatashreyas:aut-apiserver"),
        "{cause}"
    );
    assert!(cause.contains("AADSTS70021"), "{cause}");
}

#[tokio::test]
async fn a_missing_federated_token_file_is_a_backend_error() {
    let fixture = Fixture::start().await;
    std::fs::remove_file(&fixture.token_file).unwrap();

    let cause = backend_error(&fixture.reader(5000)).await;
    assert!(cause.contains("reading the federated token"), "{cause}");
}
