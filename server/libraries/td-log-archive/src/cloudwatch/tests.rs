//
// Copyright 2026 Tabsdata Inc.
//

use aws_sdk_cloudwatchlogs::error::ErrorMetadata;
use aws_sdk_cloudwatchlogs::operation::get_log_events::{GetLogEventsError, GetLogEventsOutput};
use aws_sdk_cloudwatchlogs::types::OutputLogEvent;
use aws_sdk_cloudwatchlogs::types::error::{InvalidParameterException, ResourceNotFoundException};
use aws_smithy_mocks::{Rule, RuleMode, mock, mock_client};
use futures::StreamExt;

use super::*;

const NAMESPACE: &str = "tabsdatatest";
const WORKER: &str = "wkr20260713-5";
const LOG_ID: &str = "pub-wkr20260713-5";

fn request() -> ArchiveRequest<'static> {
    ArchiveRequest {
        worker_id: WORKER,
        log_id: LOG_ID,
    }
}

fn record(container: &str, line: &str) -> String {
    serde_json::json!({
        "time": "2026-10-17T00:04:17.180769394Z",
        "stream": "stdout",
        "_p": "F",
        "log": line,
        "kubernetes": {
            "namespace_name": NAMESPACE,
            "container_name": container,
        },
    })
    .to_string()
}

fn events(lines: &[(&str, &str)], next_forward_token: &str) -> GetLogEventsOutput {
    GetLogEventsOutput::builder()
        .set_events(Some(
            lines
                .iter()
                .map(|(container, line)| {
                    OutputLogEvent::builder()
                        .message(record(container, line))
                        .build()
                })
                .collect(),
        ))
        .next_forward_token(next_forward_token)
        .build()
}

fn reader(rules: &[&Rule]) -> CloudWatchLogReader {
    let client = mock_client!(aws_sdk_cloudwatchlogs, RuleMode::Sequential, rules);
    CloudWatchLogReader::new(client, NAMESPACE)
}

async fn read(reader: &CloudWatchLogReader) -> String {
    let chunks: Vec<_> = reader.stream(request()).collect().await;
    chunks
        .into_iter()
        .map(|chunk| String::from_utf8(chunk.expect("chunk").to_vec()).expect("utf-8"))
        .collect()
}

fn header(container: &str) -> String {
    String::from_utf8(container_header(container).to_vec()).expect("utf-8")
}

#[test]
fn the_line_and_container_come_from_the_fluent_bit_record() {
    let archived = archived_line(&record("0130-run-function", "INFO started"));
    assert_eq!(archived.container.as_deref(), Some("0130-run-function"));
    assert_eq!(archived.line, "INFO started\n");
    assert_eq!(
        archived_line(&record("0130-run-function", "ends with newline\n")).line,
        "ends with newline\n"
    );
}

#[test]
fn a_log_field_holding_json_is_kept_verbatim() {
    let message = serde_json::json!({
        "log": "{\"level\":\"info\"}",
        "data": { "level": "info" },
    })
    .to_string();
    let archived = archived_line(&message);
    assert_eq!(archived.line, "{\"level\":\"info\"}\n");
    assert_eq!(archived.container, None);
}

#[test]
fn a_message_that_is_not_a_fluent_bit_record_is_kept_whole() {
    assert_eq!(archived_line("plain text").line, "plain text\n");
    assert_eq!(archived_line("{\"other\":1}").line, "{\"other\":1}\n");
}

#[tokio::test]
async fn the_worker_stream_is_read_with_a_header_whenever_the_container_changes() {
    let rule = mock!(Client::get_log_events)
        .match_requests(|request| {
            request.log_group_name() == Some("/tabsdata/tabsdatatest")
                && request.log_stream_name() == Some(LOG_ID)
                && request.start_from_head() == Some(true)
        })
        .sequence()
        .output(|| {
            events(
                &[
                    ("0010-venvloader", "installing"),
                    ("0010-venvloader", "installed"),
                    ("0130-run-function", "running"),
                ],
                "f/1",
            )
        })
        .output(|| events(&[], "f/1"))
        .build();
    let output = read(&reader(&[&rule])).await;
    let expected = [
        header("0010-venvloader"),
        "installing\ninstalled\n".to_owned(),
        header("0130-run-function"),
        "running\n".to_owned(),
    ]
    .concat();
    assert_eq!(output, expected);
}

#[tokio::test]
async fn the_stream_is_read_until_its_forward_token_stops_moving() {
    let rule = mock!(Client::get_log_events)
        .sequence()
        .output(|| events(&[("0130-run-function", "one")], "f/1"))
        .output(|| events(&[], "f/2"))
        .output(|| events(&[("0130-run-function", "two")], "f/3"))
        .output(|| events(&[], "f/3"))
        .build();
    let output = read(&reader(&[&rule])).await;
    assert_eq!(
        output,
        [header("0130-run-function"), "one\ntwo\n".to_owned()].concat()
    );
    assert_eq!(rule.num_calls(), 4);
}

#[tokio::test]
async fn a_container_continuing_on_the_next_page_keeps_its_header() {
    let rule = mock!(Client::get_log_events)
        .sequence()
        .output(|| events(&[("0130-run-function", "one")], "f/1"))
        .output(|| events(&[("0130-run-function", "two")], "f/2"))
        .output(|| events(&[], "f/2"))
        .build();
    let output = read(&reader(&[&rule])).await;
    assert_eq!(
        output,
        [header("0130-run-function"), "one\ntwo\n".to_owned()].concat()
    );
}

#[tokio::test]
async fn an_empty_stream_yields_a_not_found_message() {
    let rule = mock!(Client::get_log_events)
        .sequence()
        .output(|| events(&[], "f/0"))
        .output(|| events(&[], "f/0"))
        .build();
    let output = read(&reader(&[&rule])).await;
    assert!(output.starts_with("no logs found for worker 'wkr20260713-5'"));
    assert!(output.contains("'/tabsdata/tabsdatatest'"));
}

#[tokio::test]
async fn a_missing_stream_yields_a_not_found_message() {
    let rule = mock!(Client::get_log_events).then_error(|| {
        GetLogEventsError::ResourceNotFoundException(
            ResourceNotFoundException::builder()
                .message("The specified log stream does not exist.")
                .build(),
        )
    });
    let output = read(&reader(&[&rule])).await;
    assert!(output.starts_with("no logs found for worker 'wkr20260713-5'"));
}

#[tokio::test]
async fn a_denied_read_yields_an_access_message() {
    let rule = mock!(Client::get_log_events).then_error(|| {
        GetLogEventsError::generic(
            ErrorMetadata::builder()
                .code(ACCESS_DENIED_CODE)
                .message("not authorized to perform logs:GetLogEvents")
                .build(),
        )
    });
    let output = read(&reader(&[&rule])).await;
    assert!(output.contains("no access to CloudWatch log group '/tabsdata/tabsdatatest'"));
    assert!(output.contains("logs:GetLogEvents"));
}

#[tokio::test]
async fn any_other_failure_is_a_backend_error() {
    let rule = mock!(Client::get_log_events).then_error(|| {
        GetLogEventsError::InvalidParameterException(
            InvalidParameterException::builder()
                .message("bad parameter")
                .build(),
        )
    });
    let chunks: Vec<_> = reader(&[&rule]).stream(request()).collect().await;
    assert_eq!(chunks.len(), 1);
    assert!(matches!(chunks[0], Err(LogBridgeError::Backend { .. })));
}
