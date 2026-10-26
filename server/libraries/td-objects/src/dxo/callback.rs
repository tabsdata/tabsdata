//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::worker_input::function::FunctionOutput;
use crate::types::basic::AtTime;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use td_common::execution_status::WorkerCallbackStatus;
use utoipa::ToSchema;

#[ta_objects::Dto]
#[serde(deny_unknown_fields)]
pub struct CallbackRequest {
    pub status: WorkerCallbackStatus,
    pub start: Option<AtTime>,
    pub end: Option<AtTime>,
    #[serde(default)]
    pub exit_status: i32,
    pub error_message: Option<String>,
    pub output: Option<FunctionOutput>,
}

/// What a function run's pod tells the server about its attempt. The body
/// names its operation in `op`; one without it is a status report.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(
    untagged,
    expecting = "a status report, or an operation named in `op` (`report`, `destination_write`)"
)]
pub enum ExecutionCallbackRequest {
    /// The attempt is about to write to its destination: from here on a
    /// re-run could write twice.
    DestinationWrite {
        op: DestinationWriteOp,
    },
    Report(ReportRequest),
}

impl ExecutionCallbackRequest {
    pub fn report(report: CallbackRequest) -> Self {
        Self::Report(ReportRequest { op: None, report })
    }

    pub fn destination_write() -> Self {
        Self::DestinationWrite {
            op: DestinationWriteOp::DestinationWrite,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DestinationWriteOp {
    DestinationWrite,
}

/// Read through a map because `flatten` would hand `report` only the fields
/// it names, bypassing its `deny_unknown_fields`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(try_from = "Map<String, Value>")]
pub struct ReportRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    op: Option<ReportOp>,
    #[serde(flatten)]
    pub report: CallbackRequest,
}

impl TryFrom<Map<String, Value>> for ReportRequest {
    type Error = serde_json::Error;

    fn try_from(mut body: Map<String, Value>) -> Result<Self, Self::Error> {
        let op = body.remove("op").map(serde_json::from_value).transpose()?;
        let report = serde_json::from_value(Value::Object(body))?;
        Ok(Self { op, report })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum ReportOp {
    Report,
}

/// What the server did with a worker's status report. A `Stale` report came
/// too late to change anything: its attempt was superseded, or its run or
/// worker had already moved past it. The worker stops reporting.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum CallbackOutcome {
    Applied,
    Stale,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct CallbackResponse {
    pub outcome: CallbackOutcome,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running() -> CallbackRequest {
        CallbackRequest {
            status: WorkerCallbackStatus::Running,
            start: None,
            end: None,
            exit_status: 0,
            error_message: None,
            output: None,
        }
    }

    fn read(body: serde_json::Value) -> Result<ExecutionCallbackRequest, serde_json::Error> {
        serde_json::from_value(body)
    }

    #[test]
    fn every_operation_reads_back_as_written() {
        for request in [
            ExecutionCallbackRequest::report(running()),
            ExecutionCallbackRequest::destination_write(),
        ] {
            let body = serde_json::to_value(&request).unwrap();
            assert_eq!(
                serde_json::to_value(read(body.clone()).unwrap()).unwrap(),
                body
            );
        }
    }

    #[test]
    fn a_body_with_or_without_the_report_operation_is_a_report() {
        let mut body = serde_json::to_value(running()).unwrap();
        assert!(matches!(
            read(body.clone()),
            Ok(ExecutionCallbackRequest::Report(_))
        ));
        body["op"] = "report".into();
        assert!(matches!(
            read(body),
            Ok(ExecutionCallbackRequest::Report(_))
        ));
    }

    #[test]
    fn an_unknown_operation_is_refused_even_with_a_status() {
        let mut body = serde_json::to_value(running()).unwrap();
        body["op"] = "teleport".into();
        let error = read(body).unwrap_err().to_string();
        assert!(error.contains("destination_write"), "{error}");
    }

    #[test]
    fn a_report_with_fields_the_contract_lacks_is_refused() {
        let mut body = serde_json::to_value(running()).unwrap();
        body["context"] = serde_json::json!({"Function": {"output": []}});
        assert!(read(body.clone()).is_err());
        assert!(serde_json::from_value::<CallbackRequest>(body).is_err());
    }

    #[test]
    fn the_runner_body_reads_as_a_report() {
        let body = serde_json::json!({
            "status": "Done",
            "start": 1700000000000i64,
            "end": 1700000001000i64,
            "exit_status": 0,
            "output": {"output": [{"NoData": {"table": "t"}}]}
        });
        let ExecutionCallbackRequest::Report(report) = read(body).unwrap() else {
            panic!("not a report")
        };
        assert_eq!(report.report.status, WorkerCallbackStatus::Done);
        assert!(report.report.output.is_some());
    }
}
