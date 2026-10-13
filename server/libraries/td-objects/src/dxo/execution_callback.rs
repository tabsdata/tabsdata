//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::worker::CallbackRequest;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReportRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    op: Option<ReportOp>,
    #[serde(flatten)]
    pub report: CallbackRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum ReportOp {
    Report,
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::worker::ResponseMessagePayload;

    fn running() -> CallbackRequest {
        ResponseMessagePayload {
            status: WorkerCallbackStatus::Running,
            ..ResponseMessagePayload::default()
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
}
