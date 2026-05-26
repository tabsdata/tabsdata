//
// Copyright 2024 Tabs Data Inc.
//

//! Module that notifies the run status of a worker under Tabsdata system.

use crate::component::runner::RunnerError;
use crate::component::runner::RunnerError::{
    BadRequest, BadStatus, BrokenContent, InvalidMessageType,
};
use crate::launch::worker::{TabsDataWorker, Worker};
use http::HeaderMap;
use regex::Regex;
use reqwest::header::{HeaderName, HeaderValue};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::server::MessageAction::Notify;
use td_common::server::OrchestratorMessagePayload::{
    OrchestratorExceptionMessagePayload, OrchestratorRequestMessagePayload,
    OrchestratorResponseMessagePayload,
};
use td_common::server::{
    Callback, EXCEPTION_FILE, ExceptionMessagePayload, PayloadType, REQUEST_MESSAGE_FILE_PATTERN,
    RESPONSE_FILE, RESPONSE_FOLDER, ResponseMessagePayload, OrchestratorMessage, UNKNOWN_RUN,
};
use td_common::status::ExitStatus;
use tracing::{debug, info};

/// Notifies the execution result of a run worker.
#[allow(clippy::too_many_arguments)]
#[async_trait::async_trait]
pub trait WorkerNotifier: Debug {
    async fn notify(
        &self,
        worker: Option<&TabsDataWorker>,
        request_message: OrchestratorMessage,
        start: i64,
        end: Option<i64>,
        status: WorkerCallbackStatus,
        execution: u16,
        limit: Option<u16>,
        error: Option<String>,
    ) -> Result<bool, RunnerError>;
}

#[async_trait::async_trait]
impl WorkerNotifier for Callback {
    async fn notify(
        &self,
        worker: Option<&TabsDataWorker>,
        request_message: OrchestratorMessage,
        start: i64,
        end: Option<i64>,
        status: WorkerCallbackStatus,
        execution: u16,
        limit: Option<u16>,
        error: Option<String>,
    ) -> Result<bool, RunnerError> {
        let mut failed = false;

        fn hashmap_to_headers(input: HashMap<String, String>) -> Result<HeaderMap, RunnerError> {
            let mut header_map: HeaderMap = HeaderMap::new();
            for (key, value) in input {
                let header_name = HeaderName::from_str(&key)?;
                let header_value = HeaderValue::from_str(&value)?;
                header_map.insert(header_name, header_value);
            }
            Ok(header_map)
        }

        // ToDo: temporarily, we omit sending notifications for errors, and we wait either for final failure (Fail) or
        //       eventual success (Done). Thus, we currently do not notify to the API Server error statuses (Error).
        if matches!(status, WorkerCallbackStatus::Error) {
            info!(
                "Omitting notification of finalization for state {:?} of worker:: name: '{}' - id: '{}'",
                WorkerCallbackStatus::Error,
                match &worker {
                    Some(worker) => worker.describer().name().to_string(),
                    None => "No worker...".to_string(),
                },
                request_message.id,
            );
            return Ok(failed);
        };

        debug!(
            "Notifying worker status change:\n\
            - Worker:\n{}\n\
            - Request Message: {}\n\
            - Start Time: {}\n\
            - End Time: {}\n\
            - Status: {:?}\n\
            - Execution: {}\n\
            - Executions Limit: {}\n\
            - Error: {:?}",
            match &worker {
                Some(worker) => worker.describer().to_string(),
                None => "No worker...".to_string(),
            },
            serde_yaml::to_string(&request_message)?,
            start,
            end.map(|v| v.to_string()).unwrap_or("-1".to_string()),
            status,
            execution,
            limit.map(|v| v.to_string()).unwrap_or("-1".to_string()),
            error
        );
        match self {
            Callback::Http(callback) => {
                let request_payload = match request_message.payload {
                    OrchestratorRequestMessagePayload(payload) => payload,
                    OrchestratorResponseMessagePayload(_) | OrchestratorExceptionMessagePayload(_) => {
                        return Err(InvalidMessageType);
                    }
                };

                let exception_payload = match worker {
                    None => ExceptionMessagePayload::default(),
                    Some(worker) => {
                        if matches!(status, WorkerCallbackStatus::Running) {
                            ExceptionMessagePayload::default()
                        } else {
                            let exception_file = worker
                                .describer()
                                .work()
                                .join(RESPONSE_FOLDER)
                                .join(EXCEPTION_FILE);
                            if exception_file.exists() {
                                let exception_message = OrchestratorMessage::try_from((
                                    exception_file.clone(),
                                    PayloadType::Exception,
                                ))?;
                                let exception_payload: &ExceptionMessagePayload<Value> =
                                    match &exception_message.payload {
                                        OrchestratorRequestMessagePayload(_)
                                        | OrchestratorResponseMessagePayload(_) => {
                                            return Err(InvalidMessageType);
                                        }
                                        OrchestratorExceptionMessagePayload(payload) => payload,
                                    };
                                exception_payload.clone()
                            } else {
                                ExceptionMessagePayload::default()
                            }
                        }
                    }
                };

                let response_payload = match worker {
                    None => ResponseMessagePayload::default(),
                    Some(worker) => {
                        if matches!(status, WorkerCallbackStatus::Running) {
                            ResponseMessagePayload::default()
                        } else {
                            let response_file = worker
                                .describer()
                                .work()
                                .join(RESPONSE_FOLDER)
                                .join(RESPONSE_FILE);
                            if response_file.exists() {
                                let response_message = OrchestratorMessage::try_from((
                                    response_file.clone(),
                                    PayloadType::Response,
                                ))?;
                                let response_payload: &ResponseMessagePayload<Value> =
                                    match &response_message.payload {
                                        OrchestratorRequestMessagePayload(_)
                                        | OrchestratorExceptionMessagePayload(_) => {
                                            return Err(InvalidMessageType);
                                        }
                                        OrchestratorResponseMessagePayload(payload) => payload,
                                    };
                                response_payload.clone()
                            } else {
                                ResponseMessagePayload::default()
                            }
                        }
                    }
                };
                let mut response_payload = ResponseMessagePayload {
                    id: request_message.id,
                    class: request_payload.class,
                    worker: request_payload.worker,
                    action: Notify,
                    start,
                    end,
                    status,
                    execution: execution as i16,
                    limit: limit.map(|x| x as i16),
                    error,
                    exception_kind: exception_payload.kind,
                    exception_message: exception_payload.message,
                    exception_error_code: exception_payload.error_code,
                    exit_status: exception_payload.exit_status,
                    ..response_payload
                };

                // Exit status 202 is using to signal work termination without further retry.
                if exception_payload.exit_status == ExitStatus::TabsDataError.code() {
                    response_payload.status = WorkerCallbackStatus::Failed;
                    failed = true;
                }

                debug!(
                    "Sending message payload:\n{}",
                    serde_json::to_string_pretty(&response_payload)?
                );

                let client = reqwest::Client::new();
                let mut request = client
                    .request(callback.method.clone(), callback.url.clone())
                    .headers(hashmap_to_headers(callback.headers.clone())?);
                if callback.body {
                    request = request.json(&response_payload);
                }
                let callback_response = request.send().await;
                match callback_response {
                    Ok(response) => {
                        debug!("Received a ok http response: {:?}", response);
                        if response.status().is_success() {
                            debug!("Received a successful http response: {:?}", response);
                            let body = response.text().await;
                            match body {
                                Ok(content) => {
                                    debug!(
                                        "Received a successful http response fine body content:\n'{}'",
                                        serde_json::to_string_pretty(&content).unwrap_or(content)
                                    )
                                }
                                Err(cause) => {
                                    debug!(
                                        "Received an successful http response bad body content: '{:?}'",
                                        cause
                                    );
                                    return Err(BrokenContent { cause });
                                }
                            }
                        } else {
                            let dbg = format!("{response:?}");
                            let status = response.status();
                            let body = response.text().await.map_or_else(
                                |_| String::default(),
                                |content| serde_json::to_string_pretty(&content).unwrap_or(content),
                            );
                            debug!("Received a non-successful http response: {dbg} - {body}");
                            return Err(BadStatus { status });
                        }
                    }
                    Err(e) => {
                        debug!("Received a not ok http response: {:?}", e);
                        return Err(BadRequest { cause: e });
                    }
                };
                Ok(failed)
            }
        }
    }
}

pub fn execution(message: &OrchestratorMessage) -> u16 {
    let regex = match Regex::new(REQUEST_MESSAGE_FILE_PATTERN) {
        Ok(re) => re,
        Err(_) => return UNKNOWN_RUN,
    };
    if let Some(file_name) = message.file.file_name().and_then(|f| f.to_str())
        && let Some(captures) = regex.captures(file_name)
        && let Some(run_str) = captures.get(2).map(|m| m.as_str())
        && let Ok(run) = run_str.parse::<u16>()
    {
        return run;
    }
    UNKNOWN_RUN
}
