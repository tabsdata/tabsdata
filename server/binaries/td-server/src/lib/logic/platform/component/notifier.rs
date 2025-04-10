//
// Copyright 2024 Tabs Data Inc.
//

//! Module that notifies the run status of a worker under Tabsdata system.

use crate::logic::platform::component::runner::RunnerError;
use crate::logic::platform::component::runner::RunnerError::{
    BadRequest, BadStatus, BrokenContent, InvalidMessageType,
};
use crate::logic::platform::launch::worker::{TabsDataWorker, Worker};
use crate::logic::platform::resource::instance::{RESPONSE_FILE, RESPONSE_FOLDER};
use http::HeaderMap;
use regex::Regex;
use reqwest::header::{HeaderName, HeaderValue};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use td_common::execution_status::ExecutionUpdateStatus;
use td_common::server::MessageAction::Notify;
use td_common::server::SupervisorMessagePayload::{
    SupervisorRequestMessagePayload, SupervisorResponseMessagePayload,
};
use td_common::server::{
    Callback, PayloadType, ResponseMessagePayload, SupervisorMessage, REQUEST_MESSAGE_FILE_PATTERN,
    UNKNOWN_RUN,
};
use tracing::{debug, info};

/// Notifies the execution result of a run worker.
#[allow(clippy::too_many_arguments)]
#[async_trait::async_trait]
pub trait WorkerNotifier: Debug {
    async fn notify(
        &self,
        worker: Option<&TabsDataWorker>,
        request_message: SupervisorMessage,
        start: i64,
        end: Option<i64>,
        status: ExecutionUpdateStatus,
        execution: u16,
        limit: Option<u16>,
        error: Option<String>,
    ) -> Result<(), RunnerError>;
}

#[async_trait::async_trait]
impl WorkerNotifier for Callback {
    async fn notify(
        &self,
        worker: Option<&TabsDataWorker>,
        request_message: SupervisorMessage,
        start: i64,
        end: Option<i64>,
        status: ExecutionUpdateStatus,
        execution: u16,
        limit: Option<u16>,
        error: Option<String>,
    ) -> Result<(), RunnerError> {
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
        //       eventual success (Done). Thus, we currently do not notify to the API Server error statuses (Error) .
        if matches!(status, ExecutionUpdateStatus::Error) {
            info!("Omitting notification of finalization for state {:?} of worker:: name: '{}' - id: '{}'",
                ExecutionUpdateStatus::Error,
                match &worker {
                    Some(worker) => worker.describer().name().to_string(),
                    None => "No worker...".to_string(),
                },
                request_message.id(),
            );
            return Ok(());
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
                let request_payload = match request_message.payload() {
                    SupervisorRequestMessagePayload(payload) => payload,
                    SupervisorResponseMessagePayload(_) => {
                        return Err(InvalidMessageType);
                    }
                };
                let mut response_payload = match worker {
                    None => ResponseMessagePayload::default(),
                    Some(worker) => {
                        if matches!(status, ExecutionUpdateStatus::Running) {
                            ResponseMessagePayload::default()
                        } else {
                            let response_file = worker
                                .describer()
                                .work()
                                .join(RESPONSE_FOLDER)
                                .join(RESPONSE_FILE);
                            if response_file.exists() {
                                let response_message = SupervisorMessage::try_from((
                                    response_file.clone(),
                                    PayloadType::Response,
                                ))?;
                                let response_payload: &ResponseMessagePayload<Value> =
                                    match response_message.payload() {
                                        SupervisorRequestMessagePayload(_) => {
                                            return Err(InvalidMessageType);
                                        }
                                        SupervisorResponseMessagePayload(payload) => payload,
                                    };
                                response_payload.clone()
                            } else {
                                ResponseMessagePayload::default()
                            }
                        }
                    }
                };
                let response_payload = response_payload
                    .set_id(request_message.id().clone())
                    .set_class(request_payload.class().clone())
                    .set_worker(request_payload.worker().clone())
                    .set_action(Notify)
                    .set_start(start)
                    .set_end(end)
                    .set_status(status)
                    .set_execution(execution)
                    .set_limit(limit)
                    .set_error(error);
                debug!(
                    "Sending message payload:\n{}",
                    serde_json::to_string_pretty(&response_payload)?
                );
                let client = reqwest::Client::new();
                let mut request = client
                    .request(callback.method().clone(), callback.url().clone())
                    .headers(hashmap_to_headers(callback.headers().clone())?);
                if *callback.body() {
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
                                    debug!("Received a successful http response fine body content:\n'{}'",
                                        serde_json::to_string_pretty(&content)?)
                                }
                                Err(cause) => {
                                    debug!("Received an successful http response bad body content: '{:?}'", cause);
                                    return Err(BrokenContent { cause });
                                }
                            }
                        } else {
                            debug!("Received a non-successful http response: {:?}", response);
                            return Err(BadStatus {
                                status: response.status(),
                            });
                        }
                    }
                    Err(e) => {
                        debug!("Received a not ok http response: {:?}", e);
                        return Err(BadRequest { cause: e });
                    }
                };
                Ok(())
            }
        }
    }
}

pub fn execution(message: &SupervisorMessage) -> u16 {
    let regex = match Regex::new(REQUEST_MESSAGE_FILE_PATTERN) {
        Ok(re) => re,
        Err(_) => return UNKNOWN_RUN,
    };
    if let Some(file_name) = message.file().file_name().and_then(|f| f.to_str()) {
        if let Some(captures) = regex.captures(file_name) {
            if let Some(run_str) = captures.get(2).map(|m| m.as_str()) {
                if let Ok(run) = run_str.parse::<u16>() {
                    return run;
                }
            }
        }
    }
    UNKNOWN_RUN
}
