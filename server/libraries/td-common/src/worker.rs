//
// Copyright 2024 Tabsdata Inc.
//

//! The supervisor↔worker file protocol: the request/response/exception YAML
//! payloads exchanged through a worker's work folder, and the names/patterns
//! of those files. (The orchestrator message envelope that carries these
//! payloads lives in `td-bridge::messaging`.)

use crate::execution_status::WorkerCallbackStatus;
use crate::logging::LOG_LOCATION;
use crate::status::ExitStatus;
use const_format::concatcp;
use http::Method;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::str::FromStr;
use strum::{AsRefStr, Display, EnumString};
use url::Url;
use utoipa::ToSchema;

pub const PROC_FOLDER: &str = "proc";
pub const WORK_FOLDER: &str = "work";
pub const CAST_FOLDER: &str = "cast";

// Worker-class folders under `proc/`, one per [`WorkerClass`].
pub const INIT_FOLDER: &str = "init";
pub const REGULAR_FOLDER: &str = "regular";
pub const EPHEMERAL_FOLDER: &str = "ephemeral";

pub const LOG_FOLDER: &str = LOG_LOCATION;

// The worker's log file stems.
pub const ERR_LOG_FILE_NAME: &str = "err";
pub const FN_LOG_FILE_NAME: &str = "fn";
pub const OUT_LOG_FILE_NAME: &str = "out";
pub const TD_LOG_FILE_NAME: &str = "td";

pub const REQUEST_FOLDER: &str = "request";
pub const RESPONSE_FOLDER: &str = "response";
pub const INPUT_FOLDER: &str = "input";
pub const OUTPUT_FOLDER: &str = "output";

pub const REQUEST_FILE: &str = "request.yaml";
pub const RESPONSE_FILE: &str = "response.yaml";
pub const EXCEPTION_FILE: &str = "exception.yaml";

pub const WORKER_PID_FILE: &str = "pid";
pub const WORKER_OK_FILE: &str = "ok";
pub const WORKER_OUT_FILE: &str = "out.log";
pub const WORKER_ERR_FILE: &str = "err.log";

pub const UNKNOWN_RUN: u16 = 0;
pub const INITIAL_RUN: u16 = 1;

pub const EXTENSION_DELIMITER: &str = ".";
pub const RETRIES_DELIMITER: &str = "_";
pub const INITIAL_CALL: &str = concatcp!(RETRIES_DELIMITER, INITIAL_RUN);

pub const REQUEST_MESSAGE_FILE_PATTERN: &str =
    concatcp!(r"^(.*)", RETRIES_DELIMITER, r"([1-9][0-9]*)(\.yaml$)");
pub const REQUEST_MESSAGE_FORMAT: &str = concatcp!("{}", RETRIES_DELIMITER, "{}", "{}");

#[derive(
    ToSchema, Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize, EnumString, AsRefStr,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum WorkerClass {
    Init,
    Regular,
    #[default]
    Ephemeral,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, EnumString, AsRefStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum WorkerName {
    Function,
    CronTrigger,
    StageTrigger,
}

#[derive(
    ToSchema,
    Default,
    Debug,
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    AsRefStr,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum MessageAction {
    #[default]
    Start,
    Stop,
    Notify,
}

/// Function decorator carried on a worker message so the orchestrator can act
/// on it before the worker resolves its full message (e.g. the kubernetes
/// gateway selecting a pod template). Wire-level mirror of the td-objects
/// `Decorator`; lives in td-common so the message envelope can reference it.
#[derive(
    ToSchema,
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    AsRefStr,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Decorator {
    Publisher,
    Transformer,
    Subscriber,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestMessagePayload<T = Value>
where
    T: Clone,
{
    #[serde(default)]
    pub class: WorkerClass,
    pub worker: String,
    pub worker_id: String,
    pub action: MessageAction,
    pub arguments: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback: Option<Callback>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<T>,
}

impl<T> RequestMessagePayload<T>
where
    T: Clone + Default,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        class: WorkerClass,
        worker: String,
        worker_id: String,
        action: MessageAction,
        arguments: Vec<String>,
        callback: Option<Callback>,
        context: Option<T>,
    ) -> Self {
        RequestMessagePayload {
            class,
            worker,
            worker_id,
            action,
            arguments,
            callback,
            context,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "")]
pub struct ExceptionMessagePayload<T = Value>
where
    T: Clone,
{
    #[serde(skip)]
    _type: PhantomData<T>,
    pub kind: Option<String>,
    pub message: Option<String>,
    pub error_code: Option<String>,
    #[serde(default = "default_exit_status")]
    pub exit_status: i32,
}

fn default_exit_status() -> i32 {
    ExitStatus::Success.code()
}

impl<T> Default for ExceptionMessagePayload<T>
where
    T: Clone + Default,
{
    fn default() -> Self {
        Self {
            _type: PhantomData::<T>,
            kind: None,
            message: None,
            error_code: None,
            exit_status: default_exit_status(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ResponseMessagePayload<T = Value>
where
    T: Clone,
{
    #[serde(default = "default_action")]
    pub action: MessageAction,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub status: WorkerCallbackStatus,
    pub error_message: Option<String>,
    #[serde(default = "default_exit_status")]
    pub exit_status: i32,
    pub context: Option<T>,
}

fn default_action() -> MessageAction {
    MessageAction::Notify
}

impl<T> Default for ResponseMessagePayload<T>
where
    T: Clone,
{
    fn default() -> Self {
        Self {
            action: default_action(),
            start: None,
            end: None,
            status: WorkerCallbackStatus::Done,
            error_message: None,
            exit_status: default_exit_status(),
            context: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Callback {
    Http(HttpCallback),
}

pub type HttpCallback = HttpRequest;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HttpRequest {
    pub url: Url,
    #[schema(value_type = String)]
    #[serde(
        serialize_with = "method_serialize",
        deserialize_with = "method_deserialize"
    )]
    pub method: Method,
    pub headers: HashMap<String, String>,
    pub body: bool,
}

fn method_serialize<S>(method: &Method, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(method.as_str())
}

fn method_deserialize<'de, D>(deserializer: D) -> Result<Method, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Method::from_str(&s).map_err(serde::de::Error::custom)
}
