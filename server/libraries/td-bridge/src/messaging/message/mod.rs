//
// Copyright 2026 Tabs Data Inc.
//

pub mod cron_trigger;
pub mod function;
pub mod stage_trigger;

use crate::messaging::message::cron_trigger::CronRequest;
use crate::messaging::message::function::FunctionRequest;
use crate::messaging::message::stage_trigger::StageTriggerRequest;
use derive_new::new;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::path::PathBuf;
use td_common::worker::{ExceptionMessagePayload, RequestMessagePayload, ResponseMessagePayload};

#[derive(Debug, Clone, Eq, PartialEq, new, Serialize, Deserialize)]
pub struct OrchestratorMessage<T = Value>
where
    T: Clone,
{
    pub id: String,
    pub work: String,
    pub file: PathBuf,
    pub payload: OrchestratorMessagePayload<T>,
}

// The FunctionRequest variant has its own allow on the inner enum (rare path,
// boxed allocation isn't worth it); we adopt the same stance at this outer
// level for the same reason.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrchestratorMessagePayload<T = Value>
where
    T: Clone,
{
    OrchestratorRequestMessagePayload(RequestMessagePayload<T>),
    OrchestratorResponseMessagePayload(ResponseMessagePayload<T>),
    OrchestratorExceptionMessagePayload(ExceptionMessagePayload<T>),
    /// Kubernetes function-runner request (per-action enum). Never written to
    /// the supervisor file queue — only the kubernetes gateway produces/accepts
    /// it.
    OrchestratorFunctionRequestMessagePayload(FunctionRequest),
    /// Kubernetes stage-trigger request (per-action enum). Like the function
    /// request, never written to the supervisor file queue — only the
    /// kubernetes gateway produces/accepts it.
    OrchestratorStageTriggerRequestMessagePayload(StageTriggerRequest),
    /// Kubernetes cron-trigger request (per-action enum). Like the function
    /// request, never written to the supervisor file queue — only the
    /// kubernetes gateway produces/accepts it.
    OrchestratorCronRequestMessagePayload(CronRequest),
}
