//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::yaml::location::{Location, Locations};
use crate::types::basic::{CollectionName, FunctionName};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub mod cron;
pub mod cron_trigger;
pub mod function;
pub mod location;
pub mod stage_trigger;
pub mod stream;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum WorkerInput {
    Function(Box<function::FunctionInput>),
    CronTrigger(Box<cron_trigger::CronTriggerInput>),
    StageTrigger(Box<stage_trigger::StageTriggerInput>),
    /// Lean k8s streamer input (callback-free; the streamer reads its
    /// callbacks from env). Built by the resolve endpoint, fetched by
    /// the stream-publisher pod.
    Streamer(Box<stream::StreamerInput>),
    /// Lean k8s cron input (callback-free; the cron-publisher reads its
    /// trigger from env). Built by the resolve endpoint.
    Cron(Box<cron::CronInput>),
}

impl WorkerInput {
    pub fn collection(&self) -> &CollectionName {
        match self {
            WorkerInput::Function(i) => &i.info.collection,
            WorkerInput::CronTrigger(i) => &i.info.collection,
            WorkerInput::StageTrigger(i) => &i.info.collection,
            WorkerInput::Streamer(i) => &i.info.collection,
            WorkerInput::Cron(i) => &i.info.collection,
        }
    }

    pub fn function(&self) -> &FunctionName {
        match self {
            WorkerInput::Function(i) => &i.info.function,
            WorkerInput::CronTrigger(i) => &i.info.function,
            WorkerInput::StageTrigger(i) => &i.info.function,
            WorkerInput::Streamer(i) => &i.info.function,
            WorkerInput::Cron(i) => &i.info.function,
        }
    }
}

impl TryFrom<serde_yaml::Value> for WorkerInput {
    type Error = serde_yaml::Error;

    fn try_from(value: serde_yaml::Value) -> Result<Self, Self::Error> {
        serde_yaml::from_value(value)
    }
}

impl Locations for WorkerInput {
    fn locations(&self) -> Vec<&Location> {
        match self {
            WorkerInput::Function(i) => i.locations(),
            WorkerInput::CronTrigger(i) => i.locations(),
            WorkerInput::StageTrigger(i) => i.locations(),
            WorkerInput::Streamer(i) => i.locations(),
            WorkerInput::Cron(i) => i.locations(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum WorkerOutput {
    Function(function::FunctionOutput),
}
