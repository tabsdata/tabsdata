//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::yaml::location::{Location, Locations};
use crate::types::basic::{CollectionName, FunctionName};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub mod cron;
pub mod function_runner;
pub mod location;
pub mod watchdog;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkerInput {
    FunctionV2(Box<function_runner::v2::FunctionInput>),
    CronV1(Box<cron::v1::CronInput>),
    WatchdogV1(Box<watchdog::v1::WatchdogInput>),
}

impl WorkerInput {
    pub fn collection(&self) -> &CollectionName {
        match self {
            WorkerInput::FunctionV2(i) => &i.info.collection,
            WorkerInput::CronV1(i) => &i.info.collection,
            WorkerInput::WatchdogV1(i) => &i.info.collection,
        }
    }

    pub fn function(&self) -> &FunctionName {
        match self {
            WorkerInput::FunctionV2(i) => &i.info.function,
            WorkerInput::CronV1(i) => &i.info.function,
            WorkerInput::WatchdogV1(i) => &i.info.function,
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
            WorkerInput::FunctionV2(i) => i.locations(),
            WorkerInput::CronV1(i) => i.locations(),
            WorkerInput::WatchdogV1(i) => i.locations(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum WorkerOutput {
    // V2 kept for backwards compatibility (in case one worker response was left before upgrading)
    V2(function_runner::v2::FunctionOutput),
    FunctionV2(function_runner::v2::FunctionOutput),
    CronV1(Box<cron::v1::CronOutput>),
    WatchdogV1(Box<watchdog::v1::WatchdogOutput>),
}
