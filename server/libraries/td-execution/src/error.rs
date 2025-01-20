//
// Copyright 2025 Tabs Data Inc.
//

use td_common::uri::Versions;
use td_error::td_error;

#[td_error]
pub enum ExecutionPlannerError {
    #[error("Relative version range {0}..{1} is not valid")]
    InvalidVersionRange(isize, isize) = 0,
    #[error("Relative version range {0} is decreasing and it should always be increasing: from the oldest version to newest one")]
    DecreasingVersionRange(Versions) = 1,

    #[error("Could not fetch data version, error: {0}")]
    CouldNotFetchDataVersion(#[source] sqlx::Error) = 5000,
    #[error("Could not fetch table, error: {0}")]
    CouldNotFetchTable(#[source] sqlx::Error) = 5001,
    #[error("Could not fetch function, error: {0}")]
    CouldNotFetchFunction(#[source] sqlx::Error) = 5002,
    #[error("Function ID was not found")]
    CouldNotFindFunctionId = 5003,
    #[error("Could not fetch scheduled execution plans, error: {0}")]
    CouldNotFetchScheduledExecutionPlan(#[source] sqlx::Error) = 5004,
    #[error("Could not insert execution plan, error: {0}")]
    CouldNotInsertExecutionPlan(#[source] sqlx::Error) = 5005,
    #[error("Could not insert transaction, error: {0}")]
    CouldNotInsertTransaction(#[source] sqlx::Error) = 5006,
    #[error("Dependency without target version")]
    DependencyWithoutTargetVersion = 5007,
    #[error("Missing execution template")]
    MissingExecutionTemplate = 5008,
    #[error("Error serializing execution plan with id: {0}, error: {1}")]
    CouldNotSerializeExecutionPlan(String, serde_json::Error) = 5009,
    #[error("Error deserializing execution plan with id: {0}, error: {1}")]
    CouldNotDeserializeExecutionPlan(String, serde_json::Error) = 5010,
}
