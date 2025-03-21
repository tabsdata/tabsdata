//
// Copyright 2025 Tabs Data Inc.
//

use crate::logic::platform::resource::instance::{
    CAST_FOLDER, LOG_FOLDER, LOG_PATTERN, MESSAGE_PATTERN, PROC_FOLDER, WORK_FOLDER,
};
use glob::glob;
use std::env;
use std::path::PathBuf;
use td_common::server::WorkerClass::EPHEMERAL;
use td_common::server::WorkerName::FUNCTION;
use td_common::server::WORKSPACE_ENV;
use td_error::td_error;
use td_error::TdError;
use td_objects::datasets::dao::DsWorkerMessageWithNames;
use td_objects::datasets::dlo::WorkerLogPaths;
use td_tower::extractors::Input;

pub async fn resolve_worker_log_path(
    Input(message): Input<DsWorkerMessageWithNames>,
) -> Result<WorkerLogPaths, TdError> {
    let worker_path = env::var(WORKSPACE_ENV).map_err(ResolveWorkerLogPathError::EnvVar)?;
    let pattern = PathBuf::from(worker_path)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(EPHEMERAL.as_ref())
        .join(FUNCTION.as_ref())
        .join(WORK_FOLDER)
        .join(CAST_FOLDER)
        .join(format!("{}{}", message.id(), MESSAGE_PATTERN))
        .join(WORK_FOLDER)
        .join(LOG_FOLDER)
        .join(LOG_PATTERN);
    let pattern = pattern
        .to_str()
        .ok_or(ResolveWorkerLogPathError::EmptyPattern)?;

    let mut paths = Vec::new();
    for entry in glob(pattern).map_err(ResolveWorkerLogPathError::Pattern)? {
        match entry {
            Ok(path) => paths.push(path),
            Err(e) => Err(ResolveWorkerLogPathError::Glob(e))?,
        }
    }
    paths.sort();

    Ok(WorkerLogPaths(paths))
}

#[td_error]
enum ResolveWorkerLogPathError {
    #[error("Failed to resolve worker log path")]
    EnvVar(#[from] env::VarError) = 5000,
    #[error("Glob error trying to resolve log path")]
    Glob(#[from] glob::GlobError) = 5001,
    #[error("Pattern error trying to resolve log path")]
    Pattern(#[from] glob::PatternError) = 5002,
    #[error("Error trying to build Glob pattern to resolve log path")]
    EmptyPattern = 5003,
}
