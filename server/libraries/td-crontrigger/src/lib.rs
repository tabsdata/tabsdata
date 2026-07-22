//
// Copyright 2025 Tabsdata Inc.
//

mod cli;
mod task_runner;
#[allow(unused_imports, dead_code)]
pub(crate) use crate::task_runner::NoHeartbeat;
pub(crate) use crate::task_runner::Task;
pub(crate) use crate::task_runner::TaskRunner;
#[allow(unused_imports)]
pub(crate) use crate::task_runner::TaskRunnerError;

mod cron_task;
pub(crate) use crate::cron_task::CronTask;
#[allow(unused_imports)]
pub(crate) use crate::cron_task::CronTaskError;

mod heartbeat_task;
pub(crate) use crate::heartbeat_task::TouchFileTask;
#[allow(unused_imports)]
pub(crate) use crate::heartbeat_task::TouchFileTaskError;

mod http_task;
pub(crate) use crate::http_task::HttpTask;

pub use cli::run;
