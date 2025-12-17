//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::params::{Params, Task};
use crate::scheduler::runner::{CronJobTask, CronScheduler};
use crate::scheduler::tasks::heartbeat::Heartbeat;
use crate::scheduler::tasks::http_request::HttpRequest;
use td_common::server_runner::ServerRunner;
use td_process::launcher::cli::{Cli, NoConfig};

mod scheduler;

const CONFIG_NAME: &str = "cron";

/// Run the cron scheduler. This function is the entry point for the `cron` binary.
pub fn run() {
    Cli::<NoConfig, Params>::exec_async(
        CONFIG_NAME,
        |_config, params| {
            let Params {
                mask,
                start,
                end,
                task,
            } = params;

            let task: Box<dyn CronJobTask> = match task {
                Task::Heartbeat { output } => Box::new(Heartbeat::new(output)),
                Task::HttpRequest {
                    method,
                    url,
                    headers,
                    body,
                } => {
                    let mut task = HttpRequest::new(method, url);
                    task.with_headers(headers);
                    if let Some(body) = body {
                        task.with_body(body);
                    }
                    Box::new(task)
                }
            };

            ServerRunner::default()
                .server(CronScheduler::new(mask, start, end, task))
                .run()
        },
        None,
        None,
    );
}
