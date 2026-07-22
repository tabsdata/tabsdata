//
// Copyright 2026 Tabsdata Inc.
//

use chrono::Utc;
use cron_descriptor::cronparser::cron_expression_descriptor;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

use crate::client::tdkserver::render::report::helper::common::{
    ABSENT_CELL, STATUS_RUNNING, STATUS_STOPPED, UNGATHERED_CELL,
};
use crate::client::tdkserver::render::report::helper::table::{
    ReportColumn, ReportTable, STATUS_COLOR_SCHEME, render_tables,
};
use crate::core::system::exit::CommandResult;

fn pod_columns() -> [ReportColumn<'static>; 4] {
    [
        ReportColumn {
            header: "Name",
            color_scheme: None,
        },
        ReportColumn {
            header: "Status",
            color_scheme: Some(STATUS_COLOR_SCHEME),
        },
        ReportColumn {
            header: "Ready",
            color_scheme: None,
        },
        ReportColumn {
            header: "Age",
            color_scheme: None,
        },
    ]
}

fn job_columns() -> [ReportColumn<'static>; 4] {
    [
        ReportColumn {
            header: "Name",
            color_scheme: None,
        },
        ReportColumn {
            header: "Status",
            color_scheme: Some(STATUS_COLOR_SCHEME),
        },
        ReportColumn {
            header: "Completions",
            color_scheme: None,
        },
        ReportColumn {
            header: "Age",
            color_scheme: None,
        },
    ]
}

fn cronjob_columns() -> [ReportColumn<'static>; 5] {
    [
        ReportColumn {
            header: "Name",
            color_scheme: None,
        },
        ReportColumn {
            header: "Status",
            color_scheme: Some(STATUS_COLOR_SCHEME),
        },
        ReportColumn {
            header: "Expression",
            color_scheme: None,
        },
        ReportColumn {
            header: "Explanation",
            color_scheme: None,
        },
        ReportColumn {
            header: "Age",
            color_scheme: None,
        },
    ]
}

struct PodRow {
    name: String,
    status: String,
    ready: String,
    age: String,
}

impl PodRow {
    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.status.clone(),
            self.ready.clone(),
            self.age.clone(),
        ]
    }
}

struct JobRow {
    name: String,
    status: String,
    completions: String,
    age: String,
}

impl JobRow {
    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.status.clone(),
            self.completions.clone(),
            self.age.clone(),
        ]
    }
}

struct CronJobRow {
    name: String,
    status: String,
    expression: String,
    explanation: String,
    age: String,
}

impl CronJobRow {
    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.status.clone(),
            self.expression.clone(),
            self.explanation.clone(),
            self.age.clone(),
        ]
    }
}

pub fn cluster_status(pods: &[Pod], jobs: &[Job], cronjobs: &[CronJob]) -> CommandResult {
    let mut pod_rows: Vec<PodRow> = pods
        .iter()
        .filter(|pod| !owned_by_job(pod))
        .map(pod_row)
        .collect();
    pod_rows.sort_by(|left, right| left.name.cmp(&right.name));

    let mut job_rows: Vec<JobRow> = jobs.iter().map(job_row).collect();
    job_rows.sort_by(|left, right| left.name.cmp(&right.name));

    let mut cronjob_rows: Vec<CronJobRow> = cronjobs.iter().map(cronjob_row).collect();
    cronjob_rows.sort_by(|left, right| left.name.cmp(&right.name));

    let pod_columns = pod_columns();
    let job_columns = job_columns();
    let cronjob_columns = cronjob_columns();
    render_tables(&[
        ReportTable {
            title: "Pods",
            columns: &pod_columns,
            rows: pod_rows.iter().map(PodRow::cells).collect(),
        },
        ReportTable {
            title: "Jobs",
            columns: &job_columns,
            rows: job_rows.iter().map(JobRow::cells).collect(),
        },
        ReportTable {
            title: "CronJobs",
            columns: &cronjob_columns,
            rows: cronjob_rows.iter().map(CronJobRow::cells).collect(),
        },
    ])
}

fn owned_by_job(pod: &Pod) -> bool {
    pod.metadata
        .owner_references
        .as_ref()
        .is_some_and(|owners| owners.iter().any(|owner| owner.kind == "Job"))
}

fn pod_row(pod: &Pod) -> PodRow {
    PodRow {
        name: name_of(pod.metadata.name.as_ref()),
        status: pod
            .status
            .as_ref()
            .and_then(|status| status.phase.clone())
            .unwrap_or_else(|| UNGATHERED_CELL.to_owned()),
        ready: pod_ready(pod),
        age: age(pod.metadata.creation_timestamp.as_ref()),
    }
}

fn job_row(job: &Job) -> JobRow {
    JobRow {
        name: name_of(job.metadata.name.as_ref()),
        status: job_status(job),
        completions: job_completions(job),
        age: age(job.metadata.creation_timestamp.as_ref()),
    }
}

fn cronjob_row(cronjob: &CronJob) -> CronJobRow {
    let schedule = cronjob.spec.as_ref().map(|spec| spec.schedule.clone());
    CronJobRow {
        name: name_of(cronjob.metadata.name.as_ref()),
        status: cronjob_status(cronjob),
        expression: schedule
            .clone()
            .unwrap_or_else(|| UNGATHERED_CELL.to_owned()),
        explanation: schedule.map_or_else(|| UNGATHERED_CELL.to_owned(), cronjob_explanation),
        age: age(cronjob.metadata.creation_timestamp.as_ref()),
    }
}

fn name_of(name: Option<&String>) -> String {
    name.cloned().unwrap_or_else(|| UNGATHERED_CELL.to_owned())
}

fn pod_ready(pod: &Pod) -> String {
    match pod
        .status
        .as_ref()
        .and_then(|status| status.container_statuses.as_ref())
    {
        Some(statuses) => {
            let ready = statuses.iter().filter(|status| status.ready).count();
            format!("{ready}/{}", statuses.len())
        }
        None => "0/0".to_owned(),
    }
}

fn job_status(job: &Job) -> String {
    if let Some(conditions) = job
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
    {
        for condition in conditions {
            if condition.status == "True"
                && (condition.type_ == "Complete" || condition.type_ == "Failed")
            {
                return condition.type_.clone();
            }
        }
    }
    STATUS_RUNNING.to_owned()
}

fn job_completions(job: &Job) -> String {
    let succeeded = job
        .status
        .as_ref()
        .and_then(|status| status.succeeded)
        .unwrap_or(0);
    let total = job
        .spec
        .as_ref()
        .and_then(|spec| spec.completions)
        .unwrap_or(1);
    format!("{succeeded}/{total}")
}

fn cronjob_status(cronjob: &CronJob) -> String {
    let suspended = cronjob
        .spec
        .as_ref()
        .and_then(|spec| spec.suspend)
        .unwrap_or(false);
    if suspended {
        STATUS_STOPPED.to_owned()
    } else {
        STATUS_RUNNING.to_owned()
    }
}

fn cronjob_explanation(schedule: String) -> String {
    cron_expression_descriptor::get_description_cron(&schedule)
        .unwrap_or_else(|_| ABSENT_CELL.to_owned())
}

fn age(creation: Option<&Time>) -> String {
    let Some(creation) = creation else {
        return UNGATHERED_CELL.to_owned();
    };
    let seconds = Utc::now()
        .signed_duration_since(creation.0)
        .num_seconds()
        .max(0);
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86400 {
        format!("{}h", seconds / 3600)
    } else {
        format!("{}d", seconds / 86400)
    }
}
