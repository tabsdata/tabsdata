//
// Copyright 2026 Tabs Data Inc.
//

use chrono::Utc;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

use crate::client::tdkserver::render::report::helper::common::{STATUS_RUNNING, UNGATHERED_CELL};
use crate::client::tdkserver::render::report::helper::table::{
    ReportColumn, ReportTable, STATUS_COLOR_SCHEME, render_tables,
};
use crate::core::system::exit::CommandResult;

fn columns() -> [ReportColumn<'static>; 4] {
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

struct StatusRow {
    name: String,
    status: String,
    ready: String,
    age: String,
}

impl StatusRow {
    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.status.clone(),
            self.ready.clone(),
            self.age.clone(),
        ]
    }
}

pub fn cluster_status(pods: &[Pod], jobs: &[Job]) -> CommandResult {
    let mut pod_rows: Vec<StatusRow> = pods
        .iter()
        .filter(|pod| !owned_by_job(pod))
        .map(pod_row)
        .collect();
    pod_rows.sort_by(|left, right| left.name.cmp(&right.name));

    let mut job_rows: Vec<StatusRow> = jobs.iter().map(job_row).collect();
    job_rows.sort_by(|left, right| left.name.cmp(&right.name));

    let pod_columns = columns();
    let job_columns = columns();
    render_tables(&[
        ReportTable {
            title: "Pods",
            columns: &pod_columns,
            rows: pod_rows.iter().map(StatusRow::cells).collect(),
        },
        ReportTable {
            title: "Jobs",
            columns: &job_columns,
            rows: job_rows.iter().map(StatusRow::cells).collect(),
        },
    ])
}

fn owned_by_job(pod: &Pod) -> bool {
    pod.metadata
        .owner_references
        .as_ref()
        .is_some_and(|owners| owners.iter().any(|owner| owner.kind == "Job"))
}

fn pod_row(pod: &Pod) -> StatusRow {
    StatusRow {
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

fn job_row(job: &Job) -> StatusRow {
    StatusRow {
        name: name_of(job.metadata.name.as_ref()),
        status: job_status(job),
        ready: job_completions(job),
        age: age(job.metadata.creation_timestamp.as_ref()),
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
