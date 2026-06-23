//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Write;
use std::process::{Command, Stdio};

use chrono::Utc;
use colored::Colorize;
use comfy_table::modifiers::UTF8_SOLID_INNER_BORDERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use serde_json::json;
use td_python::python::resolve_python_binary;

const UNKNOWN: &str = "<unknown>";

const HEADERS: [&str; 4] = ["Name", "Status", "Ready", "Age"];
const STATUS_RENDERER: &str = "tdk_status";
const STATUS_COLUMN: usize = 1;

struct StatusRow {
    name: String,
    status: String,
    ready: String,
    age: String,
}

pub fn cluster_status(pods: &[Pod], jobs: &[Job]) {
    let mut pod_rows: Vec<StatusRow> = pods
        .iter()
        .filter(|pod| !owned_by_job(pod))
        .map(pod_row)
        .collect();
    pod_rows.sort_by(|left, right| left.name.cmp(&right.name));

    let mut job_rows: Vec<StatusRow> = jobs.iter().map(job_row).collect();
    job_rows.sort_by(|left, right| left.name.cmp(&right.name));

    if render_with_python(&[("Pods", &pod_rows), ("Jobs", &job_rows)]).is_some() {
        return;
    }
    render_with_rust(&[("Pods", &pod_rows), ("Jobs", &job_rows)]);
}

fn render_with_rust(tables: &[(&str, &Vec<StatusRow>)]) {
    for (title, rows) in tables {
        print!("{}", rust_table(title, rows));
    }
}

fn render_with_python(tables: &[(&str, &Vec<StatusRow>)]) -> Option<()> {
    let binary = resolve_python_binary(STATUS_RENDERER).ok()?;
    let payload = json!(
        tables
            .iter()
            .map(|(title, rows)| json!({
                "title": title,
                "columns": HEADERS,
                "status_column": STATUS_COLUMN,
                "rows": rows
                    .iter()
                    .map(|row| [&row.name, &row.status, &row.ready, &row.age])
                    .collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>()
    );
    let mut child = Command::new(binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .ok()?;
    child
        .stdin
        .take()?
        .write_all(payload.to_string().as_bytes())
        .ok()?;
    child.wait().ok()?.success().then_some(())
}

fn owned_by_job(pod: &Pod) -> bool {
    pod.metadata
        .owner_references
        .as_ref()
        .is_some_and(|owners| owners.iter().any(|owner| owner.kind == "Job"))
}

fn rust_table(title: &str, rows: &[StatusRow]) -> String {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_SOLID_INNER_BORDERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(HEADERS.iter().map(|header| {
            Cell::new(header)
                .fg(Color::Cyan)
                .add_attribute(Attribute::Bold)
        }));
    for row in rows {
        let status_color = status_color(&row.status);
        table.add_row(vec![
            Cell::new(&row.name),
            Cell::new(&row.status)
                .fg(status_color)
                .add_attribute(Attribute::Bold),
            Cell::new(&row.ready),
            Cell::new(&row.age),
        ]);
    }
    format!("\n{}\n{table}\n", title.magenta().bold())
}

fn status_color(status: &str) -> Color {
    match status {
        "Running" => Color::Green,
        "Complete" | "Succeeded" => Color::Blue,
        "Failed" => Color::Red,
        "Pending" => Color::Yellow,
        _ => Color::DarkGrey,
    }
}

fn pod_row(pod: &Pod) -> StatusRow {
    StatusRow {
        name: name_of(pod.metadata.name.as_ref()),
        status: pod
            .status
            .as_ref()
            .and_then(|status| status.phase.clone())
            .unwrap_or_else(|| UNKNOWN.to_owned()),
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
    name.cloned().unwrap_or_else(|| UNKNOWN.to_owned())
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
    "Running".to_owned()
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
        return UNKNOWN.to_owned();
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
