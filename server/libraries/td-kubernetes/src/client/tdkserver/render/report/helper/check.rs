//
// Copyright 2026 Tabsdata Inc.
//

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::json;
use td_python::python::resolve_python_binary;

use crate::base::error::provider::ProviderError;
use crate::core::system::exit::CommandResult;

const CHECK_RENDERER: &str = "tdk_check";

pub(crate) struct CheckEntry {
    pub kind: String,
    pub category: String,
    pub rule: CheckRule,
    pub files: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckRule {
    Mandatory,
    Optional,
    Delegated,
}

impl CheckRule {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mandatory => "mandatory",
            Self::Optional => "optional",
            Self::Delegated => "delegated",
        }
    }
}

pub(crate) struct CheckReport<'a> {
    pub instance: &'a str,
    pub stage: &'a str,
    pub folder: &'a str,
    pub pending: &'a [CheckEntry],
    pub provided: &'a [CheckEntry],
    pub all: bool,
    pub notes: &'a [String],
}

fn entries(entries: &[CheckEntry]) -> Vec<serde_json::Value> {
    entries
        .iter()
        .map(|entry| {
            json!({
                "kind": entry.kind,
                "category": entry.category,
                "rule": entry.rule.as_str(),
                "files": entry.files,
            })
        })
        .collect()
}

pub(crate) fn render_check(report: &CheckReport) -> CommandResult {
    let binary = resolve_python_binary(CHECK_RENDERER)?;
    let payload = json!({
        "instance": report.instance,
        "stage": report.stage,
        "folder": report.folder,
        "all": report.all,
        "pending": entries(report.pending),
        "provided": entries(report.provided),
        "notes": report.notes,
    });
    let mut child = Command::new(binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| ProviderError::DocumentRenderFailed(error.to_string()))?;
    child
        .stdin
        .take()
        .ok_or_else(|| {
            ProviderError::DocumentRenderFailed("could not open renderer stdin".to_owned())
        })?
        .write_all(payload.to_string().as_bytes())
        .map_err(|error| ProviderError::DocumentRenderFailed(error.to_string()))?;
    let status = child
        .wait()
        .map_err(|error| ProviderError::DocumentRenderFailed(error.to_string()))?;
    if !status.success() {
        return Err(ProviderError::DocumentRenderFailed(format!(
            "renderer exited with status {status}"
        ))
        .into());
    }
    Ok(())
}
