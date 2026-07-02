//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::json;
use td_python::python::resolve_python_binary;

use crate::base::error::provider::ProviderError;
use crate::core::system::exit::CommandResult;

const TABLE_RENDERER: &str = "tdk_table";

pub(crate) const STATUS_COLOR_SCHEME: &str = "status";

pub(crate) struct ReportColumn<'a> {
    pub header: &'a str,
    pub color_scheme: Option<&'a str>,
}

pub(crate) struct ReportTable<'a> {
    pub title: &'a str,
    pub columns: &'a [ReportColumn<'a>],
    pub rows: Vec<Vec<String>>,
}

pub(crate) fn render_tables(tables: &[ReportTable]) -> CommandResult {
    let binary = resolve_python_binary(TABLE_RENDERER)?;
    let payload = json!(
        tables
            .iter()
            .map(|table| json!({
                "title": table.title,
                "columns": table
                    .columns
                    .iter()
                    .map(|column| json!({
                        "name": column.header,
                        "color_scheme": column.color_scheme,
                    }))
                    .collect::<Vec<_>>(),
                "rows": table.rows,
            }))
            .collect::<Vec<_>>()
    );
    let mut child = Command::new(binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| ProviderError::TableRenderFailed(error.to_string()))?;
    child
        .stdin
        .take()
        .ok_or_else(|| {
            ProviderError::TableRenderFailed("could not open renderer stdin".to_owned())
        })?
        .write_all(payload.to_string().as_bytes())
        .map_err(|error| ProviderError::TableRenderFailed(error.to_string()))?;
    let status = child
        .wait()
        .map_err(|error| ProviderError::TableRenderFailed(error.to_string()))?;
    if !status.success() {
        return Err(
            ProviderError::TableRenderFailed(format!("renderer exited with status {status}")).into(),
        );
    }
    Ok(())
}
