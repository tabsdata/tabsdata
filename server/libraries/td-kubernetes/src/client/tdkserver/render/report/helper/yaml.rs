//
// Copyright 2026 Tabsdata Inc.
//

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::json;
use td_python::python::resolve_python_binary;

use crate::base::error::provider::ProviderError;
use crate::core::system::exit::CommandResult;

const DOCUMENT_RENDERER: &str = "tdk_yaml";

pub(crate) struct ReportDocument<'a> {
    pub title: &'a str,
    pub subtitle: Option<&'a str>,
    pub content: &'a str,
}

pub(crate) fn render_documents(documents: &[ReportDocument]) -> CommandResult {
    let binary = resolve_python_binary(DOCUMENT_RENDERER)?;
    let payload = json!(
        documents
            .iter()
            .map(|document| json!({
                "title": document.title,
                "subtitle": document.subtitle,
                "content": document.content,
            }))
            .collect::<Vec<_>>()
    );
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
