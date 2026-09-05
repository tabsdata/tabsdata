//
// Copyright 2026 Tabsdata Inc.
//

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CheckState {
    #[serde(rename = "ok")]
    Passed,
    #[serde(rename = "ko")]
    Failed,
    #[serde(rename = "warn")]
    #[allow(dead_code)]
    Warning,
    #[serde(rename = "skip")]
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub struct AssessHelp {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

impl AssessHelp {
    pub fn plain(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            command: None,
        }
    }

    pub fn with_command(text: impl Into<String>, command: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            command: Some(command.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AssessCheck {
    pub concern: &'static str,
    pub check: String,
    pub state: CheckState,
    pub seconds: f64,
    pub code: Option<&'static str>,
    pub title: Option<String>,
    pub facts: Vec<String>,
    pub help: Vec<AssessHelp>,
    pub url: Option<String>,
}

impl AssessCheck {
    pub fn passed(concern: &'static str, check: impl Into<String>, seconds: f64) -> Self {
        Self {
            concern,
            check: check.into(),
            state: CheckState::Passed,
            seconds,
            code: None,
            title: None,
            facts: Vec::new(),
            help: Vec::new(),
            url: None,
        }
    }

    pub fn skipped(
        concern: &'static str,
        check: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            concern,
            check: check.into(),
            state: CheckState::Skipped,
            seconds: 0.0,
            code: None,
            title: Some(reason.into()),
            facts: Vec::new(),
            help: Vec::new(),
            url: None,
        }
    }
}
