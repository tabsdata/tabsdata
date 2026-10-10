//
// Copyright 2026 Tabsdata Inc.
//

mod engine;

use std::path::Path;

use crate::base::error::cli::CliError;

pub use engine::editor::Outcome;

pub struct Heading {
    pub instance: String,
    pub provider: String,
}

pub fn fill(
    file: &Path,
    heading: &Heading,
    validate: impl FnMut(&Path) -> Result<(), String>,
) -> Result<Outcome, CliError> {
    engine::editor::edit(file, heading, validate).map_err(|error| CliError::EditorFailed {
        file: file.display().to_string(),
        cause: error.to_string(),
    })
}

#[cfg(test)]
mod tests;
