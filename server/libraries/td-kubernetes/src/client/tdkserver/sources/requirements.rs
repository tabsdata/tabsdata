//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;
use std::str::FromStr;

use pep508_rs::{Requirement, VerbatimUrl};
use td_error::TdError;

use crate::base::constants::{KPROFILE_ASSETS_FAMILY, VENVS};
use crate::base::error::cli::CliError;
use crate::core::system::exit::CommandResult;

pub const REQUIREMENTS_ARGUMENT: &str = "requirements";

pub const REQUIREMENTS_HELP: &str = "Complementary packages for every tabsdata base virtual \
     environment, in pip requirements format. Run with '--help' for details.";

pub const REQUIREMENTS_LONG_HELP: &str = "Complementary packages for every tabsdata base \
     virtual environment, in pip requirements format. The file is checked to be a valid pip \
     requirements file before the instance folder is created, and its requirements are added \
     to the requirements-*.txt init assets of the new instance, after the ones those already \
     hold, so the virtual environments are built with them when the instance is created. \
     Nothing else is checked: a package that cannot be installed fails the creation.\n\n\
     Example:\n  \
     tdkserver quickstart --requirements drivers.txt";

const REQUIREMENTS_OPTION_PREFIX: char = '-';

pub fn read_requirements(source: &Path) -> CommandResult<String> {
    std::fs::read_to_string(source).map_err(|error| {
        TdError::from(CliError::FileReadFailed {
            file: source.display().to_string(),
            cause: error.to_string(),
        })
        .into()
    })
}

pub fn validate_requirements(source: &Path, contents: &str) -> CommandResult {
    for (index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = match line.split_once(" #") {
            Some((requirement, _)) => requirement.trim(),
            None => line,
        };
        if line.starts_with(REQUIREMENTS_OPTION_PREFIX) {
            continue;
        }
        if let Err(error) = Requirement::<VerbatimUrl>::from_str(line) {
            return Err(TdError::from(CliError::RequirementsInvalid {
                file: source.display().to_string(),
                line: index + 1,
                cause: error.to_string(),
            })
            .into());
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ExtraRequirements {
    source: String,
    contents: String,
}

impl ExtraRequirements {
    pub fn load(source: &Path) -> CommandResult<Self> {
        let contents = read_requirements(source)?;
        validate_requirements(source, &contents)?;
        Ok(Self {
            source: source.display().to_string(),
            contents,
        })
    }

    pub fn merge(&self, init_folder: &Path) -> CommandResult {
        let assets = init_folder.join(KPROFILE_ASSETS_FAMILY.folder);
        for venv in VENVS {
            let target = assets.join(venv.requirements_asset());
            let base = read_requirements(&target)?;
            std::fs::write(&target, self.merged(&base)).map_err(|error| {
                TdError::from(CliError::FileWriteFailed {
                    file: target.display().to_string(),
                    cause: error.to_string(),
                })
            })?;
        }
        Ok(())
    }

    fn merged(&self, base: &str) -> String {
        let mut merged = base.to_owned();
        if !merged.is_empty() && !merged.ends_with('\n') {
            merged.push('\n');
        }
        merged.push_str(&format!(
            "\n# From --{REQUIREMENTS_ARGUMENT} {}\n",
            self.source
        ));
        merged.push_str(&self.contents);
        if !merged.ends_with('\n') {
            merged.push('\n');
        }
        merged
    }
}

#[cfg(test)]
mod tests;
