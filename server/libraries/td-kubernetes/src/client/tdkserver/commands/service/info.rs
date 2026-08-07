//
// Copyright 2026 Tabsdata Inc.
//

use std::process::Command;

use clap_derive::Subcommand;
use td_common::status::ExitStatus::GeneralError;
use td_python::python::resolve_python_binary;

use crate::base::constants::PROGRAM_TDK;
use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandResult, abort_with};

#[derive(Debug, Clone, Copy, Subcommand)]
pub enum InfoCommands {
    #[command(
        name = "third-party",
        about = "Show the third-party dependencies of tabsdata"
    )]
    ThirdParty,
    #[command(about = "Show the license of tabsdata")]
    License,
    #[command(
        name = "release-notes",
        about = "Show the release notes of the current version of tabsdata"
    )]
    ReleaseNotes,
}

impl InfoCommands {
    const fn subcommand(self) -> &'static str {
        match self {
            Self::ThirdParty => "third-party",
            Self::License => "license",
            Self::ReleaseNotes => "release-notes",
        }
    }
}

pub async fn command_info(command: InfoCommands, _globals: &GlobalArguments) -> CommandResult {
    let binary = resolve_python_binary(PROGRAM_TDK)?;

    let status = Command::new(binary)
        .arg("info")
        .arg(command.subcommand())
        .status()
        .map_err(|error| ProviderError::InfoDelegationFailed(error.to_string()))?;

    if !status.success() {
        return abort_with(
            GeneralError,
            ProviderError::InfoDelegationFailed(format!(
                "'{PROGRAM_TDK} info' exited with status {status}"
            )),
        );
    }

    Ok(())
}
