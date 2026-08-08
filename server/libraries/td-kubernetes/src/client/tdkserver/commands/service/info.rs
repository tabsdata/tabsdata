//
// Copyright 2026 Tabsdata Inc.
//

use std::process::Command;

use clap_derive::{Args, Subcommand};
use td_common::status::ExitStatus::GeneralError;
use td_python::python::resolve_python_binary;

use crate::base::constants::PROGRAM_TDK;
use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandResult, abort_with};

const WIDE_ARGUMENT: &str = "wide";

const WIDE_FLAG: &str = "--wide";

#[derive(Debug, Clone, Copy, Subcommand)]
pub enum InfoCommands {
    #[command(
        name = "third-party",
        about = "Show the third-party dependencies of tabsdata"
    )]
    ThirdParty(ThirdPartyArguments),
    #[command(about = "Show the license of tabsdata")]
    License,
    #[command(
        name = "release-notes",
        about = "Show the release notes of the current version of tabsdata"
    )]
    ReleaseNotes,
}

#[derive(Debug, Clone, Copy, Args)]
pub struct ThirdPartyArguments {
    #[arg(
        long,
        name = WIDE_ARGUMENT,
        required = false,
        default_value_t = false,
        long_help = "Show the license column at its full width, instead of wrapping it."
    )]
    pub wide: bool,
}

impl InfoCommands {
    const fn subcommand(self) -> &'static str {
        match self {
            Self::ThirdParty(_) => "third-party",
            Self::License => "license",
            Self::ReleaseNotes => "release-notes",
        }
    }

    const fn flags(self) -> &'static [&'static str] {
        match self {
            Self::ThirdParty(arguments) if arguments.wide => &[WIDE_FLAG],
            _ => &[],
        }
    }
}

pub async fn command_info(command: InfoCommands, _globals: &GlobalArguments) -> CommandResult {
    let binary = resolve_python_binary(PROGRAM_TDK)?;

    let status = Command::new(binary)
        .arg("info")
        .arg(command.subcommand())
        .args(command.flags())
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
