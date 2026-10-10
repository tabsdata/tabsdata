//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;

use clap_derive::Args;
use td_error::TdError;
use tracing::info;

use crate::base::constants::STUBS_FOLDER;
use crate::base::error::cli::CliError;
use crate::client::registry::stubs_root;
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::resolved_yes;
use crate::client::tdkserver::common::confirm;
use crate::core::system::exit::CommandResult;
use crate::core::system::file::copy_tree;
use crate::runtime::instance::profile::resolve_kprofile_path;

#[derive(Debug, Clone, Args)]
pub struct StubsArguments {
    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 10,
        long_help = "Skip the interactive confirmation prompt and replace an existing stubs \
                     folder immediately."
    )]
    pub yes: Option<bool>,
}

pub async fn command_stubs(arguments: StubsArguments, globals: &GlobalArguments) -> CommandResult {
    let settings = SettingsContext::settings();
    let yes = resolved_yes(arguments.yes, &settings);

    let source = resolve_kprofile_path(globals.verbose)
        .map_err(TdError::from)?
        .join(STUBS_FOLDER);
    if !source.is_dir() {
        return Err(TdError::from(CliError::FolderNotFound(source.display().to_string())).into());
    }

    let target = stubs_root()?;
    if target.exists() {
        let message = format!(
            "Folder '{}' already exists; it will be deleted and recreated. Proceed?",
            target.display()
        );
        if !confirm(yes.value, &message)? {
            info!("Operation cancelled.");
            return Ok(());
        }
        fs::remove_dir_all(&target).map_err(|error| {
            TdError::from(CliError::FolderRemoveFailed {
                folder: target.display().to_string(),
                cause: error.to_string(),
            })
        })?;
    }

    copy_tree(&source, &target).map_err(|cause| {
        TdError::from(CliError::FolderCreateFailed {
            folder: target.display().to_string(),
            cause,
        })
    })?;

    info!(
        "Copied the configuration stubs to folder '{}'. Fill one in and pass it to \
         'tdkserver quickstart' or 'tdkserver init' as '--config @FILE'.",
        target.display()
    );
    Ok(())
}
