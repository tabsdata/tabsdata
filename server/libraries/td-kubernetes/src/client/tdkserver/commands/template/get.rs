//
// Copyright 2026 Tabs Data Inc.
//

use std::path::Path;
use std::path::PathBuf;

use clap_derive::Args;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;

use crate::base::error::cli::CliError;
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandResult, abort_with};
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::ClusterProvider;

#[derive(Debug, Clone, Args)]
pub struct GetArguments {
    #[arg(
        long,
        name = "provider",
        required = true,
        value_enum,
        long_help = "Provider whose templates are rendered: 'kminus', 'docker', 'eks' or 'fargate'."
    )]
    pub provider: ClusterProvider,

    #[arg(
        long,
        name = "folder",
        required = true,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Empty target folder. One file per template config, the file name matching \
                     the config key (e.g. 'apiserver.yaml')."
    )]
    pub folder: PathBuf,
}

pub async fn command_get(arguments: GetArguments, globals: &GlobalArguments) -> CommandResult {
    let GetArguments { provider, folder } = arguments;
    let reporter = InteractiveReporter::new();
    get_templates(&reporter, provider, &folder, globals.verbose)
}

pub fn get_templates(
    reporter: &dyn Reporter,
    provider: ClusterProvider,
    folder: &Path,
    verbose: bool,
) -> CommandResult {
    if !folder.is_dir() {
        return abort_with(
            GeneralError,
            CliError::FolderNotFound(folder.display().to_string()),
        );
    }
    let mut entries = std::fs::read_dir(folder).map_err(|error| {
        TdError::from(CliError::FolderReadFailed {
            folder: folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    if entries.next().is_some() {
        return abort_with(
            GeneralError,
            CliError::FolderNotEmpty(folder.display().to_string()),
        );
    }

    let step = reporter.step("Rendering tabsdata templates");
    let profile = match load_profile(provider, verbose) {
        Ok(profile) => profile,
        Err(error) => {
            step.fail();
            return Err(error.into());
        }
    };

    let mut count = 0usize;
    for (key, value) in profile.configs {
        let file = folder.join(&key);
        if let Err(error) = std::fs::write(&file, value) {
            step.fail();
            return Err(TdError::from(CliError::FileWriteFailed {
                file: file.display().to_string(),
                cause: error.to_string(),
            })
            .into());
        }
        count += 1;
    }

    let noun = if count == 1 { "template" } else { "templates" };
    step.done(&format!(
        "Rendered {count} tabsdata {noun} to {}",
        folder.display()
    ));
    Ok(())
}
