//
// Copyright 2026 Tabs Data Inc.
//

use std::path::Path;
use std::path::PathBuf;

use clap_derive::Args;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;

use crate::base::constants::{FOLDER_IMAGES, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY};
use crate::base::error::cli::CliError;
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandResult, abort_with};
use crate::core::system::file::copy_tree;
use crate::runtime::instance::profile::{RenderedConfigmap, load_profile, resolve_kprofile_path};
use crate::runtime::provider::ClusterProvider;

#[derive(Debug, Clone, Args)]
pub struct GetArguments {
    #[arg(
        long,
        name = "provider",
        required = true,
        value_enum,
        long_help = "Provider whose templates are initialized: 'kminus', 'docker', 'eks' or 'fargate'."
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

    let step = reporter.step("Initializing tabsdata templates");
    let profile = match load_profile(provider, verbose) {
        Ok(profile) => profile,
        Err(error) => {
            step.fail();
            return Err(error.into());
        }
    };

    let initialize = || -> CommandResult {
        write_family(folder, KPROFILE_CONFIGS_FAMILY.folder, profile.configs)?;
        write_family(folder, KPROFILE_PODS_FAMILY.folder, profile.pods)?;
        copy_images(folder, verbose)
    };
    if let Err(error) = initialize() {
        step.fail();
        return Err(error);
    }

    step.done(&format!(
        "Initialized tabsdata templates in {}",
        folder.display()
    ));
    Ok(())
}

fn write_family(folder: &Path, family: &str, files: RenderedConfigmap) -> CommandResult {
    let family_folder = folder.join(family);
    std::fs::create_dir_all(&family_folder).map_err(|error| {
        TdError::from(CliError::FolderCreateFailed {
            folder: family_folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    for (key, value) in files {
        let file = family_folder.join(&key);
        std::fs::write(&file, value).map_err(|error| {
            TdError::from(CliError::FileWriteFailed {
                file: file.display().to_string(),
                cause: error.to_string(),
            })
        })?;
    }
    Ok(())
}

fn copy_images(folder: &Path, verbose: bool) -> CommandResult {
    let images_source = resolve_kprofile_path(verbose)?.join(FOLDER_IMAGES);
    if !images_source.is_dir() {
        return Ok(());
    }
    let images_target = folder.join(FOLDER_IMAGES);
    copy_tree(&images_source, &images_target).map_err(|cause| {
        TdError::from(CliError::FileWriteFailed {
            file: images_target.display().to_string(),
            cause,
        })
        .into()
    })
}
