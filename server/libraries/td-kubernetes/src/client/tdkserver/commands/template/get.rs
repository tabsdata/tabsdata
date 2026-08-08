//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;
use std::path::PathBuf;
use std::sync::LazyLock;

use clap_derive::Args;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;

use crate::base::constants::{
    FOLDER_IMAGES, FOREIGN_ASSETS, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY,
    KPROFILE_PODS_FAMILY,
};
use crate::base::error::cli::CliError;
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::{CommandResult, abort_with};
use crate::core::system::file::copy_tree;
use crate::runtime::instance::profile::{
    RenderedConfigmap, load_profile, resolve_foreign_path, resolve_kprofile_path,
};
use crate::runtime::provider::ClusterProvider;

const DISCOURAGED_NOTICE: &str = "\
# Editing this file is highly discouraged. It pins the versions tabsdata was built\n\
# and tested against, and changing them can break the instance in ways that are hard\n\
# to diagnose. Edit it only when a member of the tabsdata team suggests the change to\n\
# address a specific issue or bug.\n";

static PROVIDER_LONG_HELP: LazyLock<String> = LazyLock::new(|| {
    format!(
        "Provider whose templates are initialized: {}.",
        ClusterProvider::supported_names()
    )
});

#[derive(Debug, Clone, Args)]
pub struct GetArguments {
    #[arg(
        long,
        name = "provider",
        required = true,
        value_enum,
        long_help = PROVIDER_LONG_HELP.as_str()
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
        write_family(folder, KPROFILE_PODS_FAMILY.folder, profile.pods)?;
        write_family(folder, KPROFILE_ASSETS_FAMILY.folder, profile.assets)?;
        write_family(folder, KPROFILE_CONFIGS_FAMILY.folder, profile.configs)?;
        write_foreign_assets(folder, verbose)?;
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

fn write_foreign_assets(folder: &Path, verbose: bool) -> CommandResult {
    let assets_folder = folder.join(KPROFILE_ASSETS_FAMILY.folder);
    for foreign in FOREIGN_ASSETS {
        let source = resolve_foreign_path(*foreign, verbose)?;
        let content = std::fs::read_to_string(&source).map_err(|error| {
            TdError::from(CliError::FileReadFailed {
                file: source.display().to_string(),
                cause: error.to_string(),
            })
        })?;
        let file = assets_folder.join(foreign.asset());
        std::fs::write(&file, discouraged(&content)).map_err(|error| {
            TdError::from(CliError::FileWriteFailed {
                file: file.display().to_string(),
                cause: error.to_string(),
            })
        })?;
    }
    Ok(())
}

fn discouraged(content: &str) -> String {
    let mut lines = content.lines();
    let mut header = String::new();
    for line in lines.by_ref() {
        header.push_str(line);
        header.push('\n');
        if line.trim().is_empty() {
            break;
        }
    }
    let body: String = lines.map(|line| format!("{line}\n")).collect();
    format!("{header}{DISCOURAGED_NOTICE}\n{body}")
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

#[cfg(test)]
mod tests;
