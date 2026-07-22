//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use clap_derive::Args;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;
use tracing::{info, warn};

use crate::base::constants::{CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY};
use crate::base::error::cli::CliError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::{CommandError, CommandResult, abort_with};
use crate::core::system::file::is_hidden_or_system;
use crate::kube::api::{LabelFilter, get_configmaps, replace_configmap};
use crate::kube::context::KubeContext;
use crate::runtime::provider::{Provider, provider_for_or_tdkcontext, require_started_cluster};

#[derive(Debug, Clone, Args)]
#[command(group(
    clap::ArgGroup::new("source")
        .args(["folder", "file"])
        .required(true)
        .multiple(false),
))]
pub struct PutArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[arg(
        long,
        name = "folder",
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Source folder containing one file per config to update. \
                     Each file name must map (via the naming convention) to an existing \
                     config in the cluster; otherwise that file is skipped with a warning. \
                     File contents replace the configmap entry. Mutually exclusive with --file."
    )]
    pub folder: Option<PathBuf>,

    #[arg(
        long,
        name = "file",
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Single file to upload. The file name must map (via the naming \
                     convention) to an existing config in the cluster. File contents \
                     replace the configmap entry. Mutually exclusive with --folder."
    )]
    pub file: Option<PathBuf>,
}

pub enum PutSource {
    Folder(PathBuf),
    File(PathBuf),
}

impl PutSource {
    fn from_args(folder: Option<PathBuf>, file: Option<PathBuf>) -> Self {
        match (folder, file) {
            (Some(folder), _) => PutSource::Folder(folder),
            (_, Some(file)) => PutSource::File(file),
            _ => unreachable!("clap ArgGroup ensures exactly one of --folder/--file"),
        }
    }
}

pub async fn command_put(arguments: PutArguments, globals: &GlobalArguments) -> CommandResult {
    let PutArguments {
        cluster,
        instance,
        folder,
        file,
    } = arguments;
    let source = PutSource::from_args(folder, file);
    ensure_source(&source)?;
    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    require_started_cluster(provider.as_ref()).await?;
    put_configs(provider.as_ref(), &source).await
}

pub fn ensure_source(source: &PutSource) -> CommandResult {
    match source {
        PutSource::Folder(folder) => {
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
            if entries.next().is_none() {
                return abort_with(
                    GeneralError,
                    CliError::FolderEmpty(folder.display().to_string()),
                );
            }
        }
        PutSource::File(file) => {
            if !file.is_file() {
                return abort_with(
                    GeneralError,
                    CliError::FolderNotFound(file.display().to_string()),
                );
            }
        }
    }
    Ok(())
}

pub async fn put_configs(provider: &dyn Provider, source: &PutSource) -> CommandResult {
    let context = provider.kube_context().await?;
    let known_configs = list_known_configs(&context).await?;
    let candidates = collect_candidates(source)?;

    let mut warnings: Vec<String> = Vec::new();
    let mut replaced: Vec<String> = Vec::new();
    let mut error: Option<CommandError> = None;

    for candidate in candidates {
        match candidate {
            Candidate::Skip(reason) => warnings.push(reason),
            Candidate::Process { file_name, path } => {
                match process_candidate(&context, &known_configs, &file_name, &path).await {
                    Ok(Outcome::Replaced) => replaced.push(file_name),
                    Ok(Outcome::Skipped(reason)) => warnings.push(reason),
                    Err(failure) => {
                        error = Some(failure);
                        break;
                    }
                }
            }
        }
    }

    report_summary(&replaced, &warnings, "config");
    match error {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
}

fn report_summary(replaced: &[String], warnings: &[String], noun_singular: &str) {
    for warning in warnings {
        warn!("{warning}");
    }
    for name in replaced {
        info!("Replaced: {name}.");
    }
    let count = replaced.len();
    let noun = if count == 1 {
        noun_singular.to_owned()
    } else {
        format!("{noun_singular}s")
    };
    info!("Replaced {count} tabsdata {noun}.");
}

async fn list_known_configs(context: &KubeContext) -> CommandResult<BTreeSet<String>> {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.kind);
    let configs = get_configmaps(context, Some(filter))
        .await?
        .into_iter()
        .map(|(name, _entries)| name)
        .collect();
    Ok(configs)
}

enum Candidate {
    Process { file_name: String, path: PathBuf },
    Skip(String),
}

enum Outcome {
    Replaced,
    Skipped(String),
}

fn collect_candidates(source: &PutSource) -> CommandResult<Vec<Candidate>> {
    match source {
        PutSource::Folder(folder) => collect_folder_candidates(folder),
        PutSource::File(file) => Ok(vec![candidate_for_file(file)]),
    }
}

fn collect_folder_candidates(folder: &Path) -> CommandResult<Vec<Candidate>> {
    let reader = std::fs::read_dir(folder).map_err(|error| {
        TdError::from(CliError::FolderReadFailed {
            folder: folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    let mut entries: Vec<std::fs::DirEntry> = reader.filter_map(|entry| entry.ok()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let path = entry.path();
        if is_hidden_or_system(&path) {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            out.push(Candidate::Skip(format!(
                "Subfolder '{file_name}' ignored; expected a flat layout."
            )));
            continue;
        }
        out.push(Candidate::Process { file_name, path });
    }
    Ok(out)
}

fn candidate_for_file(path: &Path) -> Candidate {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Candidate::Skip(format!(
            "File '{}' has no readable name; ignored.",
            path.display()
        ));
    };
    Candidate::Process {
        file_name: file_name.to_owned(),
        path: path.to_path_buf(),
    }
}

async fn process_candidate(
    context: &KubeContext,
    known_configs: &BTreeSet<String>,
    file_name: &str,
    path: &Path,
) -> CommandResult<Outcome> {
    let id = id_from_file(file_name);
    let configmap_name = match to_kube_resource_name(
        KPROFILE_CONFIGS_FAMILY.scope,
        id,
        KPROFILE_CONFIGS_FAMILY.kind,
        file_name,
    ) {
        Ok(name) => name,
        Err(error) => {
            return Ok(Outcome::Skipped(format!(
                "File '{file_name}' does not map to a valid config name: {error}; ignored."
            )));
        }
    };
    if !known_configs.contains(&configmap_name) {
        return Ok(Outcome::Skipped(format!(
            "No config named '{configmap_name}' exists; ignored."
        )));
    }
    let bytes = std::fs::read(path).map_err(|error| {
        TdError::from(CliError::FileReadFailed {
            file: file_name.to_owned(),
            cause: error.to_string(),
        })
    })?;
    let text = String::from_utf8(bytes).map_err(|error| {
        TdError::from(CliError::FileNotUtf8 {
            file: file_name.to_owned(),
            cause: error.to_string(),
        })
    })?;
    let mut data: BTreeMap<String, String> = BTreeMap::new();
    data.insert(file_name.to_owned(), text);
    replace_configmap(
        context,
        &configmap_name,
        data,
        BTreeMap::new(),
        Some(KPROFILE_CONFIGS_FAMILY.kind),
    )
    .await?;
    Ok(Outcome::Replaced)
}
