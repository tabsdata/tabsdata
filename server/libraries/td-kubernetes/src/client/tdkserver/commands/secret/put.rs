//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use clap_derive::Args;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;
use tracing::{info, warn};

use crate::base::constants::{
    CONFIGMAP_FAMILY_LABEL, KIND_SECRET_CONFIG, KPROFILE_CONFIGS_FAMILY, SECRET_FAMILY_CONFIG,
};
use crate::base::error::cli::CliError;
use crate::base::error::secret::SecretError;
use crate::base::naming::{id_from_file, name_from_kube_resource_name, to_kube_resource_name};
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::{CommandError, CommandResult, abort_with};
use crate::core::system::file::is_hidden_or_system;
use crate::kube::api::domain::labels::LabelFilter;
use crate::kube::api::entity::configmap::{get_configmap, list_configmaps};
use crate::kube::api::entity::secret::{get_secret, upsert_secret};
use crate::kube::context::KubeContext;
use crate::runtime::provider::{provider_for_or_tdkcontext, require_started_cluster};
use crate::runtime::secrets::discovery::extract_secret_leaves;
use crate::runtime::secrets::parser::parse_secret_manifest;
use crate::runtime::secrets::settler::{Settlement, settle_secret_keys};

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
        long_help = "Source folder containing one Secret manifest per file. \
                     Each file is a Kubernetes Secret YAML (as produced by `tdkserver secret get`). \
                     The Secret name from each manifest must match a Secret already in the cluster's \
                     'config' family; otherwise that file is skipped with a warning. \
                     File contents replace the live Secret entirely. Mutually exclusive with --file."
    )]
    pub folder: Option<PathBuf>,

    #[arg(
        long,
        name = "file",
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Single Secret manifest to upload. The Secret name from the manifest must match \
                     a Secret already in the cluster's 'config' family. File contents replace the \
                     live Secret entirely. Mutually exclusive with --folder."
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
    let context = provider.kube_context().await?;
    let known_configs = list_known_configs(&context).await?;
    let candidates = collect_candidates(&source)?;

    let mut warnings: Vec<String> = Vec::new();
    let mut replaced: Vec<(String, KeyBreakdown)> = Vec::new();
    let mut error: Option<CommandError> = None;

    for candidate in candidates {
        match candidate {
            Candidate::Skip(reason) => warnings.push(reason),
            Candidate::Process { file_name, path } => {
                match process_candidate(&context, &known_configs, &file_name, &path).await {
                    Ok(Outcome::Replaced(breakdown)) => replaced.push((file_name, breakdown)),
                    Ok(Outcome::Skipped(reason)) => warnings.push(reason),
                    Err(failure) => {
                        error = Some(failure);
                        break;
                    }
                }
            }
        }
    }

    report_summary(&replaced, &warnings);
    match error {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
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

async fn list_known_configs(context: &KubeContext) -> CommandResult<BTreeSet<String>> {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.kind);
    let configmaps = list_configmaps(context, Some(filter))
        .await?
        .into_iter()
        .map(|(name, _keys)| name)
        .collect();
    Ok(configmaps)
}

enum Candidate {
    Process { file_name: String, path: PathBuf },
    Skip(String),
}

struct KeyBreakdown {
    overwritten: Vec<String>,
    preserved: Vec<String>,
    created: Vec<String>,
}

enum Outcome {
    Replaced(KeyBreakdown),
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
    let parsed = parse_secret_manifest(path)?;
    let file_segment = match name_from_kube_resource_name(
        &parsed.name,
        KPROFILE_CONFIGS_FAMILY.scope,
        KIND_SECRET_CONFIG,
    ) {
        Ok(segment) => segment,
        Err(_) => {
            return Ok(Outcome::Skipped(format!(
                "Manifest '{file_name}' has Secret name '{}' which does not belong to a kprofile config secret; ignored.",
                parsed.name
            )));
        }
    };
    let id = id_from_file(file_segment);
    let configmap_name = to_kube_resource_name(
        KPROFILE_CONFIGS_FAMILY.scope,
        id,
        KPROFILE_CONFIGS_FAMILY.kind,
        file_segment,
    )?;
    if !known_configs.contains(&configmap_name) {
        return Ok(Outcome::Skipped(format!(
            "Manifest '{file_name}' targets Secret '{}' but no matching config '{configmap_name}' exists; ignored.",
            parsed.name
        )));
    }
    let config_file = file_segment.to_owned();
    let config_yaml = load_config_yaml(context, &configmap_name, &config_file).await?;
    let actual_keys: BTreeSet<String> = parsed.data.keys().cloned().collect();
    settle_secret_keys(&Settlement {
        config: &config_file,
        config_yaml: &config_yaml,
        secret: &parsed.name,
        actual_keys: Some(actual_keys),
    })?;
    let generator_keys = generated_keys_from_config(&config_file, &config_yaml)?;
    let live = get_secret(context, &parsed.name).await?;
    let MergeResult { data, breakdown } =
        merge_with_live(parsed.data, live.as_ref(), &generator_keys);
    upsert_secret(
        context,
        &parsed.name,
        data,
        parsed.labels,
        None,
        Some(SECRET_FAMILY_CONFIG),
        None,
    )
    .await?;
    Ok(Outcome::Replaced(breakdown))
}

struct MergeResult {
    data: BTreeMap<String, String>,
    breakdown: KeyBreakdown,
}

fn merge_with_live(
    manifest: BTreeMap<String, String>,
    live: Option<&BTreeMap<String, Vec<u8>>>,
    generator_keys: &BTreeSet<String>,
) -> MergeResult {
    let mut data: BTreeMap<String, String> = BTreeMap::new();
    let mut overwritten: Vec<String> = Vec::new();
    let mut preserved: Vec<String> = Vec::new();
    let mut created: Vec<String> = Vec::new();
    for (key, manifest_value) in manifest {
        let live_value = live.and_then(|map| map.get(&key));
        let live_text = live_value.and_then(|bytes| std::str::from_utf8(bytes).ok());
        let live_non_empty = matches!(live_text, Some(text) if !text.is_empty());
        let manifest_empty = manifest_value.is_empty();
        let is_generated = generator_keys.contains(&key);
        if is_generated && manifest_empty && live_non_empty {
            data.insert(key.clone(), live_text.unwrap().to_owned());
            preserved.push(key);
        } else if live_value.is_some() {
            data.insert(key.clone(), manifest_value);
            overwritten.push(key);
        } else {
            data.insert(key.clone(), manifest_value);
            created.push(key);
        }
    }
    MergeResult {
        data,
        breakdown: KeyBreakdown {
            overwritten,
            preserved,
            created,
        },
    }
}

fn generated_keys_from_config(
    config_file: &str,
    config_yaml: &str,
) -> CommandResult<BTreeSet<String>> {
    let mut yaml: serde_yaml::Value =
        serde_yaml::from_str(config_yaml).map_err(|error| SecretError::ConfigYamlParseFailed {
            config: config_file.to_owned(),
            cause: error.to_string(),
        })?;
    let leaves = extract_secret_leaves(config_file, &mut yaml)?;
    Ok(leaves
        .into_iter()
        .filter_map(|leaf| leaf.generator.map(|_| leaf.key))
        .collect())
}

async fn load_config_yaml(
    context: &KubeContext,
    configmap_name: &str,
    config_file: &str,
) -> CommandResult<String> {
    let data = get_configmap(context, configmap_name)
        .await?
        .ok_or_else(|| SecretError::ConfigYamlParseFailed {
            config: config_file.to_owned(),
            cause: format!("configmap '{configmap_name}' disappeared between listing and read"),
        })?;
    let yaml =
        data.get(config_file)
            .cloned()
            .ok_or_else(|| SecretError::ConfigYamlParseFailed {
                config: config_file.to_owned(),
                cause: format!("configmap '{configmap_name}' has no '{config_file}' entry"),
            })?;
    Ok(yaml)
}

fn report_summary(replaced: &[(String, KeyBreakdown)], warnings: &[String]) {
    for warning in warnings {
        warn!("{warning}");
    }
    for (name, breakdown) in replaced {
        info!("Replaced: {name}.");
        for key in &breakdown.created {
            info!("  - created:     {key}.");
        }
        for key in &breakdown.overwritten {
            info!("  - overwritten: {key}.");
        }
        for key in &breakdown.preserved {
            info!("  - preserved:   {key} (live non-empty, manifest empty).");
        }
    }
    let count = replaced.len();
    let noun = if count == 1 { "secret" } else { "secrets" };
    info!("Replaced {count} tabsdata {noun}.");
}

#[cfg(test)]
mod tests;
