//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as SystemCommand;

use chrono::Utc;
use td_python::python::resolve_python_binary;
use td_python::tabsdata::tabsdata_version;
use tracing::debug;

use crate::base::constants::{
    CONFIGMAP_FAMILY_LABEL, Family, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY,
    KPROFILE_PODS_FAMILY, PROGRAM_TD_RESOURCE, RESOURCE_KPROFILE,
};
use crate::base::error::profile::ProfileError;
use crate::core::system::file::is_hidden_or_system;
use crate::core::system::subprocess;
use crate::core::system::subprocess::apply_detached_flags;
use crate::kube::api::{LabelFilter, get_configmaps};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::resolver::ConfigurationResolver;
use crate::runtime::instance::manifest::{MANIFEST_FILE, Manifest, deserialize_manifest};
use crate::runtime::instance::profile::bundle::{ParsedFileName, parse_filename, pick_winner};
use crate::runtime::provider::ClusterProvider;

pub type RenderedConfigmap = BTreeMap<String, String>;

pub struct Profile {
    pub pods: RenderedConfigmap,
    pub assets: RenderedConfigmap,
    pub configs: RenderedConfigmap,
    pub manifest: Manifest,
}

const CONFIGMAP_FAMILIES: &[&Family] = &[
    &KPROFILE_PODS_FAMILY,
    &KPROFILE_ASSETS_FAMILY,
    &KPROFILE_CONFIGS_FAMILY,
];

const SCHEMA_EXTENSION: &str = ".yamls";
const SCHEMA_SOURCE_EXTENSION: &str = ".schema.yaml";
const SCHEMA_SOURCE_MARKER: &str = ".schema";
const SCHEMA_SOURCE_SUFFIX: &str = ".yaml";

fn normalize_schema_extension(file_name: &str) -> String {
    match file_name.strip_suffix(SCHEMA_SOURCE_EXTENSION) {
        Some(stem) => format!("{stem}{SCHEMA_EXTENSION}"),
        None => file_name.to_owned(),
    }
}

fn normalize_schema_name(parsed: ParsedFileName) -> ParsedFileName {
    if parsed.extensions != SCHEMA_SOURCE_SUFFIX {
        return parsed;
    }
    match parsed.name.strip_suffix(SCHEMA_SOURCE_MARKER) {
        Some(stem) => ParsedFileName {
            name: stem.to_owned(),
            tokens: parsed.tokens,
            extensions: SCHEMA_EXTENSION.to_owned(),
        },
        None => parsed,
    }
}

pub fn resolve_kprofile_path(verbose: bool) -> Result<PathBuf, ProfileError> {
    let resolver = resolve_python_binary(PROGRAM_TD_RESOURCE).map_err(|error| {
        ProfileError::ResourceBinaryNotFound(PROGRAM_TD_RESOURCE.to_owned(), error.to_string())
    })?;
    let resolver_display = resolver.display().to_string();
    let command_label = format!("{resolver_display} {RESOURCE_KPROFILE}");
    let mut command = SystemCommand::new(&resolver);
    command.arg(RESOURCE_KPROFILE);
    apply_detached_flags(&mut command);
    let output = subprocess::run(&mut command, PROGRAM_TD_RESOURCE, verbose).map_err(|error| {
        ProfileError::ResourceBinaryFailed(command_label.clone(), error.to_string())
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        return Err(ProfileError::ResourceBinaryStatus(
            command_label,
            output.status.to_string(),
            stderr,
        ));
    }
    let stdout = String::from_utf8(output.stdout)
        .map_err(|_| ProfileError::ResourceBinaryNonUtf8Output(resolver_display.clone()))?;
    let path = PathBuf::from(stdout.trim());
    if !path.is_dir() {
        return Err(ProfileError::KprofilesPathInvalid(
            path.display().to_string(),
        ));
    }
    Ok(path)
}

struct DiscoveryFile {
    file: String,
    parsed: ParsedFileName,
    content: String,
}

pub fn load_profile(provider: ClusterProvider, verbose: bool) -> Result<Profile, ProfileError> {
    let root = resolve_kprofile_path(verbose)?;
    let tabsdata_version = tabsdata_version()
        .map_err(|error| ProfileError::TabsdataVersionResolveFailed(error.to_string()))?;
    let axes = provider.axes();
    let preferred = provider.preferred_values();

    let mut maps: BTreeMap<&'static str, RenderedConfigmap> = BTreeMap::new();
    let mut entries: Vec<(String, String)> = Vec::new();

    for family in CONFIGMAP_FAMILIES {
        let family_path = root.join(family.folder);
        if !family_path.is_dir() {
            return Err(ProfileError::FamilyFolderMissing(
                family.folder.to_owned(),
                family_path.display().to_string(),
            ));
        }
        let mut map = BTreeMap::new();
        load_configmap_folder(
            &family_path,
            family,
            axes,
            preferred,
            &mut map,
            &mut entries,
        )?;
        maps.insert(family.folder, map);
    }

    let now = Utc::now();
    let manifest = Manifest::initial_install(&tabsdata_version, now, entries);
    Ok(Profile {
        pods: maps.remove(KPROFILE_PODS_FAMILY.folder).unwrap_or_default(),
        assets: maps
            .remove(KPROFILE_ASSETS_FAMILY.folder)
            .unwrap_or_default(),
        configs: maps
            .remove(KPROFILE_CONFIGS_FAMILY.folder)
            .unwrap_or_default(),
        manifest,
    })
}

fn load_configmap_folder(
    folder_path: &Path,
    family: &Family,
    axes: &[bundle::Axis],
    preferred: &[&'static str],
    map: &mut RenderedConfigmap,
    entries: &mut Vec<(String, String)>,
) -> Result<(), ProfileError> {
    let folder = family.folder;
    let mut discoveries: Vec<DiscoveryFile> = Vec::new();
    let read = fs::read_dir(folder_path).map_err(|error| {
        ProfileError::WalkFailed(folder_path.display().to_string(), error.to_string())
    })?;
    for entry in read {
        let entry = entry.map_err(|error| {
            ProfileError::WalkFailed(folder_path.display().to_string(), error.to_string())
        })?;
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if is_hidden_or_system(&path) {
            continue;
        }
        let file_type = entry.file_type().map_err(|error| {
            ProfileError::StatFailed(path.display().to_string(), error.to_string())
        })?;
        if file_type.is_dir() {
            return Err(ProfileError::FamilyFolderNotFlat(
                folder.to_owned(),
                file_name.to_owned(),
            ));
        }
        if !file_type.is_file() {
            continue;
        }
        let parsed = normalize_schema_name(parse_filename(&normalize_schema_extension(file_name)));
        let content = fs::read_to_string(&path).map_err(|error| {
            ProfileError::ReadFailed(path.display().to_string(), error.to_string())
        })?;
        discoveries.push(DiscoveryFile {
            file: file_name.to_owned(),
            parsed,
            content,
        });
    }

    let mut groups: BTreeMap<(String, String), Vec<usize>> = BTreeMap::new();
    for (index, discovery) in discoveries.iter().enumerate() {
        let key = (
            discovery.parsed.name.clone(),
            discovery.parsed.extensions.clone(),
        );
        groups.entry(key).or_default().push(index);
    }

    for (_, indices) in groups {
        let group_parsed: Vec<ParsedFileName> = indices
            .iter()
            .map(|index| discoveries[*index].parsed.clone())
            .collect();
        let Some(group_winner) = pick_winner(&group_parsed, axes, preferred) else {
            continue;
        };
        let winner_index = indices[group_winner];
        let winner = &discoveries[winner_index];
        let stripped_name = winner.parsed.render_stripped();
        if let Err(component) = validate_path_component(&stripped_name) {
            return Err(ProfileError::InvalidPathComponent(
                component,
                folder.to_owned(),
            ));
        }
        if winner.file != stripped_name {
            debug!(
                "Bundle resolved '{folder}/{stripped_name}' from '{folder}/{}'",
                winner.file
            );
        }
        let manifest_key = format!("{folder}/{stripped_name}");
        let source = format!("{folder}/{}", winner.file);
        entries.push((manifest_key, source));
        map.insert(stripped_name, winner.content.clone());
    }

    Ok(())
}

pub async fn render_for_configmaps(
    loaded: &Profile,
    resolver: &ConfigurationResolver,
) -> Result<RenderedConfigmaps, ProfileError> {
    let pods = resolve_map(&loaded.pods, &KPROFILE_PODS_FAMILY, resolver).await?;
    let assets = loaded.assets.clone();
    let configs = resolve_map(&loaded.configs, &KPROFILE_CONFIGS_FAMILY, resolver).await?;
    let manifest = serde_yaml::to_string(&loaded.manifest)
        .map_err(|error| ProfileError::ManifestSerializeFailed(error.to_string()))?;
    Ok(RenderedConfigmaps {
        pods,
        assets,
        configs,
        manifest,
    })
}

pub struct RenderedConfigmaps {
    pub pods: RenderedConfigmap,
    pub assets: RenderedConfigmap,
    pub configs: RenderedConfigmap,
    pub manifest: String,
}

async fn resolve_map(
    source: &RenderedConfigmap,
    family: &Family,
    resolver: &ConfigurationResolver,
) -> Result<RenderedConfigmap, ProfileError> {
    let mut data = BTreeMap::new();
    for (key, value) in source {
        let resolved = resolver.resolve(value).await.map_err(|error| {
            ProfileError::PlaceholderResolutionFailed(
                family.folder.to_owned(),
                key.clone(),
                error.to_string(),
            )
        })?;
        data.insert(key.clone(), resolved);
    }
    Ok(data)
}

pub async fn read_family_configmaps(
    context: &KubeContext,
    family: &Family,
) -> Result<RenderedConfigmap, ProfileError> {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, family.kind);
    let configmaps = get_configmaps(context, Some(filter)).await?;
    let mut merged: RenderedConfigmap = BTreeMap::new();
    for (configmap_name, entries) in configmaps {
        for (key, value) in entries {
            let content = String::from_utf8(value).map_err(|error| {
                ProfileError::ConfigmapEntryNotUtf8(
                    configmap_name.clone(),
                    key.clone(),
                    error.to_string(),
                )
            })?;
            merged.insert(key, content);
        }
    }
    Ok(merged)
}

pub fn parse_from_configmaps(
    pods: &RenderedConfigmap,
    assets: &RenderedConfigmap,
    configs: &RenderedConfigmap,
    manifest_yaml: &str,
) -> Result<Profile, ProfileError> {
    let manifest = deserialize_manifest(manifest_yaml).map_err(|error| {
        ProfileError::ManifestDeserializeFailed(MANIFEST_FILE.to_owned(), error.to_string())
    })?;
    Ok(Profile {
        pods: pods.clone(),
        assets: assets.clone(),
        configs: configs.clone(),
        manifest,
    })
}

pub fn configs_for_worker<'a>(
    profile: &'a Profile,
    worker: &'a str,
) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
    profile.configs.iter().filter_map(move |(key, value)| {
        let stem = match key.find('.') {
            Some(dot) => &key[..dot],
            None => key.as_str(),
        };
        if stem == worker {
            Some((key.as_str(), value.as_str()))
        } else {
            None
        }
    })
}

fn validate_path_component(name: &str) -> Result<(), String> {
    if !is_valid_path_component(name) {
        return Err(name.to_owned());
    }
    Ok(())
}

fn is_valid_path_component(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '.'))
}

pub mod bundle;

#[cfg(test)]
mod tests;
