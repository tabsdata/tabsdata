//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as SystemCommand;

use chrono::Utc;
use td_common::status::ExitStatus::{GeneralError, TabsDataError};
use td_python::python::resolve_python_binary;
use td_python::tabsdata::tabsdata_version;
use tracing::info;

use crate::cerror;
use crate::base::constants::{
    CONFIGMAP_TABSDATA_CONFIGS, CONFIGMAP_TABSDATA_GLOBALS, CONFIGMAP_TABSDATA_PODS,
    KPROFILE_CONFIGMAPS_FOLDER, KPROFILE_CONFIGS_FOLDER, KPROFILE_GLOBALS_FOLDER,
    KPROFILE_PODS_FOLDER, PROGRAM_TD_RESOURCE, RESOURCE_KPROFILE,
};
use crate::runtime::gateway::resolver::ConfigurationResolver;
use crate::runtime::instance::manifest::{MANIFEST_FILE, Manifest, deserialize_manifest};
use crate::runtime::instance::profile::bundle::{ParsedFileName, parse_filename, pick_winner};
use crate::runtime::provider::ClusterProvider;
use crate::core::system::exit::{Result, abort};
use crate::core::system::subprocess;
use crate::core::system::subprocess::apply_detached_flags;

pub type RenderedConfigmap = BTreeMap<String, String>;

pub struct Profile {
    pub globals: RenderedConfigmap,
    pub pods: RenderedConfigmap,
    pub configs: RenderedConfigmap,
    pub manifest: Manifest,
}

const CONFIGMAP_FOLDERS: &[&str] = &[
    KPROFILE_GLOBALS_FOLDER,
    KPROFILE_PODS_FOLDER,
    KPROFILE_CONFIGS_FOLDER,
];

pub fn resolve_kprofile_path(verbose: bool) -> Result<PathBuf> {
    let resolver = match resolve_python_binary(PROGRAM_TD_RESOURCE) {
        Ok(path) => path,
        Err(error) => {
            cerror!("Failed to locate the '{PROGRAM_TD_RESOURCE}' binary: {error}");
            return abort(GeneralError);
        }
    };
    let mut command = SystemCommand::new(&resolver);
    command.arg(RESOURCE_KPROFILE);
    apply_detached_flags(&mut command);
    let output = match subprocess::run(&mut command, PROGRAM_TD_RESOURCE, verbose) {
        Ok(output) => output,
        Err(error) => {
            cerror!(
                "Failed to run '{} {RESOURCE_KPROFILE}': {error}",
                resolver.display()
            );
            return abort(GeneralError);
        }
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        cerror!(
            "'{} {RESOURCE_KPROFILE}' failed with status {}: {}",
            resolver.display(),
            output.status,
            stderr.trim()
        );
        return abort(GeneralError);
    }
    let stdout = match String::from_utf8(output.stdout) {
        Ok(text) => text,
        Err(error) => {
            cerror!(
                "Output of '{}' is not valid UTF-8: {error}",
                resolver.display()
            );
            return abort(GeneralError);
        }
    };
    let path = PathBuf::from(stdout.trim());
    if !path.is_dir() {
        cerror!(
            "Resolved profile path '{}' does not exist or is not a folder.",
            path.display()
        );
        return abort(TabsDataError);
    }
    Ok(path)
}

struct DiscoveryFile {
    file: String,
    parsed: ParsedFileName,
    content: String,
}

pub fn load_profile(provider: ClusterProvider, verbose: bool) -> Result<Profile> {
    let root = resolve_kprofile_path(verbose)?;
    let configmaps_root = root.join(KPROFILE_CONFIGMAPS_FOLDER);
    if !configmaps_root.is_dir() {
        cerror!(
            "Profile is missing the '{KPROFILE_CONFIGMAPS_FOLDER}' folder at '{}'.",
            configmaps_root.display()
        );
        return abort(TabsDataError);
    }
    let tabsdata_version = match tabsdata_version() {
        Ok(version) => version,
        Err(error) => {
            cerror!("Failed to resolve the tabsdata package version: {error}");
            return abort(TabsDataError);
        }
    };
    let axes = provider.axes();
    let preferred = provider.preferred_values();

    let mut maps: BTreeMap<&'static str, RenderedConfigmap> = BTreeMap::new();
    let mut entries: Vec<(String, String)> = Vec::new();

    for folder in CONFIGMAP_FOLDERS {
        let folder_path = configmaps_root.join(folder);
        if !folder_path.is_dir() {
            cerror!(
                "Profile is missing the required folder \
                 '{KPROFILE_CONFIGMAPS_FOLDER}/{folder}' at '{}'.",
                folder_path.display()
            );
            return abort(TabsDataError);
        }
        let mut map = BTreeMap::new();
        load_configmap_folder(
            &folder_path,
            folder,
            axes,
            preferred,
            &mut map,
            &mut entries,
        )?;
        maps.insert(*folder, map);
    }

    let now = Utc::now();
    let manifest = Manifest::initial_install(&tabsdata_version, now, entries);
    Ok(Profile {
        globals: maps.remove(KPROFILE_GLOBALS_FOLDER).unwrap_or_default(),
        pods: maps.remove(KPROFILE_PODS_FOLDER).unwrap_or_default(),
        configs: maps.remove(KPROFILE_CONFIGS_FOLDER).unwrap_or_default(),
        manifest,
    })
}

fn load_configmap_folder(
    folder_path: &Path,
    folder: &str,
    axes: &[bundle::Axis],
    preferred: &[&'static str],
    map: &mut RenderedConfigmap,
    entries: &mut Vec<(String, String)>,
) -> Result {
    let mut discoveries: Vec<DiscoveryFile> = Vec::new();
    let read = match fs::read_dir(folder_path) {
        Ok(entries) => entries,
        Err(error) => {
            cerror!(
                "Failed to read profile folder '{}': {error}",
                folder_path.display()
            );
            return abort(GeneralError);
        }
    };
    for entry in read {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                cerror!("Failed to enumerate '{}': {error}", folder_path.display());
                return abort(GeneralError);
            }
        };
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name.starts_with('.') {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                cerror!("Failed to stat '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        if file_type.is_dir() {
            cerror!(
                "Profile folder 'configmaps/{folder}' must be flat, but contains \
                 the subfolder '{file_name}'."
            );
            return abort(TabsDataError);
        }
        if !file_type.is_file() {
            continue;
        }
        let parsed = parse_filename(file_name);
        let content = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(error) => {
                cerror!("Failed to read '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
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
            cerror!(
                "Profile entry '{component}' in 'configmaps/{folder}' contains characters \
                 not allowed in a configmap key. Only ASCII alphanumerics, '-', and '.' are \
                 permitted (no '_')."
            );
            return abort(TabsDataError);
        }
        if winner.file != stripped_name {
            info!(
                "Bundle resolved 'configmaps/{folder}/{stripped_name}' from \
                 'configmaps/{folder}/{}'",
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
) -> Result<RenderedConfigmaps> {
    let globals = resolve_map(&loaded.globals, KPROFILE_GLOBALS_FOLDER, resolver).await?;
    let pods = resolve_map(&loaded.pods, KPROFILE_PODS_FOLDER, resolver).await?;
    let configs = resolve_map(&loaded.configs, KPROFILE_CONFIGS_FOLDER, resolver).await?;

    let manifest_yaml = match serde_yaml::to_string(&loaded.manifest) {
        Ok(yaml) => yaml,
        Err(error) => {
            cerror!("Failed to serialize the profile manifest: {error}");
            return abort(GeneralError);
        }
    };
    let mut globals = globals;
    globals.insert(MANIFEST_FILE.to_owned(), manifest_yaml);

    Ok(RenderedConfigmaps {
        globals,
        pods,
        configs,
    })
}

pub struct RenderedConfigmaps {
    pub globals: RenderedConfigmap,
    pub pods: RenderedConfigmap,
    pub configs: RenderedConfigmap,
}

impl RenderedConfigmaps {
    pub fn into_named(self) -> [(&'static str, RenderedConfigmap); 3] {
        [
            (CONFIGMAP_TABSDATA_GLOBALS, self.globals),
            (CONFIGMAP_TABSDATA_PODS, self.pods),
            (CONFIGMAP_TABSDATA_CONFIGS, self.configs),
        ]
    }
}

async fn resolve_map(
    source: &RenderedConfigmap,
    folder: &str,
    resolver: &ConfigurationResolver,
) -> Result<RenderedConfigmap> {
    let mut data = BTreeMap::new();
    for (key, value) in source {
        let resolved = match resolver.resolve_sys(value).await {
            Ok(resolved) => resolved,
            Err(error) => {
                cerror!("Failed to resolve placeholders in '{folder}/{key}': {error}");
                return abort(TabsDataError);
            }
        };
        data.insert(key.clone(), resolved);
    }
    Ok(data)
}

pub async fn resolve_profile_entry(
    loaded: &Profile,
    resolver: &ConfigurationResolver,
    folder: &str,
    key: &str,
) -> Result<String> {
    let source = match folder {
        KPROFILE_GLOBALS_FOLDER => &loaded.globals,
        KPROFILE_PODS_FOLDER => &loaded.pods,
        KPROFILE_CONFIGS_FOLDER => &loaded.configs,
        other => {
            cerror!("Unknown profile folder '{other}'.");
            return abort(GeneralError);
        }
    };
    let Some(content) = source.get(key) else {
        cerror!("Profile is missing the '{folder}/{key}' entry.");
        return abort(TabsDataError);
    };
    match resolver.resolve_sys(content).await {
        Ok(resolved) => Ok(resolved),
        Err(error) => {
            cerror!("Failed to resolve placeholders in '{folder}/{key}': {error}");
            abort(TabsDataError)
        }
    }
}

pub fn parse_from_configmaps(
    globals: &RenderedConfigmap,
    pods: &RenderedConfigmap,
    configs: &RenderedConfigmap,
) -> Result<Profile> {
    let manifest_yaml = match globals.get(MANIFEST_FILE) {
        Some(yaml) => yaml,
        None => {
            cerror!(
                "Configmap '{KPROFILE_GLOBALS_FOLDER}' is missing the '{MANIFEST_FILE}' entry."
            );
            return abort(TabsDataError);
        }
    };
    let manifest = match deserialize_manifest(manifest_yaml) {
        Ok(manifest) => manifest,
        Err(error) => {
            cerror!("Failed to deserialize '{MANIFEST_FILE}': {error}");
            return abort(TabsDataError);
        }
    };
    let mut globals_clone = globals.clone();
    globals_clone.remove(MANIFEST_FILE);
    Ok(Profile {
        globals: globals_clone,
        pods: pods.clone(),
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

fn validate_path_component(name: &str) -> std::result::Result<(), String> {
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
mod tests {
    use super::*;
    mod profile;
}
