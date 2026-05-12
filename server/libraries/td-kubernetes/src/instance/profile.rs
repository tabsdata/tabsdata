//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as SystemCommand;

use serde::{Deserialize, Serialize};
use td_common::status::ExitStatus::{GeneralError, TabsDataError};
use td_python::python::resolve_python_binary;
use td_python::tabsdata::tabsdata_version;
use walkdir::WalkDir;

use crate::cerror;
use crate::constants::{PROGRAM_TD_RESOURCE, RESOURCE_KPROFILES};
use crate::system::exit::{Result, abort};
use crate::system::subprocess::apply_detached_flags;
use crate::topology::{PROVIDER_PREFIX_K, ProviderPrefixes, Topology};

#[derive(Serialize, Deserialize)]
pub struct ConfigMapEntry {
    pub profile: String,
    pub path: String,
    pub version: String,
    pub content: String,
}

pub fn resolve_kprofiles_path() -> Result<PathBuf> {
    let resolver = match resolve_python_binary(PROGRAM_TD_RESOURCE) {
        Ok(path) => path,
        Err(error) => {
            cerror!("Failed to locate the '{PROGRAM_TD_RESOURCE}' binary: {error}");
            return abort(GeneralError);
        }
    };
    let mut command = SystemCommand::new(&resolver);
    command.arg(RESOURCE_KPROFILES);
    apply_detached_flags(&mut command);
    let output = match command.output() {
        Ok(output) => output,
        Err(error) => {
            cerror!(
                "Failed to run '{} {RESOURCE_KPROFILES}': {error}",
                resolver.display()
            );
            return abort(GeneralError);
        }
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        cerror!(
            "'{} {RESOURCE_KPROFILES}' failed with status {}: {}",
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
            "Resolved profiles path '{}' does not exist or is not a directory.",
            path.display()
        );
        return abort(TabsDataError);
    }
    Ok(path)
}

pub fn load_profile(profile: &str, topology: &dyn Topology) -> Result<BTreeMap<String, String>> {
    let kprofiles = resolve_kprofiles_path()?;
    let root = kprofiles.join(profile);
    if !root.is_dir() {
        cerror!("Profile '{profile}' not found at '{}'.", root.display());
        return abort(TabsDataError);
    }
    let version = match tabsdata_version() {
        Ok(version) => version,
        Err(error) => {
            cerror!("Failed to resolve the tabsdata package version: {error}");
            return abort(TabsDataError);
        }
    };
    let prefixes = topology.provider_prefixes();
    let mut data = BTreeMap::new();
    walk(&root, prefixes, profile, &version, &mut data)?;
    Ok(data)
}

fn walk(
    profile_root: &Path,
    prefixes: &ProviderPrefixes,
    profile: &str,
    version: &str,
    data: &mut BTreeMap<String, String>,
) -> Result {
    let walker = WalkDir::new(profile_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| {
            entry.depth() == 0
                || entry
                    .file_name()
                    .to_str()
                    .is_none_or(|name| !name.starts_with('.'))
        });
    for entry in walker {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                let path = error
                    .path()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| profile_root.to_path_buf());
                cerror!("Failed to walk '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(key) = filename_for_provider(name, prefixes) else {
            continue;
        };
        if !is_valid_configmap_key(&key) {
            cerror!(
                "File name '{key}' contains characters not allowed in a ConfigMap key. \
                 Allowed characters are alphanumerics, '-', '_' and '.'."
            );
            return abort(TabsDataError);
        }
        if data.contains_key(&key) {
            cerror!("Duplicate file name '{key}' encountered while loading the profile.");
            return abort(TabsDataError);
        }
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) => {
                cerror!("Failed to read '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        let relative_path = path
            .strip_prefix(profile_root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let entry = ConfigMapEntry {
            profile: profile.to_owned(),
            path: relative_path,
            version: version.to_owned(),
            content,
        };
        let json = match serde_json::to_string_pretty(&entry) {
            Ok(json) => json,
            Err(error) => {
                cerror!(
                    "Failed to serialize ConfigMap entry for '{}': {error}",
                    path.display()
                );
                return abort(GeneralError);
            }
        };
        data.insert(key, json);
    }
    Ok(())
}

fn filename_for_provider(name: &str, prefixes: &ProviderPrefixes) -> Option<String> {
    if prefixes.drop.iter().any(|prefix| name.starts_with(prefix)) {
        return None;
    }
    if let Some(rest) = name.strip_prefix(prefixes.keep) {
        return Some(format!("{PROVIDER_PREFIX_K}{rest}"));
    }
    Some(name.to_owned())
}

fn is_valid_configmap_key(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}
