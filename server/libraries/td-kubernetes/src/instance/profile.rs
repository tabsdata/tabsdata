//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command as SystemCommand;

use chrono::Utc;
use td_common::status::ExitStatus::{GeneralError, TabsDataError};
use td_python::python::resolve_python_binary;
use td_python::tabsdata::tabsdata_version;
use vfs::{MemoryFS, VfsPath};
use walkdir::WalkDir;

use crate::cerror;
use crate::constants::{PROGRAM_TD_RESOURCE, RESOURCE_KPROFILE};
use crate::instance::manifest::{MANIFEST_FILE, Manifest};
use crate::system::exit::{Result, abort};
use crate::system::subprocess::apply_detached_flags;
use crate::topology::{PROVIDER_PREFIX_K, ProviderPrefixes, Topology};

pub const PATH_SEPARATOR: &str = "__";

pub struct Profile {
    pub tree: VfsPath,
    pub manifest: Manifest,
}

pub fn resolve_kprofile_path() -> Result<PathBuf> {
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
    let output = match command.output() {
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
            "Resolved profile path '{}' does not exist or is not a directory.",
            path.display()
        );
        return abort(TabsDataError);
    }
    Ok(path)
}

pub fn load_profile(topology: &dyn Topology) -> Result<Profile> {
    let root = resolve_kprofile_path()?;
    let tabsdata_version = match tabsdata_version() {
        Ok(version) => version,
        Err(error) => {
            cerror!("Failed to resolve the tabsdata package version: {error}");
            return abort(TabsDataError);
        }
    };
    let prefixes = topology.provider_prefixes();
    let memory: VfsPath = MemoryFS::new().into();
    let mut paths: Vec<String> = Vec::new();

    let walker = WalkDir::new(&root)
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
                    .unwrap_or_else(|| root.clone());
                cerror!("Failed to walk '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(filtered_name) = filename_for_provider(file_name, prefixes) else {
            continue;
        };
        let relative = path.strip_prefix(&root).unwrap_or(path);
        let parent = relative.parent().unwrap_or_else(|| Path::new(""));
        let parent_string = parent.to_string_lossy().replace('\\', "/");
        let manifest_path = if parent_string.is_empty() {
            filtered_name.clone()
        } else {
            format!("{parent_string}/{filtered_name}")
        };
        if let Err(component) = validate_path_components(&manifest_path) {
            cerror!(
                "Path component '{component}' in '{manifest_path}' contains characters not \
                 allowed in a profile path. Only ASCII alphanumerics, '-', and '.' are \
                 permitted (no '_')."
            );
            return abort(TabsDataError);
        }
        let content = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) => {
                cerror!("Failed to read '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        let target = match memory.join(&manifest_path) {
            Ok(target) => target,
            Err(error) => {
                cerror!("Failed to compose virtual path for '{manifest_path}': {error}");
                return abort(GeneralError);
            }
        };
        let parent = target.parent();
        if let Err(error) = parent.create_dir_all() {
            cerror!(
                "Failed to create virtual directory '{}': {error}",
                parent.as_str()
            );
            return abort(GeneralError);
        }
        let mut writer = match target.create_file() {
            Ok(writer) => writer,
            Err(error) => {
                cerror!(
                    "Failed to create virtual file '{}': {error}",
                    target.as_str()
                );
                return abort(GeneralError);
            }
        };
        if let Err(error) = writer.write_all(&content) {
            cerror!(
                "Failed to write virtual file '{}': {error}",
                target.as_str()
            );
            return abort(GeneralError);
        }
        paths.push(manifest_path);
    }

    let now = Utc::now();
    let manifest = Manifest::initial_install(&tabsdata_version, now, paths);
    Ok(Profile {
        tree: memory,
        manifest,
    })
}

pub fn encode_for_configmap(loaded: &Profile) -> Result<BTreeMap<String, String>> {
    let mut data = BTreeMap::new();
    for decoded_path in loaded.manifest.entries.keys() {
        let target = match loaded.tree.join(decoded_path) {
            Ok(target) => target,
            Err(error) => {
                cerror!("Failed to resolve virtual path '{decoded_path}': {error}");
                return abort(GeneralError);
            }
        };
        let mut reader = match target.open_file() {
            Ok(reader) => reader,
            Err(error) => {
                cerror!("Failed to open virtual file '{}': {error}", target.as_str());
                return abort(GeneralError);
            }
        };
        let mut content = String::new();
        if let Err(error) = reader.read_to_string(&mut content) {
            cerror!(
                "Failed to read virtual file '{}' as UTF-8: {error}",
                target.as_str()
            );
            return abort(GeneralError);
        }
        data.insert(encode_path(decoded_path), content);
    }
    let manifest_yaml = match serde_yaml::to_string(&loaded.manifest) {
        Ok(yaml) => yaml,
        Err(error) => {
            cerror!("Failed to serialize the profile manifest: {error}");
            return abort(GeneralError);
        }
    };
    data.insert(MANIFEST_FILE.to_owned(), manifest_yaml);
    Ok(data)
}

pub fn encode_path(decoded: &str) -> String {
    decoded.replace('/', PATH_SEPARATOR)
}

pub fn decode_path(encoded: &str) -> String {
    encoded.replace(PATH_SEPARATOR, "/")
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

fn validate_path_components(path: &str) -> std::result::Result<(), String> {
    for component in path.split('/') {
        if !is_valid_path_component(component) {
            return Err(component.to_owned());
        }
    }
    Ok(())
}

fn is_valid_path_component(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '.'))
}

#[cfg(test)]
mod tests;
