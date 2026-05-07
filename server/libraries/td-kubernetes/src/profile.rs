//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as SystemCommand;

use td_common::status::ExitStatus::GeneralError;
use td_python::python::resolve_python_binary;

use crate::cerror;
use crate::constants::{PROGRAM_TD_RESOURCE, RESOURCE_KPROFILES};
use crate::exit::{Outcome, abort};
use crate::subprocess::apply_detached_flags;
use crate::topology::{FABRIC_PREFIX_K, FabricPrefixes, Topology};

pub fn resolve_kprofiles_path() -> Outcome<PathBuf> {
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
        return abort(GeneralError);
    }
    Ok(path)
}

pub fn load_profile(profile: &str, topology: &dyn Topology) -> Outcome<BTreeMap<String, String>> {
    let kprofiles = resolve_kprofiles_path()?;
    let root = kprofiles.join(profile);
    if !root.is_dir() {
        cerror!("Profile '{profile}' not found at '{}'.", root.display());
        return abort(GeneralError);
    }
    let prefixes = topology.fabric_prefixes();
    let mut data = BTreeMap::new();
    walk(&root, prefixes, &mut data)?;
    Ok(data)
}

fn walk(
    directory: &Path,
    prefixes: &FabricPrefixes,
    data: &mut BTreeMap<String, String>,
) -> Outcome {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            cerror!(
                "Failed to read directory '{}': {error}",
                directory.display()
            );
            return abort(GeneralError);
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                cerror!(
                    "Failed to read entry under '{}': {error}",
                    directory.display()
                );
                return abort(GeneralError);
            }
        };
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                cerror!("Failed to stat '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        if metadata.is_dir() {
            walk(&path, prefixes, data)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        let Some(key) = filename_for_fabric(name, prefixes) else {
            continue;
        };
        if !is_valid_configmap_key(&key) {
            cerror!(
                "File name '{key}' contains characters not allowed in a ConfigMap key. \
                 Allowed characters are alphanumerics, '-', '_' and '.'."
            );
            return abort(GeneralError);
        }
        if data.contains_key(&key) {
            cerror!("Duplicate file name '{key}' encountered while loading the profile.");
            return abort(GeneralError);
        }
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) => {
                cerror!("Failed to read '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        data.insert(key, contents);
    }
    Ok(())
}

fn filename_for_fabric(name: &str, prefixes: &FabricPrefixes) -> Option<String> {
    if prefixes.drop.iter().any(|prefix| name.starts_with(prefix)) {
        return None;
    }
    if let Some(rest) = name.strip_prefix(prefixes.keep) {
        return Some(format!("{FABRIC_PREFIX_K}{rest}"));
    }
    Some(name.to_owned())
}

fn is_valid_configmap_key(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}
