//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use pep440_rs::Version;
use serde::Serialize;

use crate::base::constants::{
    FOREIGN_ASSETS, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY,
    SCHEMA_EXTENSION,
};
use crate::base::error::profile::ProfileError;
use crate::base::error::upgrade::UpgradeError;
use crate::core::system::exit::CommandResult;
use crate::runtime::instance::profile::bundle::parse_filename;
use crate::runtime::instance::profile::{Profile, load_profile_from};
use crate::runtime::instance::upgrade::apply::apply_step;
use crate::runtime::instance::upgrade::catalog::{load_inventory, load_spec};
use crate::runtime::instance::upgrade::plan::plan;
use crate::runtime::instance::upgrade::store::FolderStore;
use crate::runtime::provider::ClusterProvider;

pub const FAMILIES: [&str; 3] = [
    KPROFILE_CONFIGS_FAMILY.folder,
    KPROFILE_PODS_FAMILY.folder,
    KPROFILE_ASSETS_FAMILY.folder,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Mismatch {
    pub provider: ClusterProvider,
    pub version: String,
    pub file: String,
    pub reason: &'static str,
}

impl fmt::Display for Mismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} upgraded to {}: {} {}",
            self.provider.as_str(),
            self.version,
            self.file,
            self.reason
        )
    }
}

pub fn template_folder(templates: &Path, version: &str, provider: ClusterProvider) -> PathBuf {
    templates.join(version).join(provider.as_str())
}

pub async fn replay_all(
    kupgrade: &Path,
    templates: &Path,
    diverged_output: Option<&Path>,
) -> CommandResult<Vec<Mismatch>> {
    let mut mismatches = Vec::new();
    for provider in ClusterProvider::supported_providers() {
        let work = tempfile::tempdir().map_err(|error| UpgradeError::OfflineTargetWriteFailed {
            path: std::env::temp_dir().display().to_string(),
            cause: error.to_string(),
        })?;
        mismatches
            .extend(replay(kupgrade, templates, provider, work.path(), diverged_output).await?);
    }
    Ok(mismatches)
}

pub fn kprofile_providers(kprofile: &Path) -> Vec<ClusterProvider> {
    let mut tokens = BTreeSet::new();
    for family in FAMILIES {
        let Ok(entries) = std::fs::read_dir(kprofile.join(family)) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name().to_string_lossy().into_owned();
            tokens.extend(parse_filename(&name).tokens);
        }
    }
    ClusterProvider::supported_providers()
        .filter(|provider| tokens.contains(provider.as_str()))
        .collect()
}

pub fn stale_templates(
    kprofile: &Path,
    templates: &Path,
    version: &str,
) -> Result<Vec<Mismatch>, ProfileError> {
    let providers = kprofile_providers(kprofile);
    let mut mismatches: Vec<Mismatch> = ClusterProvider::supported_providers()
        .filter(|provider| !providers.contains(provider))
        .filter(|provider| template_folder(templates, version, *provider).exists())
        .map(|provider| Mismatch {
            provider,
            version: version.to_owned(),
            file: String::new(),
            reason: "has a template although its kprofile holds no variant for it",
        })
        .collect();
    for provider in providers {
        let profile = load_profile_from(kprofile, provider, version)?;
        let folder = template_folder(templates, version, provider);
        mismatches.extend(compare_profile(&profile, &folder, provider, version));
    }
    Ok(mismatches)
}

pub fn write_templates(kprofile: &Path, templates: &Path, version: &str) -> CommandResult {
    for provider in kprofile_providers(kprofile) {
        let profile = load_profile_from(kprofile, provider, version)?;
        let folder = template_folder(templates, version, provider);
        write_profile(&profile, &folder).map_err(|error| {
            UpgradeError::OfflineTargetWriteFailed {
                path: folder.display().to_string(),
                cause: error.to_string(),
            }
        })?;
    }
    Ok(())
}

pub fn template_versions(templates: &Path) -> Vec<String> {
    let mut versions: Vec<(Version, String)> = std::fs::read_dir(templates)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| entry.path().is_dir())
                .filter_map(|entry| {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    Version::from_str(&name).ok().map(|version| (version, name))
                })
                .collect()
        })
        .unwrap_or_default();
    versions.sort();
    versions.into_iter().map(|(_, name)| name).collect()
}

pub async fn replay(
    kupgrade: &Path,
    templates: &Path,
    provider: ClusterProvider,
    work: &Path,
    diverged_output: Option<&Path>,
) -> CommandResult<Vec<Mismatch>> {
    let inventory = load_inventory(kupgrade)?;
    let mut entries = inventory.versions.clone();
    entries.reverse();
    let Some(start) = entries
        .iter()
        .position(|entry| template_folder(templates, &entry.version, provider).is_dir())
    else {
        return Ok(vec![Mismatch {
            provider,
            version: inventory.newest().to_owned(),
            file: String::new(),
            reason: "has no template in any version",
        }]);
    };
    reset(
        &template_folder(templates, &entries[start].version, provider),
        work,
    )?;
    let store = FolderStore::resolved(work);
    let mut mismatches = Vec::new();
    for entry in &entries[start + 1..] {
        if entry.upgrader {
            let spec = load_spec(kupgrade, &entry.version)?;
            for step in plan(&spec, provider.axes(), provider.preferred_values()) {
                apply_step(&store, kupgrade, &entry.version, &step).await?;
            }
        }
        let expected = template_folder(templates, &entry.version, provider);
        if !expected.is_dir() {
            mismatches.push(Mismatch {
                provider,
                version: entry.version.clone(),
                file: String::new(),
                reason: "has no template for this version",
            });
            return Ok(mismatches);
        }
        let diverged = compare_folders(work, &expected, provider, &entry.version);
        if !diverged.is_empty() {
            mismatches.extend(diverged);
            if let Some(output) = diverged_output {
                let kept = output.join(provider.as_str()).join(&entry.version);
                reset(work, &kept)?;
            }
            reset(&expected, work)?;
        }
    }
    Ok(mismatches)
}

fn reset(source: &Path, work: &Path) -> CommandResult {
    let failed = |error: std::io::Error| UpgradeError::OfflineTargetWriteFailed {
        path: work.display().to_string(),
        cause: error.to_string(),
    };
    if work.exists() {
        std::fs::remove_dir_all(work).map_err(failed)?;
    }
    copy_tree(source, work).map_err(failed)?;
    Ok(())
}

pub fn compare_folders(
    actual: &Path,
    expected: &Path,
    provider: ClusterProvider,
    version: &str,
) -> Vec<Mismatch> {
    let mut mismatches = Vec::new();
    for family in FAMILIES {
        let found = files_of(&actual.join(family));
        let wanted = files_of(&expected.join(family));
        for file in found.union(&wanted) {
            let reason = match (found.contains(file), wanted.contains(file)) {
                (true, false) => Some("is left over"),
                (false, true) => Some("is missing"),
                _ => {
                    let left = read(&actual.join(family).join(file));
                    let right = read(&expected.join(family).join(file));
                    (!equivalent(file, &left, &right)).then_some("differs")
                }
            };
            if let Some(reason) = reason {
                mismatches.push(Mismatch {
                    provider,
                    version: version.to_owned(),
                    file: format!("{family}/{file}"),
                    reason,
                });
            }
        }
    }
    mismatches
}

pub fn compare_profile(
    profile: &Profile,
    expected: &Path,
    provider: ClusterProvider,
    version: &str,
) -> Vec<Mismatch> {
    let mut mismatches = Vec::new();
    for (family, files) in [
        (KPROFILE_CONFIGS_FAMILY.folder, &profile.configs),
        (KPROFILE_PODS_FAMILY.folder, &profile.pods),
        (KPROFILE_ASSETS_FAMILY.folder, &profile.assets),
    ] {
        let wanted = files_of(&expected.join(family));
        let found: BTreeSet<String> = files
            .keys()
            .filter(|file| !is_binary_provided(file))
            .cloned()
            .collect();
        for file in found.union(&wanted) {
            let reason = match (files.get(file), wanted.contains(file)) {
                (Some(_), false) => Some("is not in the template"),
                (None, true) => Some("is only in the template"),
                (Some(content), true) => {
                    let right = read(&expected.join(family).join(file));
                    (!equivalent(file, content, &right)).then_some("differs from the template")
                }
                (None, false) => None,
            };
            if let Some(reason) = reason {
                mismatches.push(Mismatch {
                    provider,
                    version: version.to_owned(),
                    file: format!("{family}/{file}"),
                    reason,
                });
            }
        }
    }
    mismatches
}

pub fn write_profile(profile: &Profile, folder: &Path) -> std::io::Result<()> {
    if folder.exists() {
        std::fs::remove_dir_all(folder)?;
    }
    for (family, files) in [
        (KPROFILE_CONFIGS_FAMILY.folder, &profile.configs),
        (KPROFILE_PODS_FAMILY.folder, &profile.pods),
        (KPROFILE_ASSETS_FAMILY.folder, &profile.assets),
    ] {
        let target = folder.join(family);
        std::fs::create_dir_all(&target)?;
        for (file, content) in files {
            if !is_binary_provided(file) {
                std::fs::write(target.join(file), content)?;
            }
        }
    }
    Ok(())
}

fn is_binary_provided(file: &str) -> bool {
    file.ends_with(SCHEMA_EXTENSION) || FOREIGN_ASSETS.iter().any(|foreign| foreign.asset() == file)
}

fn files_of(folder: &Path) -> BTreeSet<String> {
    std::fs::read_dir(folder)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| entry.path().is_file())
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|file| !is_binary_provided(file))
                .collect()
        })
        .unwrap_or_default()
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

fn equivalent(file: &str, left: &str, right: &str) -> bool {
    let structured = file.ends_with(".yaml") || file.ends_with(".yamls");
    if structured {
        let parsed = (
            serde_yaml::from_str::<serde_yaml::Value>(left),
            serde_yaml::from_str::<serde_yaml::Value>(right),
        );
        if let (Ok(left), Ok(right)) = parsed {
            return left == right;
        }
    }
    uncommented(left) == uncommented(right)
}

fn uncommented(text: &str) -> Vec<&str> {
    text.lines()
        .map(str::trim_end)
        .filter(|line| !line.trim_start().starts_with('#') && !line.is_empty())
        .collect()
}

fn copy_tree(source: &Path, target: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let destination = target.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &destination)?;
        } else {
            std::fs::copy(entry.path(), destination)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
