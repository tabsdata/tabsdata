//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::base::constants::{
    FOREIGN_ASSETS, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY,
    SCHEMA_EXTENSION,
};
use crate::base::error::upgrade::UpgradeError;
use crate::core::system::exit::CommandResult;
use crate::runtime::instance::profile::bundle::parse_filename;
use crate::runtime::instance::profile::{Profile, load_profile_from};
use crate::runtime::instance::upgrade::apply::apply_step;
use crate::runtime::instance::upgrade::catalog::{load_inventory, load_spec};
use crate::runtime::instance::upgrade::inventory::InventoryEntry;
use crate::runtime::instance::upgrade::plan::plan;
use crate::runtime::instance::upgrade::store::FolderStore;
use crate::runtime::provider::ClusterProvider;

pub const FAMILIES: [&str; 3] = [
    KPROFILE_CONFIGS_FAMILY.folder,
    KPROFILE_PODS_FAMILY.folder,
    KPROFILE_ASSETS_FAMILY.folder,
];

const KPROFILE_SUBPATH: &str = "variant/resources/kprofile";
const KUPGRADE_SUBPATH: &str = "variant/resources/kupgrade";
const CONFIG_SUBPATH: &str = "variant/config/kupgrade";
const PATCHES_FOLDER: &str = "patches";
const DIGESTS_FILE: &str = "digests.yaml";
const DIGESTS_HEADER: &str = "#\n# Copyright 2026 Tabsdata Inc.\n#\n\n";

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
            "{} at {}: ",
            self.provider.as_str(),
            self.version
        )?;
        if !self.file.is_empty() {
            write!(formatter, "{} ", self.file)?;
        }
        write!(formatter, "{}", self.reason)
    }
}

#[derive(Clone, Debug)]
pub struct Sources {
    pub repository: PathBuf,
    pub kupgrade: PathBuf,
    pub kprofile: PathBuf,
    pub config: PathBuf,
}

impl Sources {
    pub fn of(repository: &Path) -> Self {
        let repository =
            std::path::absolute(repository).unwrap_or_else(|_| repository.to_path_buf());
        Self {
            kupgrade: repository.join(KUPGRADE_SUBPATH),
            kprofile: repository.join(KPROFILE_SUBPATH),
            config: repository.join(CONFIG_SUBPATH),
            repository,
        }
    }

    pub fn with_kupgrade(mut self, kupgrade: &Path) -> Self {
        self.kupgrade = std::path::absolute(kupgrade).unwrap_or_else(|_| kupgrade.to_path_buf());
        self
    }

    fn patch(&self, version: &str) -> PathBuf {
        self.config
            .join(PATCHES_FOLDER)
            .join(format!("{version}.patch"))
    }

    fn digests(&self) -> PathBuf {
        self.config.join(DIGESTS_FILE)
    }
}

#[derive(Clone, Debug)]
pub struct Templates {
    pub folder: PathBuf,
    pub head: String,
    pub versions: Vec<(InventoryEntry, Vec<ClusterProvider>)>,
}

impl Templates {
    pub fn folder_of(&self, version: &str, provider: ClusterProvider) -> PathBuf {
        self.folder.join(version).join(provider.as_str())
    }

    fn released(&self) -> impl Iterator<Item = &(InventoryEntry, Vec<ClusterProvider>)> {
        self.versions
            .iter()
            .filter(|(entry, _)| entry.version != self.head)
    }
}

pub fn materialize(sources: &Sources, folder: &Path) -> CommandResult<Templates> {
    let inventory = load_inventory(&sources.kupgrade)?;
    let head = inventory.newest().to_owned();
    let mut entries = inventory.versions.clone();
    entries.reverse();
    let mut versions = Vec::new();
    for entry in entries {
        let kprofile = if entry.version == head {
            sources.kprofile.clone()
        } else {
            let extracted = folder.join("kprofiles").join(&entry.version);
            extract_kprofile(&sources.repository, &entry.version, &extracted)?;
            let patch = sources.patch(&entry.version);
            if patch.is_file() {
                apply_patch(&patch, &entry.version, &extracted)?;
            }
            extracted
        };
        let providers = kprofile_providers(&kprofile);
        for provider in &providers {
            let profile = load_profile_from(&kprofile, *provider, &entry.version)?;
            let target = folder
                .join("templates")
                .join(&entry.version)
                .join(provider.as_str());
            write_profile(&profile, &target).map_err(|error| {
                UpgradeError::OfflineTargetWriteFailed {
                    path: target.display().to_string(),
                    cause: error.to_string(),
                }
            })?;
        }
        versions.push((entry, providers));
    }
    Ok(Templates {
        folder: folder.join("templates"),
        head,
        versions,
    })
}

pub fn materialize_scratch(sources: &Sources) -> CommandResult<(tempfile::TempDir, Templates)> {
    let scratch = tempfile::tempdir().map_err(|error| UpgradeError::OfflineTargetWriteFailed {
        path: std::env::temp_dir().display().to_string(),
        cause: error.to_string(),
    })?;
    let templates = materialize(sources, scratch.path())?;
    Ok((scratch, templates))
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

fn extract_kprofile(repository: &Path, tag: &str, target: &Path) -> CommandResult {
    let failed = |cause: String| UpgradeError::TagKprofileReadFailed {
        repository: repository.display().to_string(),
        tag: tag.to_owned(),
        cause,
    };
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["archive", "--format=tar", tag, KPROFILE_SUBPATH])
        .output()
        .map_err(|error| failed(error.to_string()))?;
    if !output.status.success() {
        return Err(failed(String::from_utf8_lossy(&output.stderr).trim().to_owned()).into());
    }
    let prefix = Path::new(KPROFILE_SUBPATH);
    let mut archive = tar::Archive::new(output.stdout.as_slice());
    for entry in archive
        .entries()
        .map_err(|error| failed(error.to_string()))?
    {
        let mut entry = entry.map_err(|error| failed(error.to_string()))?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry
            .path()
            .map_err(|error| failed(error.to_string()))?
            .into_owned();
        let Ok(relative) = path.strip_prefix(prefix) else {
            continue;
        };
        let destination = target.join(relative);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| failed(error.to_string()))?;
        }
        entry
            .unpack(&destination)
            .map_err(|error| failed(error.to_string()))?;
    }
    Ok(())
}

fn apply_patch(patch: &Path, tag: &str, kprofile: &Path) -> CommandResult {
    let failed = |cause: String| UpgradeError::PatchApplyFailed {
        patch: patch.display().to_string(),
        tag: tag.to_owned(),
        cause,
    };
    let output = Command::new("git")
        .arg("apply")
        .arg("--unsafe-paths")
        .arg("-p1")
        .arg(patch)
        .current_dir(kprofile)
        .output()
        .map_err(|error| failed(error.to_string()))?;
    if !output.status.success() {
        return Err(failed(String::from_utf8_lossy(&output.stderr).trim().to_owned()).into());
    }
    Ok(())
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Digests {
    versions: BTreeMap<String, BTreeMap<String, String>>,
}

fn read_digests(path: &Path) -> CommandResult<Digests> {
    if !path.exists() {
        return Ok(Digests::default());
    }
    let failed = |cause: String| UpgradeError::DigestsReadFailed {
        path: path.display().to_string(),
        cause,
    };
    let text = std::fs::read_to_string(path).map_err(|error| failed(error.to_string()))?;
    Ok(serde_yaml::from_str(&text).map_err(|error| failed(error.to_string()))?)
}

fn write_digests(path: &Path, digests: &Digests) -> CommandResult {
    let failed = |cause: String| UpgradeError::DigestsWriteFailed {
        path: path.display().to_string(),
        cause,
    };
    let rendered = serde_yaml::to_string(digests).map_err(|error| failed(error.to_string()))?;
    std::fs::write(path, format!("{DIGESTS_HEADER}{rendered}"))
        .map_err(|error| failed(error.to_string()))?;
    Ok(())
}

fn digest_of(folder: &Path) -> String {
    let mut hasher = Sha256::new();
    for family in FAMILIES {
        for file in files_of(&folder.join(family)) {
            hasher.update(format!("{family}/{file}\0").as_bytes());
            hasher.update(std::fs::read(folder.join(family).join(&file)).unwrap_or_default());
            hasher.update(b"\0");
        }
    }
    hex::encode(hasher.finalize())
}

pub fn drifted_digests(sources: &Sources, templates: &Templates) -> CommandResult<Vec<Mismatch>> {
    let recorded = read_digests(&sources.digests())?;
    let mut mismatches = Vec::new();
    let mut expected: BTreeSet<(String, String)> = BTreeSet::new();
    for (entry, providers) in templates.released() {
        for provider in providers {
            expected.insert((entry.version.clone(), provider.as_str().to_owned()));
            let digest = digest_of(&templates.folder_of(&entry.version, *provider));
            let reason = match recorded
                .versions
                .get(&entry.version)
                .and_then(|providers| providers.get(provider.as_str()))
            {
                None => Some("has no recorded digest"),
                Some(stored) if *stored != digest => {
                    Some("resolves differently from when its digest was recorded")
                }
                Some(_) => None,
            };
            if let Some(reason) = reason {
                mismatches.push(Mismatch {
                    provider: *provider,
                    version: entry.version.clone(),
                    file: String::new(),
                    reason,
                });
            }
        }
    }
    for (version, providers) in &recorded.versions {
        for provider in providers.keys() {
            let Some(provider) = ClusterProvider::supported_providers()
                .find(|supported| supported.as_str() == provider)
            else {
                continue;
            };
            if !expected.contains(&(version.clone(), provider.as_str().to_owned())) {
                mismatches.push(Mismatch {
                    provider,
                    version: version.clone(),
                    file: String::new(),
                    reason: "has a recorded digest but no released template",
                });
            }
        }
    }
    Ok(mismatches)
}

pub fn record_digests(sources: &Sources, templates: &Templates) -> CommandResult<usize> {
    let path = sources.digests();
    let mut digests = read_digests(&path)?;
    let mut recorded = 0;
    for (entry, providers) in templates.released() {
        for provider in providers {
            let slot = digests
                .versions
                .entry(entry.version.clone())
                .or_default()
                .entry(provider.as_str().to_owned());
            if let std::collections::btree_map::Entry::Vacant(vacant) = slot {
                vacant.insert(digest_of(&templates.folder_of(&entry.version, *provider)));
                recorded += 1;
            }
        }
    }
    if recorded > 0 {
        write_digests(&path, &digests)?;
    }
    Ok(recorded)
}

pub async fn replay_all(
    sources: &Sources,
    templates: &Templates,
    diverged_output: Option<&Path>,
) -> CommandResult<Vec<Mismatch>> {
    let mut mismatches = Vec::new();
    for provider in ClusterProvider::supported_providers() {
        let work = tempfile::tempdir().map_err(|error| UpgradeError::OfflineTargetWriteFailed {
            path: std::env::temp_dir().display().to_string(),
            cause: error.to_string(),
        })?;
        mismatches
            .extend(replay(sources, templates, provider, work.path(), diverged_output).await?);
    }
    Ok(mismatches)
}

pub async fn replay(
    sources: &Sources,
    templates: &Templates,
    provider: ClusterProvider,
    work: &Path,
    diverged_output: Option<&Path>,
) -> CommandResult<Vec<Mismatch>> {
    let serves = |providers: &Vec<ClusterProvider>| providers.contains(&provider);
    let Some(start) = templates
        .versions
        .iter()
        .position(|(_, providers)| serves(providers))
    else {
        return Ok(vec![Mismatch {
            provider,
            version: templates.head.clone(),
            file: String::new(),
            reason: "is served by no version's kprofile",
        }]);
    };
    let first = &templates.versions[start].0.version;
    reset(&templates.folder_of(first, provider), work)?;
    let store = FolderStore::resolved(work);
    let mut mismatches = Vec::new();
    for (entry, providers) in &templates.versions[start + 1..] {
        if entry.upgrader {
            let spec = load_spec(&sources.kupgrade, &entry.version)?;
            for step in plan(&spec, provider.axes(), provider.preferred_values()) {
                apply_step(&store, &sources.kupgrade, &entry.version, &step).await?;
            }
        }
        if !serves(providers) {
            mismatches.push(Mismatch {
                provider,
                version: entry.version.clone(),
                file: String::new(),
                reason: "is no longer served by this version's kprofile",
            });
            return Ok(mismatches);
        }
        let expected = templates.folder_of(&entry.version, provider);
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

fn write_profile(profile: &Profile, folder: &Path) -> std::io::Result<()> {
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
