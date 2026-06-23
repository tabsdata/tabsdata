//
// Copyright 2026 Tabs Data Inc.
//

mod resolve;

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

use crate::base::constants::{KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY};
use crate::base::error::gateway::ResolverError;
use crate::runtime::gateway::resolver::fusion::resolve::resolve_config_file;
use crate::runtime::gateway::resolver::placeholder::SystemValues;
use crate::runtime::gateway::resolver::walker::SecretEntry;
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::{Profile, RenderedConfigmap};

pub struct FusedSecrets {
    pub by_file: Vec<(String, Vec<SecretEntry>)>,
}

pub fn fuse(
    profile: &Profile,
    system: &SystemValues,
    auto: &HashMap<String, String>,
    init: &Path,
    stage: &Path,
) -> Result<FusedSecrets, ResolverError> {
    wipe_stage(stage)?;
    compare_family(
        init,
        KPROFILE_CONFIGS_FAMILY.folder,
        &profile.configs,
        false,
    )?;
    compare_family(init, KPROFILE_PODS_FAMILY.folder, &profile.pods, true)?;
    let secrets = resolve_configs_files(profile, system, auto, init, stage)?;
    stage_pods_files(profile, stage)?;
    stage_manifest_file(profile, stage)?;
    Ok(secrets)
}

fn stage_manifest_file(profile: &Profile, stage: &Path) -> Result<(), ResolverError> {
    let manifest = serde_yaml::to_string(&profile.manifest).map_err(|cause| {
        ResolverError::StageWriteFailed {
            path: MANIFEST_FILE.to_owned(),
            cause: cause.to_string(),
        }
    })?;
    write_staged(&stage.join(MANIFEST_FILE), &manifest)
}

fn resolve_configs_files(
    profile: &Profile,
    system: &SystemValues,
    auto: &HashMap<String, String>,
    init: &Path,
    stage: &Path,
) -> Result<FusedSecrets, ResolverError> {
    let init_configs = init.join(KPROFILE_CONFIGS_FAMILY.folder);
    let stage_configs = stage.join(KPROFILE_CONFIGS_FAMILY.folder);
    create_folder(&stage_configs)?;
    let mut by_file = Vec::new();
    for (file, template) in &profile.configs {
        let init_file = init_configs.join(file);
        let init_content = fs::read_to_string(&init_file).map_err(|cause| {
            ResolverError::InitializationReadFailed {
                path: init_file.display().to_string(),
                cause: cause.to_string(),
            }
        })?;
        let outcome = resolve_config_file(template, &init_content, system, auto)?;
        write_staged(&stage_configs.join(file), &outcome.rendered)?;
        if !outcome.secrets.is_empty() {
            by_file.push((file.clone(), outcome.secrets));
        }
    }
    Ok(FusedSecrets { by_file })
}

fn stage_pods_files(profile: &Profile, stage: &Path) -> Result<(), ResolverError> {
    let stage_pods = stage.join(KPROFILE_PODS_FAMILY.folder);
    create_folder(&stage_pods)?;
    for (file, content) in &profile.pods {
        write_staged(&stage_pods.join(file), content)?;
    }
    Ok(())
}

fn create_folder(folder: &Path) -> Result<(), ResolverError> {
    fs::create_dir_all(folder).map_err(|cause| ResolverError::StageWriteFailed {
        path: folder.display().to_string(),
        cause: cause.to_string(),
    })
}

fn write_staged(file: &Path, content: &str) -> Result<(), ResolverError> {
    fs::write(file, content).map_err(|cause| ResolverError::StageWriteFailed {
        path: file.display().to_string(),
        cause: cause.to_string(),
    })
}

fn wipe_stage(stage: &Path) -> Result<(), ResolverError> {
    if !stage.exists() {
        return Ok(());
    }
    fs::remove_dir_all(stage).map_err(|cause| ResolverError::StageWipeFailed {
        path: stage.display().to_string(),
        cause: cause.to_string(),
    })
}

fn compare_family(
    init: &Path,
    family: &str,
    canonical: &RenderedConfigmap,
    strict_content: bool,
) -> Result<(), ResolverError> {
    let family_folder = init.join(family);
    let present = read_flat_family(&family_folder, family)?;

    for file in &present {
        if !canonical.contains_key(file) {
            return Err(ResolverError::InitializationExtraFile {
                family: family.to_owned(),
                file: file.clone(),
            });
        }
    }
    for file in canonical.keys() {
        if !present.contains(file) {
            return Err(ResolverError::InitializationMissingFile {
                family: family.to_owned(),
                file: file.clone(),
            });
        }
    }

    if strict_content {
        for (file, content) in canonical {
            let path = family_folder.join(file);
            let present_content = fs::read_to_string(&path).map_err(|cause| {
                ResolverError::InitializationReadFailed {
                    path: path.display().to_string(),
                    cause: cause.to_string(),
                }
            })?;
            if &present_content != content {
                return Err(ResolverError::InitializationPodModified { file: file.clone() });
            }
        }
    }

    Ok(())
}

fn read_flat_family(family_folder: &Path, family: &str) -> Result<BTreeSet<String>, ResolverError> {
    let mut files = BTreeSet::new();
    let entries =
        fs::read_dir(family_folder).map_err(|cause| ResolverError::InitializationReadFailed {
            path: family_folder.display().to_string(),
            cause: cause.to_string(),
        })?;
    for entry in entries {
        let entry = entry.map_err(|cause| ResolverError::InitializationReadFailed {
            path: family_folder.display().to_string(),
            cause: cause.to_string(),
        })?;
        let file_type =
            entry
                .file_type()
                .map_err(|cause| ResolverError::InitializationReadFailed {
                    path: entry.path().display().to_string(),
                    cause: cause.to_string(),
                })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if file_type.is_dir() {
            return Err(ResolverError::InitializationNotFlat {
                family: family.to_owned(),
                subfolder: name,
            });
        }
        files.insert(name);
    }
    Ok(files)
}
