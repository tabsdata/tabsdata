//
// Copyright 2026 Tabsdata Inc.
//

mod resolve;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

use serde_yaml::Value;

use crate::base::constants::{
    KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY,
};
use crate::base::error::gateway::ResolverError;
use crate::runtime::gateway::resolver::fusion::resolve::resolve_config_file;
use crate::runtime::gateway::resolver::placeholder::SystemValues;
use crate::runtime::gateway::resolver::reveal::prefill_secrets;
use crate::runtime::gateway::resolver::schema::Schema;
use crate::runtime::gateway::resolver::walker::SecretEntry;
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::profile::{Profile, RenderedConfigmap};
use crate::runtime::secrets::discovery::extract_secret_leaves;

const CONFIG_EXTENSION: &str = ".yaml";
const SCHEMA_EXTENSION: &str = ".yamls";

pub struct FusedSecrets {
    pub by_file: Vec<(String, Vec<SecretEntry>)>,
}

pub struct MergedConfig {
    pub file: String,
    pub rendered: String,
    pub secrets: Vec<SecretEntry>,
}

pub fn merge(
    profile: &Profile,
    system: &SystemValues,
    input: &Path,
    server_secrets: &HashMap<String, HashMap<String, String>>,
) -> Result<Vec<MergedConfig>, ResolverError> {
    compare_family(
        input,
        KPROFILE_CONFIGS_FAMILY.folder,
        &profile.configs,
        false,
    )?;
    let input_configs = input.join(KPROFILE_CONFIGS_FAMILY.folder);
    let empty_auto: HashMap<String, String> = HashMap::new();
    let vault: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    seed_vault_from_server_secrets(&vault, &profile.configs, server_secrets);
    let mut merged = Vec::new();
    for (file, template) in &profile.configs {
        if file.ends_with(SCHEMA_EXTENSION) {
            continue;
        }
        let input_file = input_configs.join(file);
        let input_content = fs::read_to_string(&input_file).map_err(|cause| {
            ResolverError::InitializationReadFailed {
                path: input_file.display().to_string(),
                cause: cause.to_string(),
            }
        })?;
        let prefilled = match server_secrets.get(file) {
            Some(values) => prefill_secrets(&input_content, values)?,
            None => input_content,
        };
        let template = bake_schema_secrets(file, template, &profile.configs)?;
        let outcome = resolve_config_file(&template, &prefilled, system, &empty_auto, &vault)?;
        merged.push(MergedConfig {
            file: file.clone(),
            rendered: outcome.rendered,
            secrets: outcome.secrets,
        });
    }
    Ok(merged)
}

fn seed_vault_from_server_secrets(
    vault: &RefCell<HashMap<String, String>>,
    configs: &RenderedConfigmap,
    server_secrets: &HashMap<String, HashMap<String, String>>,
) {
    let mut vault = vault.borrow_mut();
    for (file, template) in configs {
        if file.ends_with(SCHEMA_EXTENSION) {
            continue;
        }
        let Some(values) = server_secrets.get(file) else {
            continue;
        };
        let Ok(mut yaml) = serde_yaml::from_str::<Value>(template) else {
            continue;
        };
        let Ok(leaves) = extract_secret_leaves(file, &mut yaml) else {
            continue;
        };
        for leaf in leaves {
            let Some(generator) = leaf.generator else {
                continue;
            };
            let Some(value) = values.get(&leaf.key).filter(|value| !value.is_empty()) else {
                continue;
            };
            vault
                .entry(generator.name().to_owned())
                .or_insert_with(|| value.clone());
        }
    }
}

pub fn fuse(
    profile: &Profile,
    system: &SystemValues,
    auto: &HashMap<String, String>,
    init: &Path,
    stage: &Path,
) -> Result<FusedSecrets, ResolverError> {
    wipe_stage(stage)?;
    compare_family(init, KPROFILE_PODS_FAMILY.folder, &profile.pods, true)?;
    compare_family(init, KPROFILE_ASSETS_FAMILY.folder, &profile.assets, true)?;
    compare_family(
        init,
        KPROFILE_CONFIGS_FAMILY.folder,
        &profile.configs,
        false,
    )?;
    stage_pods_files(profile, stage)?;
    stage_assets_files(profile, stage)?;
    let secrets = resolve_configs_files(profile, system, auto, init, stage)?;
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
    let vault: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    let mut by_file = Vec::new();
    for (file, template) in &profile.configs {
        if file.ends_with(SCHEMA_EXTENSION) {
            continue;
        }
        let init_file = init_configs.join(file);
        let init_content = fs::read_to_string(&init_file).map_err(|cause| {
            ResolverError::InitializationReadFailed {
                path: init_file.display().to_string(),
                cause: cause.to_string(),
            }
        })?;
        let template = bake_schema_secrets(file, template, &profile.configs)?;
        let outcome = resolve_config_file(&template, &init_content, system, auto, &vault)?;
        write_staged(&stage_configs.join(file), &outcome.rendered)?;
        by_file.push((file.clone(), outcome.secrets));
    }
    Ok(FusedSecrets { by_file })
}

fn bake_schema_secrets(
    file: &str,
    config: &str,
    configs: &RenderedConfigmap,
) -> Result<String, ResolverError> {
    let Some(stem) = file.strip_suffix(CONFIG_EXTENSION) else {
        return Ok(config.to_owned());
    };
    let Some(schema_text) = configs.get(&format!("{stem}{SCHEMA_EXTENSION}")) else {
        return Ok(config.to_owned());
    };
    let schema =
        Schema::parse(file, schema_text).map_err(|cause| ResolverError::SchemaParseFailed {
            path: file.to_owned(),
            cause: cause.to_string(),
        })?;
    if schema.is_empty() {
        return Ok(config.to_owned());
    }
    let mut value: Value =
        serde_yaml::from_str(config).map_err(|cause| ResolverError::ParseFailed {
            path: file.to_owned(),
            cause: cause.to_string(),
        })?;
    schema.rewrite_secrets(&mut value);
    serde_yaml::to_string(&value).map_err(|cause| ResolverError::ParseFailed {
        path: file.to_owned(),
        cause: cause.to_string(),
    })
}

fn stage_pods_files(profile: &Profile, stage: &Path) -> Result<(), ResolverError> {
    let stage_pods = stage.join(KPROFILE_PODS_FAMILY.folder);
    create_folder(&stage_pods)?;
    for (file, content) in &profile.pods {
        write_staged(&stage_pods.join(file), content)?;
    }
    Ok(())
}

fn stage_assets_files(profile: &Profile, stage: &Path) -> Result<(), ResolverError> {
    let stage_assets = stage.join(KPROFILE_ASSETS_FAMILY.folder);
    create_folder(&stage_assets)?;
    for (file, content) in &profile.assets {
        write_staged(&stage_assets.join(file), content)?;
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
                return Err(ResolverError::InitializationTemplateModified {
                    family: family.to_owned(),
                    file: file.clone(),
                });
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

#[cfg(test)]
mod tests;
