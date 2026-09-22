//
// Copyright 2026 Tabsdata Inc.
//

mod resolve;

use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{
    LiteralOrigin, PATH_SEPARATOR, parse_bare_placeholder,
};
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

use serde_yaml::Value;

use crate::base::constants::{
    CUSTOMIZABLE_ASSETS, FOREIGN_ASSETS, ForeignAsset, KPROFILE_ASSETS_FAMILY,
    KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY, SCHEMA_EXTENSION,
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
        &[],
        &[],
    )?;
    let input_configs = input.join(KPROFILE_CONFIGS_FAMILY.folder);
    let empty_auto: HashMap<String, String> = HashMap::new();
    let vault: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    let literals: RefCell<HashMap<String, LiteralOrigin>> = RefCell::new(HashMap::new());
    let values: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    seed_vault_from_server_secrets(&vault, &profile.configs, server_secrets);
    seed_literals_from_input(&literals, &vault, &profile.configs, &input_configs);
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
        let schema = schema_of(file, &profile.configs)?;
        let template = bake_schema_secrets(file, template, schema.as_ref())?;
        let outcome = resolve_config_file(
            file,
            &template,
            &prefilled,
            system,
            &empty_auto,
            &vault,
            &literals,
            &values,
            schema.as_ref(),
        )?;
        merged.push(MergedConfig {
            file: file.clone(),
            rendered: outcome.rendered,
            secrets: outcome.secrets,
        });
    }
    Ok(merged)
}

fn seed_literals_from_input(
    literals: &RefCell<HashMap<String, LiteralOrigin>>,
    vault: &RefCell<HashMap<String, String>>,
    configs: &RenderedConfigmap,
    input_configs: &Path,
) {
    let mut literals = literals.borrow_mut();
    let mut vault = vault.borrow_mut();
    for (file, template) in configs {
        if file.ends_with(SCHEMA_EXTENSION) {
            continue;
        }
        let Ok(input_content) = fs::read_to_string(input_configs.join(file)) else {
            continue;
        };
        let Ok(template_value) = serde_yaml::from_str::<Value>(template) else {
            continue;
        };
        let Ok(input_value) = serde_yaml::from_str::<Value>(&input_content) else {
            continue;
        };
        let mut path: Vec<String> = Vec::new();
        harvest_supplied(
            &template_value,
            &input_value,
            &mut path,
            &mut literals,
            &mut vault,
        );
    }
}

fn harvest_supplied(
    template: &Value,
    init: &Value,
    path: &mut Vec<String>,
    literals: &mut HashMap<String, LiteralOrigin>,
    vault: &mut HashMap<String, String>,
) {
    match template {
        Value::String(text) => {
            let Some(placeholder) = parse_bare_placeholder(text) else {
                return;
            };
            let Ok(kind) = placeholder.kind.parse::<PlaceholderKind>() else {
                return;
            };
            let shared = matches!(
                kind,
                PlaceholderKind::Literal | PlaceholderKind::LiteralAuto | PlaceholderKind::Secret
            );
            if !shared {
                return;
            }
            let Some(category) = placeholder.category.filter(|category| !category.is_empty())
            else {
                return;
            };
            let Some(value) = scalar_text(init) else {
                return;
            };
            if value.is_empty() || value == *text {
                return;
            }
            match kind {
                PlaceholderKind::Secret => {
                    vault.entry(category).or_insert(value);
                }
                _ => {
                    literals.entry(category).or_insert_with(|| LiteralOrigin {
                        value,
                        path: path.join(PATH_SEPARATOR),
                    });
                }
            }
        }
        Value::Mapping(map) => {
            let Value::Mapping(init_map) = init else {
                return;
            };
            for (key, child) in map {
                let Some(segment) = key.as_str() else {
                    continue;
                };
                let Some(init_child) = init_map.get(key) else {
                    continue;
                };
                path.push(segment.to_owned());
                harvest_supplied(child, init_child, path, literals, vault);
                path.pop();
            }
        }
        Value::Sequence(items) => {
            let Value::Sequence(init_items) = init else {
                return;
            };
            for (index, item) in items.iter().enumerate() {
                let Some(init_item) = init_items.get(index) else {
                    continue;
                };
                path.push(index.to_string());
                harvest_supplied(item, init_item, path, literals, vault);
                path.pop();
            }
        }
        _ => {}
    }
}

fn scalar_text(node: &Value) -> Option<String> {
    match node {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
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
    compare_families(profile, init)?;
    stage_pods_files(profile, stage)?;
    stage_assets_files(profile, init, stage)?;
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
    let literals: RefCell<HashMap<String, LiteralOrigin>> = RefCell::new(HashMap::new());
    let values: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    seed_literals_from_input(&literals, &vault, &profile.configs, &init_configs);
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
        let schema = schema_of(file, &profile.configs)?;
        let template = bake_schema_secrets(file, template, schema.as_ref())?;
        let outcome = resolve_config_file(
            file,
            &template,
            &init_content,
            system,
            auto,
            &vault,
            &literals,
            &values,
            schema.as_ref(),
        )?;
        write_staged(&stage_configs.join(file), &outcome.rendered)?;
        by_file.push((file.clone(), outcome.secrets));
    }
    Ok(FusedSecrets { by_file })
}

fn schema_of(file: &str, configs: &RenderedConfigmap) -> Result<Option<Schema>, ResolverError> {
    let Some(stem) = file.strip_suffix(CONFIG_EXTENSION) else {
        return Ok(None);
    };
    let Some(schema_text) = configs.get(&format!("{stem}{SCHEMA_EXTENSION}")) else {
        return Ok(None);
    };
    let schema =
        Schema::parse(file, schema_text).map_err(|cause| ResolverError::SchemaParseFailed {
            path: file.to_owned(),
            cause: cause.to_string(),
        })?;
    Ok((!schema.is_empty()).then_some(schema))
}

fn bake_schema_secrets(
    file: &str,
    config: &str,
    schema: Option<&Schema>,
) -> Result<String, ResolverError> {
    let Some(schema) = schema else {
        return Ok(config.to_owned());
    };
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

fn stage_assets_files(profile: &Profile, init: &Path, stage: &Path) -> Result<(), ResolverError> {
    let init_assets = init.join(KPROFILE_ASSETS_FAMILY.folder);
    let stage_assets = stage.join(KPROFILE_ASSETS_FAMILY.folder);
    create_folder(&stage_assets)?;
    for (file, content) in &profile.assets {
        if CUSTOMIZABLE_ASSETS.contains(&file.as_str()) {
            let init_file = init_assets.join(file);
            let init_content = fs::read_to_string(&init_file).map_err(|cause| {
                ResolverError::InitializationReadFailed {
                    path: init_file.display().to_string(),
                    cause: cause.to_string(),
                }
            })?;
            write_staged(&stage_assets.join(file), &init_content)?;
            continue;
        }
        write_staged(&stage_assets.join(file), content)?;
    }
    for foreign in FOREIGN_ASSETS {
        let init_file = init_assets.join(foreign.asset());
        let init_content = fs::read_to_string(&init_file).map_err(|cause| {
            ResolverError::InitializationReadFailed {
                path: init_file.display().to_string(),
                cause: cause.to_string(),
            }
        })?;
        write_staged(&stage_assets.join(foreign.asset()), &init_content)?;
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

pub fn compare_families(profile: &Profile, init: &Path) -> Result<(), ResolverError> {
    compare_family(
        init,
        KPROFILE_PODS_FAMILY.folder,
        &profile.pods,
        true,
        &[],
        &[],
    )?;
    compare_family(
        init,
        KPROFILE_ASSETS_FAMILY.folder,
        &profile.assets,
        true,
        CUSTOMIZABLE_ASSETS,
        FOREIGN_ASSETS,
    )?;
    compare_family(
        init,
        KPROFILE_CONFIGS_FAMILY.folder,
        &profile.configs,
        false,
        &[],
        &[],
    )
}

fn compare_family(
    init: &Path,
    family: &str,
    canonical: &RenderedConfigmap,
    strict_content: bool,
    customizable: &[&str],
    foreign: &[ForeignAsset],
) -> Result<(), ResolverError> {
    let family_folder = init.join(family);
    let present = read_flat_family(&family_folder, family)?;

    for file in &present {
        if !canonical.contains_key(file)
            && !foreign.iter().any(|entry| entry.asset() == file.as_str())
        {
            return Err(ResolverError::InitializationExtraFile {
                family: family.to_owned(),
                file: file.clone(),
            });
        }
    }
    for file in canonical
        .keys()
        .map(String::as_str)
        .chain(foreign.iter().map(|entry| entry.asset()))
    {
        if !present.contains(file) {
            return Err(ResolverError::InitializationMissingFile {
                family: family.to_owned(),
                file: file.to_owned(),
            });
        }
    }

    if strict_content {
        for (file, content) in canonical {
            if customizable.contains(&file.as_str()) {
                continue;
            }
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
