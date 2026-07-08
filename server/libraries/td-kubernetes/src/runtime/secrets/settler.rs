//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::{BTreeMap, BTreeSet};

use crate::base::constants::{
    CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY, SECRET_FAMILY_CONFIG,
};
use crate::base::error::secret::SecretError;
use crate::base::naming::name_from_kube_resource_name;
use crate::kube::api::{LabelFilter, get_configmaps, get_secret, upsert_secret};
use crate::kube::context::KubeContext;
use crate::runtime::secrets::discovery::{Generator, SecretLeaf, extract_secret_leaves};
use crate::runtime::secrets::source::to_secret_name;

pub struct Settlement<'a> {
    pub config: &'a str,
    pub config_yaml: &'a str,
    pub secret: &'a str,
    pub actual_keys: Option<BTreeSet<String>>,
}

pub fn settle_secret_keys(input: &Settlement<'_>) -> Result<(), SecretError> {
    let mut yaml: serde_yaml::Value = serde_yaml::from_str(input.config_yaml).map_err(|error| {
        SecretError::ConfigYamlParseFailed {
            config: input.config.to_owned(),
            cause: error.to_string(),
        }
    })?;
    let leaves = extract_secret_leaves(input.config, &mut yaml)?;
    let expected: BTreeSet<String> = leaves.into_iter().map(|leaf| leaf.key).collect();

    match (&input.actual_keys, expected.is_empty()) {
        (None, true) => Ok(()),
        (None, false) => Err(SecretError::SettlementSecretMissing {
            config: input.config.to_owned(),
            secret: input.secret.to_owned(),
            expected: sorted_join(&expected),
        }),
        (Some(_), true) => Err(SecretError::SettlementSecretUnexpected {
            config: input.config.to_owned(),
            secret: input.secret.to_owned(),
        }),
        (Some(actual), false) => {
            let extra: BTreeSet<&str> = actual.difference(&expected).map(String::as_str).collect();
            let missing: BTreeSet<&str> = expected.difference(actual).map(String::as_str).collect();
            if extra.is_empty() && missing.is_empty() {
                Ok(())
            } else {
                Err(SecretError::SettlementKeysMismatch {
                    config: input.config.to_owned(),
                    secret: input.secret.to_owned(),
                    extra: sorted_join_str(&extra),
                    missing: sorted_join_str(&missing),
                })
            }
        }
    }
}

pub async fn auto_fill_generated_secrets(context: &KubeContext) -> Result<(), SecretError> {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.kind);
    let configmaps = get_configmaps(context, Some(filter)).await?;
    let mut leaves_by_secret: Vec<(String, Vec<SecretLeaf>)> = Vec::with_capacity(configmaps.len());
    for (configmap_name, entries) in configmaps {
        let config_file = name_from_kube_resource_name(
            &configmap_name,
            KPROFILE_CONFIGS_FAMILY.scope,
            KPROFILE_CONFIGS_FAMILY.kind,
        )?
        .to_owned();
        let Some((_, bytes)) = entries.into_iter().next() else {
            continue;
        };
        let Ok(config_yaml) = String::from_utf8(bytes) else {
            continue;
        };
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(&config_yaml).map_err(|error| {
            SecretError::ConfigYamlParseFailed {
                config: config_file.clone(),
                cause: error.to_string(),
            }
        })?;
        let leaves = extract_secret_leaves(&config_file, &mut yaml)?;
        let secret_name = to_secret_name(&config_file)?;
        leaves_by_secret.push((secret_name, leaves));
    }
    let mut existing = collect_existing_values(context, &leaves_by_secret).await?;
    let invalidated = invalidated_generators(&leaves_by_secret, &existing);
    for generator in &invalidated {
        existing.remove(generator);
    }
    for (secret_name, leaves) in &leaves_by_secret {
        fill_generated_keys(context, secret_name, leaves, &mut existing, &invalidated).await?;
    }
    Ok(())
}

fn invalidated_generators(
    leaves_by_secret: &[(String, Vec<SecretLeaf>)],
    existing: &BTreeMap<Generator, String>,
) -> BTreeSet<Generator> {
    let declared: BTreeSet<Generator> = leaves_by_secret
        .iter()
        .flat_map(|(_, leaves)| leaves.iter().filter_map(|leaf| leaf.generator))
        .collect();
    let mut invalidated: BTreeSet<Generator> = declared
        .iter()
        .copied()
        .filter(|generator| !existing.contains_key(generator))
        .collect();
    loop {
        let cascade: Vec<Generator> = declared
            .iter()
            .copied()
            .filter(|generator| !invalidated.contains(generator))
            .filter(|generator| {
                generator
                    .dependencies()
                    .iter()
                    .any(|dependency| invalidated.contains(dependency))
            })
            .collect();
        if cascade.is_empty() {
            return invalidated;
        }
        invalidated.extend(cascade);
    }
}

async fn collect_existing_values(
    context: &KubeContext,
    leaves_by_secret: &[(String, Vec<SecretLeaf>)],
) -> Result<BTreeMap<Generator, String>, SecretError> {
    let mut existing: BTreeMap<Generator, String> = BTreeMap::new();
    for (secret_name, leaves) in leaves_by_secret {
        let Some(live) = get_secret(context, secret_name).await? else {
            continue;
        };
        for leaf in leaves {
            let Some(generator) = leaf.generator else {
                continue;
            };
            let Some(bytes) = live.get(&leaf.key) else {
                continue;
            };
            let Ok(value) = String::from_utf8(bytes.clone()) else {
                continue;
            };
            if value.is_empty() {
                continue;
            }
            merge_existing_value(&mut existing, generator, value)?;
        }
    }
    Ok(existing)
}

fn merge_existing_value(
    existing: &mut BTreeMap<Generator, String>,
    generator: Generator,
    value: String,
) -> Result<(), SecretError> {
    if let Some(previous) = existing.get(&generator)
        && previous != &value
    {
        return Err(SecretError::GeneratorValueDivergent {
            generator: generator.name().to_owned(),
        });
    }
    existing.insert(generator, value);
    Ok(())
}

async fn fill_generated_keys(
    context: &KubeContext,
    secret_name: &str,
    leaves: &[SecretLeaf],
    existing: &mut BTreeMap<Generator, String>,
    invalidated: &BTreeSet<Generator>,
) -> Result<(), SecretError> {
    let generated: Vec<(&str, Generator)> = leaves
        .iter()
        .filter_map(|leaf| {
            leaf.generator
                .map(|generator| (leaf.key.as_str(), generator))
        })
        .collect();
    if generated.is_empty() {
        return Ok(());
    }
    let live = get_secret(context, secret_name).await?;
    let needs_fill: Vec<(&str, Generator)> = generated
        .iter()
        .copied()
        .filter(|(key, generator)| {
            invalidated.contains(generator)
                || match live.as_ref().and_then(|data| data.get(*key)) {
                    None => true,
                    Some(value) => value.is_empty(),
                }
        })
        .collect();
    if needs_fill.is_empty() {
        return Ok(());
    }
    let mut data: BTreeMap<String, String> = match live {
        Some(existing) => existing
            .into_iter()
            .map(|(key, bytes)| (key, String::from_utf8(bytes).unwrap_or_default()))
            .collect(),
        None => BTreeMap::new(),
    };
    for (key, value) in resolve_generated_values(&needs_fill, existing)? {
        data.insert(key.to_owned(), value);
    }
    upsert_secret(
        context,
        secret_name,
        data,
        BTreeMap::new(),
        None,
        Some(SECRET_FAMILY_CONFIG),
    )
    .await?;
    Ok(())
}

fn resolve_generated_values<'a>(
    needs_fill: &[(&'a str, Generator)],
    resolved: &mut BTreeMap<Generator, String>,
) -> Result<Vec<(&'a str, String)>, SecretError> {
    let mut pending = needs_fill.to_vec();
    let mut filled: Vec<(&'a str, String)> = Vec::with_capacity(pending.len());
    while let Some(index) = pending.iter().position(|(_, generator)| {
        resolved.contains_key(generator)
            || generator
                .dependencies()
                .iter()
                .all(|dependency| resolved.contains_key(dependency))
    }) {
        let (key, generator) = pending.remove(index);
        let value = match resolved.get(&generator) {
            Some(existing) => existing.clone(),
            None => generator.generate(resolved)?,
        };
        resolved.insert(generator, value.clone());
        filled.push((key, value));
    }
    if let Some((_, generator)) = pending.first() {
        let unresolved = generator
            .dependencies()
            .iter()
            .copied()
            .find(|dependency| !resolved.contains_key(dependency));
        return match unresolved {
            Some(dependency) if pending.iter().any(|(_, waiting)| *waiting == dependency) => {
                Err(SecretError::SecretDependencyCyclic {
                    generator: generator.name().to_owned(),
                })
            }
            Some(dependency) => Err(SecretError::SecretDependencyMissing {
                generator: generator.name().to_owned(),
                dependency: dependency.name().to_owned(),
            }),
            None => Err(SecretError::SecretDependencyCyclic {
                generator: generator.name().to_owned(),
            }),
        };
    }
    Ok(filled)
}

pub async fn settle_cluster_configs_and_secrets(context: &KubeContext) -> Result<(), SecretError> {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.kind);
    let configmaps = get_configmaps(context, Some(filter)).await?;
    let mut failures: Vec<SecretError> = Vec::new();
    for (configmap_name, entries) in configmaps {
        let config_file = name_from_kube_resource_name(
            &configmap_name,
            KPROFILE_CONFIGS_FAMILY.scope,
            KPROFILE_CONFIGS_FAMILY.kind,
        )?
        .to_owned();
        let Some((_, bytes)) = entries.into_iter().next() else {
            failures.push(SecretError::ConfigYamlParseFailed {
                config: config_file,
                cause: format!("configmap '{configmap_name}' is empty"),
            });
            continue;
        };
        let config_yaml = match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(error) => {
                failures.push(SecretError::ConfigmapEntryNotUtf8 {
                    configmap: configmap_name.clone(),
                    cause: error.to_string(),
                });
                continue;
            }
        };
        let secret_name = to_secret_name(&config_file)?;
        let live = get_secret(context, &secret_name).await?;
        let actual_keys = live.map(|data| data.into_keys().collect::<BTreeSet<String>>());
        if let Err(error) = settle_secret_keys(&Settlement {
            config: &config_file,
            config_yaml: &config_yaml,
            secret: &secret_name,
            actual_keys,
        }) {
            failures.push(error);
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(SecretError::SettlementBatchFailed {
            failures: format_failures(&failures),
        })
    }
}

fn format_failures(failures: &[SecretError]) -> String {
    let mut out = String::new();
    for failure in failures {
        out.push('\n');
        out.push_str("    - ");
        let text = failure.to_string();
        let mut lines = text.split('\n');
        if let Some(first) = lines.next() {
            out.push_str(first);
        }
        for line in lines {
            out.push('\n');
            out.push_str("      ");
            out.push_str(line);
        }
    }
    out
}

const KEY_INDENT: &str = "    - ";

fn sorted_join(values: &BTreeSet<String>) -> String {
    if values.is_empty() {
        " none.".to_owned()
    } else {
        let mut out = String::new();
        for value in values {
            out.push('\n');
            out.push_str(KEY_INDENT);
            out.push_str(value);
        }
        out
    }
}

fn sorted_join_str(values: &BTreeSet<&str>) -> String {
    if values.is_empty() {
        " none.".to_owned()
    } else {
        let mut out = String::new();
        for value in values {
            out.push('\n');
            out.push_str(KEY_INDENT);
            out.push_str(value);
        }
        out
    }
}

#[cfg(test)]
mod tests;
