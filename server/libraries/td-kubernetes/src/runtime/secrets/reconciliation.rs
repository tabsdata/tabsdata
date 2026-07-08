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

pub struct Reconciliation<'a> {
    pub config: &'a str,
    pub config_yaml: &'a str,
    pub secret: &'a str,
    pub actual_keys: Option<BTreeSet<String>>,
}

pub fn reconcile_secret_keys(input: &Reconciliation<'_>) -> Result<(), SecretError> {
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
        (None, false) => Err(SecretError::ReconciliationSecretMissing {
            config: input.config.to_owned(),
            secret: input.secret.to_owned(),
            expected: sorted_join(&expected),
        }),
        (Some(_), true) => Err(SecretError::ReconciliationSecretUnexpected {
            config: input.config.to_owned(),
            secret: input.secret.to_owned(),
        }),
        (Some(actual), false) => {
            let extra: BTreeSet<&str> = actual.difference(&expected).map(String::as_str).collect();
            let missing: BTreeSet<&str> = expected.difference(actual).map(String::as_str).collect();
            if extra.is_empty() && missing.is_empty() {
                Ok(())
            } else {
                Err(SecretError::ReconciliationKeysMismatch {
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
        fill_generated_keys(context, &secret_name, &leaves).await?;
    }
    Ok(())
}

async fn fill_generated_keys(
    context: &KubeContext,
    secret_name: &str,
    leaves: &[SecretLeaf],
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
    let mut needs_fill: Vec<(&str, Generator)> = generated
        .iter()
        .copied()
        .filter(
            |(key, _)| match live.as_ref().and_then(|data| data.get(*key)) {
                None => true,
                Some(value) => value.is_empty(),
            },
        )
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
    // Values by generator, so a derived generator (e.g. a service token) can read
    // the sibling it's signed from. Seed with values already present in the secret.
    let mut resolved: BTreeMap<Generator, String> = generated
        .iter()
        .filter_map(|(key, generator)| {
            data.get(*key)
                .filter(|value| !value.is_empty())
                .map(|value| (*generator, value.clone()))
        })
        .collect();
    // Topological fill: each step takes a generator whose deps are all resolved
    // and produces it. Whatever is left has a missing or cyclic dependency.
    while let Some(index) = needs_fill.iter().position(|(_, generator)| {
        generator
            .deps()
            .iter()
            .all(|dep| resolved.contains_key(dep))
    }) {
        let (key, generator) = needs_fill.remove(index);
        let value = generator.generate(&resolved)?;
        resolved.insert(generator, value.clone());
        data.insert(key.to_owned(), value);
    }
    if let Some((_, generator)) = needs_fill.first() {
        let dependency = generator
            .deps()
            .iter()
            .copied()
            .find(|dep| !resolved.contains_key(dep))
            .unwrap_or(*generator);
        return Err(SecretError::DerivedSecretMissingDependency {
            generator: generator.name().to_owned(),
            dependency: dependency.name().to_owned(),
        });
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

pub async fn reconcile_cluster_configs_and_secrets(
    context: &KubeContext,
) -> Result<(), SecretError> {
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
        if let Err(error) = reconcile_secret_keys(&Reconciliation {
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
        Err(SecretError::ReconciliationBatchFailed {
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
