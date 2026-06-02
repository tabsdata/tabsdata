//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeSet;

use crate::base::constants::{CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY};
use crate::base::error::secret::SecretError;
use crate::base::naming::to_configmap_file;
use crate::kube::api::{LabelFilter, get_configmaps, get_secret};
use crate::kube::context::KubeContext;
use crate::runtime::secrets::discovery::extract_secret_leaves;
use crate::runtime::secrets::source::to_secret_name;

pub struct Reconciliation<'a> {
    pub config: &'a str,
    pub config_yaml: &'a str,
    pub secret: &'a str,
    pub actual_keys: Option<BTreeSet<String>>,
}

pub fn reconcile_secret_keys(input: &Reconciliation<'_>) -> Result<(), SecretError> {
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(input.config_yaml).map_err(|error| {
            SecretError::ConfigYamlParseFailed {
                config: input.config.to_owned(),
                cause: error.to_string(),
            }
        })?;
    let leaves = extract_secret_leaves(input.config, &yaml)?;
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
            let extra: BTreeSet<&str> =
                actual.difference(&expected).map(String::as_str).collect();
            let missing: BTreeSet<&str> =
                expected.difference(actual).map(String::as_str).collect();
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

pub async fn reconcile_cluster_configs_and_secrets(
    context: &KubeContext,
) -> Result<(), SecretError> {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.item);
    let configmaps = get_configmaps(context, Some(filter)).await?;
    let mut failures: Vec<SecretError> = Vec::new();
    for (configmap_name, entries) in configmaps {
        let config_file = to_configmap_file(&KPROFILE_CONFIGS_FAMILY, &configmap_name)?.to_owned();
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
        let secret_name = to_secret_name(&config_file);
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
