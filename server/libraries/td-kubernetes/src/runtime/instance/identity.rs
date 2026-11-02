//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ServiceAccount;

use crate::base::error::launcher::LauncherError;
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::sizing::PodPlacement;
use crate::runtime::instance::tabsdata::{TABSDATA_FILE, TabsdataConfig};

pub fn inject_identity(
    profile: &Profile,
    account_template_key: &str,
    account: &mut ServiceAccount,
) -> Result<(), LauncherError> {
    let Some(config_yaml) = profile.configs.get(TABSDATA_FILE) else {
        return Ok(());
    };
    apply_identity(config_yaml, account).map_err(|cause| LauncherError::IdentityConfigParseFailed {
        config: TABSDATA_FILE.to_owned(),
        account: account_template_key.to_owned(),
        cause: cause.to_string(),
    })
}

pub fn apply_identity(
    config_yaml: &str,
    account: &mut ServiceAccount,
) -> Result<(), serde_yaml::Error> {
    let config = TabsdataConfig::parse(config_yaml)?;
    let placement = PodPlacement::of(account.metadata.labels.as_ref());
    let component = config.component(
        placement.component.as_deref().unwrap_or_default(),
        placement.tier.as_deref(),
    );
    let wanted: Vec<_> = component
        .service_account
        .annotations
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    let annotations = account
        .metadata
        .annotations
        .get_or_insert_with(BTreeMap::new);
    for (key, value) in wanted {
        annotations.insert(key.clone(), value.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
