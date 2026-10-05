//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use td_python::tabsdata::tabsdata_version;

use crate::base::constants::{CONFIGMAP_TABSDATA_MANIFEST, TD_VERSION_LABEL};
use crate::base::error::manifest::ManifestError;
use crate::core::system::exit::CommandResult;
use crate::kube::api::entity::configmap::get_versioned_configmap;
use crate::kube::context::KubeContext;
use crate::runtime::instance::manifest::{MANIFEST_FILE, Manifest, deserialize_manifest};

pub struct InstanceManifest {
    pub manifest: Manifest,
    pub data: BTreeMap<String, String>,
    pub labels: BTreeMap<String, String>,
    pub resource_version: Option<String>,
}

impl InstanceManifest {
    pub fn stamp(&self) -> Option<&str> {
        self.labels.get(TD_VERSION_LABEL).map(String::as_str)
    }
}

pub async fn read_manifest(
    context: &KubeContext,
    instance: &str,
) -> CommandResult<Option<InstanceManifest>> {
    let Some(configmap) = get_versioned_configmap(context, CONFIGMAP_TABSDATA_MANIFEST).await?
    else {
        return Ok(None);
    };
    let Some(yaml) = configmap.data.get(MANIFEST_FILE) else {
        return Err(ManifestError::EntryMissing(format!(
            "{CONFIGMAP_TABSDATA_MANIFEST}/{MANIFEST_FILE} of instance '{instance}'"
        ))
        .into());
    };
    let manifest = deserialize_manifest(yaml)
        .map_err(|error| ManifestError::ParseFailed(error.to_string()))?;
    Ok(Some(InstanceManifest {
        manifest,
        data: configmap.data,
        labels: configmap.labels,
        resource_version: configmap.resource_version,
    }))
}

pub async fn require_manifest(
    context: &KubeContext,
    instance: &str,
) -> CommandResult<InstanceManifest> {
    read_manifest(context, instance).await?.ok_or_else(|| {
        ManifestError::ConfigmapMissing {
            instance: instance.to_owned(),
            configmap: CONFIGMAP_TABSDATA_MANIFEST.to_owned(),
        }
        .into()
    })
}

pub async fn instance_version(
    context: &KubeContext,
    instance: &str,
    stamped: Option<&str>,
) -> CommandResult<String> {
    if let Some(version) = stamped {
        return Ok(version.to_owned());
    }
    if let Some(read) = read_manifest(context, instance).await? {
        return Ok(read.manifest.version);
    }
    product_version()
}

pub fn product_version() -> CommandResult<String> {
    tabsdata_version()
        .map_err(|error| ManifestError::TabsdataVersionResolveFailed(error.to_string()).into())
}
