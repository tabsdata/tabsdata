//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams};

use crate::base::constants::{CONFIGMAP_FAMILY_LABEL, HTTP_STATUS_CONFLICT};
use crate::base::error::kubernetes::{KubeConfigMapError, KubernetesError};
use crate::kube::api::{LabelFilter, label_tabsdata, tabsdata_selector};
use crate::kube::context::KubeContext;

pub async fn exists_configmap(context: &KubeContext, name: &str) -> Result<bool, KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(context.client().clone(), context.namespace());
    match api.get_opt(name).await {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(error) => Err(KubeConfigMapError::QueryFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn create_configmap(
    context: &KubeContext,
    name: &str,
    data: BTreeMap<String, String>,
    owner: Option<OwnerReference>,
    family: Option<&str>,
) -> Result<(), KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(context.client().clone(), context.namespace());
    let mut metadata = ObjectMeta {
        name: Some(name.to_owned()),
        namespace: Some(context.namespace().to_owned()),
        owner_references: owner.map(|reference| vec![reference]),
        ..Default::default()
    };
    let extras: &[LabelFilter<'_>] = match family {
        Some(family) => &[LabelFilter::new(CONFIGMAP_FAMILY_LABEL, family)],
        None => &[],
    };
    label_tabsdata(&mut metadata, extras);
    let configmap = ConfigMap {
        metadata,
        data: Some(data),
        ..Default::default()
    };
    api.create(&PostParams::default(), &configmap)
        .await
        .map(|_| ())
        .map_err(|error| match &error {
            kube::Error::Api(response) if response.code == HTTP_STATUS_CONFLICT => {
                KubeConfigMapError::AlreadyExists {
                    namespace: context.namespace().to_owned(),
                    name: name.to_owned(),
                }
                .into()
            }
            _ => KubeConfigMapError::CreateFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into(),
        })
}

pub async fn get_configmap(
    context: &KubeContext,
    name: &str,
) -> Result<Option<BTreeMap<String, String>>, KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(context.client().clone(), context.namespace());
    match api.get_opt(name).await {
        Ok(Some(configmap)) => Ok(Some(configmap.data.unwrap_or_default())),
        Ok(None) => Ok(None),
        Err(error) => Err(KubeConfigMapError::ReadFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn require_configmap(
    context: &KubeContext,
    name: &str,
) -> Result<BTreeMap<String, String>, KubernetesError> {
    match get_configmap(context, name).await? {
        Some(data) => Ok(data),
        None => Err(KubeConfigMapError::NotFound {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
        }
        .into()),
    }
}

pub async fn delete_configmap(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(context.client().clone(), context.namespace());
    api.delete(name, &DeleteParams::default())
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeConfigMapError::DeleteFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_configmaps(
    context: &KubeContext,
    label: Option<LabelFilter<'_>>,
) -> Result<Vec<(String, Vec<String>)>, KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(label);
    let list_params = ListParams::default().labels_from(&selector);
    let configmaps =
        api.list(&list_params)
            .await
            .map_err(|error| KubeConfigMapError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            })?;
    let mut listed = Vec::new();
    for configmap in configmaps {
        let Some(name) = configmap.metadata.name else {
            continue;
        };
        let mut keys: Vec<String> = configmap
            .data
            .into_iter()
            .flatten()
            .map(|(key, _)| key)
            .chain(
                configmap
                    .binary_data
                    .into_iter()
                    .flatten()
                    .map(|(key, _)| key),
            )
            .collect();
        keys.sort();
        listed.push((name, keys));
    }
    listed.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(listed)
}

pub async fn get_configmaps(
    context: &KubeContext,
    label: Option<LabelFilter<'_>>,
) -> Result<Vec<(String, Vec<(String, Vec<u8>)>)>, KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(label);
    let list_params = ListParams::default().labels_from(&selector);
    let configmaps =
        api.list(&list_params)
            .await
            .map_err(|error| KubeConfigMapError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            })?;
    let mut fetched = Vec::new();
    for configmap in configmaps {
        let Some(name) = configmap.metadata.name else {
            continue;
        };
        let mut entries: Vec<(String, Vec<u8>)> = configmap
            .data
            .into_iter()
            .flatten()
            .map(|(key, value)| (key, value.into_bytes()))
            .chain(
                configmap
                    .binary_data
                    .into_iter()
                    .flatten()
                    .map(|(key, value)| (key, value.0)),
            )
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        fetched.push((name, entries));
    }
    fetched.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(fetched)
}

pub async fn merge_configmap(
    context: &KubeContext,
    name: &str,
    updates: BTreeMap<String, String>,
) -> Result<(), KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(context.client().clone(), context.namespace());
    let mut configmap = match api.get_opt(name).await {
        Ok(Some(configmap)) => configmap,
        Ok(None) => {
            return Err(KubeConfigMapError::NotFound {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
            }
            .into());
        }
        Err(error) => {
            return Err(KubeConfigMapError::QueryFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into());
        }
    };
    configmap
        .data
        .get_or_insert_with(BTreeMap::new)
        .extend(updates);
    api.replace(name, &PostParams::default(), &configmap)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeConfigMapError::ReplaceFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
