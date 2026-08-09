//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams};
use tracing::debug;

use crate::base::constants::{HTTP_STATUS_CONFLICT, HTTP_STATUS_NOT_FOUND, SECRET_FAMILY_LABEL};
use crate::base::error::kubernetes::{KubeSecretError, KubernetesError};
use crate::kube::api::{LabelFilter, label_tabsdata, tabsdata_selector};
use crate::kube::context::KubeContext;

pub async fn create_secret(
    context: &KubeContext,
    name: &str,
    data: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
    owner: Option<OwnerReference>,
    family: Option<&str>,
) -> Result<(), KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    let mut metadata = ObjectMeta {
        name: Some(name.to_owned()),
        namespace: Some(context.namespace().to_owned()),
        owner_references: owner.map(|reference| vec![reference]),
        labels: Some(labels),
        ..Default::default()
    };
    label_tabsdata(&mut metadata, &family_labels(family));
    let secret = Secret {
        metadata,
        data: Some(to_byte_strings(data)),
        ..Default::default()
    };
    api.create(&PostParams::default(), &secret)
        .await
        .map(|_| ())
        .map_err(|error| match &error {
            kube::Error::Api(response) if response.code == HTTP_STATUS_CONFLICT => {
                KubeSecretError::AlreadyExists {
                    namespace: context.namespace().to_owned(),
                    name: name.to_owned(),
                }
                .into()
            }
            _ => KubeSecretError::CreateFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into(),
        })
}

pub async fn replace_secret(
    context: &KubeContext,
    name: &str,
    data: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
    family: Option<&str>,
) -> Result<(), KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    let existing = match api.get_opt(name).await {
        Ok(Some(secret)) => secret,
        Ok(None) => {
            return Err(KubeSecretError::NotFound {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
            }
            .into());
        }
        Err(error) => {
            return Err(KubeSecretError::QueryFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into());
        }
    };
    let mut metadata = ObjectMeta {
        name: Some(name.to_owned()),
        namespace: Some(context.namespace().to_owned()),
        resource_version: existing.metadata.resource_version,
        owner_references: existing.metadata.owner_references,
        labels: Some(labels),
        ..Default::default()
    };
    label_tabsdata(&mut metadata, &family_labels(family));
    let secret = Secret {
        metadata,
        data: Some(to_byte_strings(data)),
        ..Default::default()
    };
    api.replace(name, &PostParams::default(), &secret)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeSecretError::ReplaceFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn upsert_secret(
    context: &KubeContext,
    name: &str,
    data: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
    owner: Option<OwnerReference>,
    family: Option<&str>,
) -> Result<(), KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    match api.get_opt(name).await {
        Ok(Some(existing)) => {
            let mut metadata = ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(context.namespace().to_owned()),
                resource_version: existing.metadata.resource_version,
                owner_references: existing.metadata.owner_references,
                labels: Some(labels),
                ..Default::default()
            };
            label_tabsdata(&mut metadata, &family_labels(family));
            let secret = Secret {
                metadata,
                data: Some(to_byte_strings(data)),
                ..Default::default()
            };
            api.replace(name, &PostParams::default(), &secret)
                .await
                .map(|_| ())
                .map_err(|error| {
                    KubeSecretError::ReplaceFailed {
                        namespace: context.namespace().to_owned(),
                        name: name.to_owned(),
                        cause: error.to_string(),
                    }
                    .into()
                })
        }
        Ok(None) => {
            let mut metadata = ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(context.namespace().to_owned()),
                owner_references: owner.map(|reference| vec![reference]),
                labels: Some(labels),
                ..Default::default()
            };
            label_tabsdata(&mut metadata, &family_labels(family));
            let secret = Secret {
                metadata,
                data: Some(to_byte_strings(data)),
                ..Default::default()
            };
            api.create(&PostParams::default(), &secret)
                .await
                .map(|_| ())
                .map_err(|error| {
                    KubeSecretError::CreateFailed {
                        namespace: context.namespace().to_owned(),
                        name: name.to_owned(),
                        cause: error.to_string(),
                    }
                    .into()
                })
        }
        Err(error) => Err(KubeSecretError::QueryFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn delete_secret(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == HTTP_STATUS_NOT_FOUND => {
            debug!(
                secret = name,
                namespace = %context.namespace(),
                "secret.delete: already deleted"
            );
            Ok(())
        }
        Err(error) => Err(KubeSecretError::DeleteFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn delete_secrets(context: &KubeContext) -> Result<bool, KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(None);
    let parameters = ListParams::default().labels(&selector.to_string());
    let present = api
        .list(&parameters)
        .await
        .map_err(|error| KubeSecretError::ListFailed {
            namespace: context.namespace().to_owned(),
            label_selector: selector.to_string(),
            cause: error.to_string(),
        })?
        .items;
    for secret in &present {
        if let Some(name) = &secret.metadata.name {
            delete_secret(context, name).await?;
        }
    }
    Ok(!present.is_empty())
}

pub async fn get_secret(
    context: &KubeContext,
    name: &str,
) -> Result<Option<BTreeMap<String, Vec<u8>>>, KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    match api.get_opt(name).await {
        Ok(Some(secret)) => {
            let mut merged: BTreeMap<String, Vec<u8>> = BTreeMap::new();
            for (key, value) in secret.data.into_iter().flatten() {
                merged.insert(key, value.0);
            }
            for (key, value) in secret.string_data.into_iter().flatten() {
                merged.insert(key, value.into_bytes());
            }
            Ok(Some(merged))
        }
        Ok(None) => Ok(None),
        Err(error) => Err(KubeSecretError::ReadFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn get_secrets(
    context: &KubeContext,
    label: Option<LabelFilter<'_>>,
) -> Result<Vec<(String, Vec<(String, Vec<u8>)>)>, KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(label);
    let list_params = ListParams::default().labels_from(&selector);
    let secrets = api
        .list(&list_params)
        .await
        .map_err(|error| KubeSecretError::ListFailed {
            namespace: context.namespace().to_owned(),
            label_selector: selector.to_string(),
            cause: error.to_string(),
        })?;
    let mut fetched = Vec::new();
    for secret in secrets {
        let Some(name) = secret.metadata.name else {
            continue;
        };
        let mut entries: Vec<(String, Vec<u8>)> = secret
            .data
            .into_iter()
            .flatten()
            .map(|(key, value)| (key, value.0))
            .chain(
                secret
                    .string_data
                    .into_iter()
                    .flatten()
                    .map(|(key, value)| (key, value.into_bytes())),
            )
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        fetched.push((name, entries));
    }
    fetched.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(fetched)
}

pub async fn require_secret(
    context: &KubeContext,
    name: &str,
) -> Result<BTreeMap<String, Vec<u8>>, KubernetesError> {
    match get_secret(context, name).await? {
        Some(data) => Ok(data),
        None => Err(KubeSecretError::NotFound {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
        }
        .into()),
    }
}

pub async fn list_secrets(
    context: &KubeContext,
    label: Option<LabelFilter<'_>>,
) -> Result<Vec<(String, Vec<String>)>, KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(label);
    let list_params = ListParams::default().labels_from(&selector);
    let secrets = api
        .list(&list_params)
        .await
        .map_err(|error| KubeSecretError::ListFailed {
            namespace: context.namespace().to_owned(),
            label_selector: selector.to_string(),
            cause: error.to_string(),
        })?;
    let mut listed = Vec::new();
    for secret in secrets {
        let Some(name) = secret.metadata.name else {
            continue;
        };
        let mut keys: Vec<String> = secret
            .data
            .into_iter()
            .flatten()
            .map(|(key, _)| key)
            .chain(secret.string_data.into_iter().flatten().map(|(key, _)| key))
            .collect();
        keys.sort();
        listed.push((name, keys));
    }
    listed.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(listed)
}

pub async fn exists_secret(context: &KubeContext, name: &str) -> Result<bool, KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    match api.get_opt(name).await {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(error) => Err(KubeSecretError::QueryFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

fn family_labels(family: Option<&str>) -> Vec<LabelFilter<'_>> {
    match family {
        Some(value) => vec![LabelFilter::new(SECRET_FAMILY_LABEL, value)],
        None => Vec::new(),
    }
}

fn to_byte_strings(data: BTreeMap<String, String>) -> BTreeMap<String, ByteString> {
    data.into_iter()
        .map(|(key, value)| (key, ByteString(value.into_bytes())))
        .collect()
}
