//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams};

use crate::base::error::kubernetes::{KubeSecretError, KubernetesError};
use crate::kube::api::{label_tabsdata, part_of_tabsdata};
use crate::kube::context::KubeContext;

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

pub async fn upsert_secret(
    context: &KubeContext,
    name: &str,
    data: BTreeMap<String, String>,
) -> Result<(), KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    let mut metadata = ObjectMeta {
        name: Some(name.to_owned()),
        namespace: Some(context.namespace().to_owned()),
        ..Default::default()
    };
    label_tabsdata(&mut metadata);
    let body = Secret {
        metadata,
        data: Some(
            data.into_iter()
                .map(|(key, value)| (key, ByteString(value.into_bytes())))
                .collect(),
        ),
        ..Default::default()
    };
    match api.get_opt(name).await {
        Ok(Some(existing)) => {
            let mut updated = body;
            updated.metadata.resource_version = existing.metadata.resource_version;
            api.replace(name, &PostParams::default(), &updated)
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
        Ok(None) => api
            .create(&PostParams::default(), &body)
            .await
            .map(|_| ())
            .map_err(|error| {
                KubeSecretError::CreateFailed {
                    namespace: context.namespace().to_owned(),
                    name: name.to_owned(),
                    cause: error.to_string(),
                }
                .into()
            }),
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
    api.delete(name, &DeleteParams::default())
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeSecretError::DeleteFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_secrets(
    context: &KubeContext,
) -> Result<Vec<(String, Vec<String>)>, KubernetesError> {
    let api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = part_of_tabsdata();
    let list_params = ListParams::default().labels_from(&selector);
    let secrets = api.list(&list_params).await.map_err(|error| {
        KubeSecretError::ListFailed {
            namespace: context.namespace().to_owned(),
            label_selector: selector.to_string(),
            cause: error.to_string(),
        }
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
            .chain(
                secret
                    .string_data
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
