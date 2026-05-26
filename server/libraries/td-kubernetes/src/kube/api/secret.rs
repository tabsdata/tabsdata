//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};

use crate::base::error::kubernetes::{KubeSecretError, KubernetesError};

pub async fn exists_secret(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<bool, KubernetesError> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    match api.get_opt(name).await {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(error) => Err(KubeSecretError::QueryFailed {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn upsert_secret(
    client: &Client,
    namespace: &str,
    name: &str,
    data: BTreeMap<String, Vec<u8>>,
) -> Result<(), KubernetesError> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let body = Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(
            data.into_iter()
                .map(|(key, value)| (key, ByteString(value)))
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
                        namespace: namespace.to_owned(),
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
                    namespace: namespace.to_owned(),
                    name: name.to_owned(),
                    cause: error.to_string(),
                }
                .into()
            }),
        Err(error) => Err(KubeSecretError::QueryFailed {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn delete_secret(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<(), KubernetesError> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    api.delete(name, &DeleteParams::default())
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeSecretError::DeleteFailed {
                namespace: namespace.to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
