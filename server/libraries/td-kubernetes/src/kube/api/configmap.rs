//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};

use crate::base::error::kubernetes::{KubeConfigMapError, KubernetesError};

pub async fn exists_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<bool, KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    match api.get_opt(name).await {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(error) => Err(KubeConfigMapError::QueryFailed {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn create_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
    data: BTreeMap<String, String>,
) -> Result<(), KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let configmap = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    };
    api.create(&PostParams::default(), &configmap)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeConfigMapError::CreateFailed {
                namespace: namespace.to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn get_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<Option<BTreeMap<String, String>>, KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    match api.get_opt(name).await {
        Ok(Some(configmap)) => Ok(Some(configmap.data.unwrap_or_default())),
        Ok(None) => Ok(None),
        Err(error) => Err(KubeConfigMapError::ReadFailed {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn require_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<BTreeMap<String, String>, KubernetesError> {
    match get_configmap(client, namespace, name).await? {
        Some(data) => Ok(data),
        None => Err(KubeConfigMapError::NotFound {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
        .into()),
    }
}

pub async fn delete_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<(), KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    api.delete(name, &DeleteParams::default())
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeConfigMapError::DeleteFailed {
                namespace: namespace.to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
