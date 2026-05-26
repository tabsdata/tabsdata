//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::core::system::exit::{Result, abort};

pub async fn configmap_exists(client: &Client, namespace: &str, name: &str) -> Result<bool> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    match api.get_opt(name).await {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(error) => {
            cerror!("Failed to query ConfigMap '{name}' in namespace '{namespace}': {error}");
            abort(GeneralError)
        }
    }
}

pub async fn create_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
    data: BTreeMap<String, String>,
) -> Result {
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
    if let Err(error) = api.create(&PostParams::default(), &configmap).await {
        cerror!("Failed to create ConfigMap '{name}' in namespace '{namespace}': {error}");
        return abort(GeneralError);
    }
    Ok(())
}

pub async fn read_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<Option<BTreeMap<String, String>>> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    match api.get_opt(name).await {
        Ok(Some(configmap)) => Ok(Some(configmap.data.unwrap_or_default())),
        Ok(None) => Ok(None),
        Err(error) => {
            cerror!("Failed to read ConfigMap '{name}' in namespace '{namespace}': {error}");
            abort(GeneralError)
        }
    }
}

pub async fn require_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<BTreeMap<String, String>> {
    match read_configmap(client, namespace, name).await? {
        Some(data) => Ok(data),
        None => {
            cerror!("ConfigMap '{name}' not found in namespace '{namespace}'.");
            abort(GeneralError)
        }
    }
}

pub async fn delete_configmap(client: &Client, namespace: &str, name: &str) -> Result {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    if let Err(error) = api.delete(name, &DeleteParams::default()).await {
        cerror!("Failed to delete ConfigMap '{name}' in namespace '{namespace}': {error}");
        return abort(GeneralError);
    }
    Ok(())
}
