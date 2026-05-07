//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use td_common::status::ExitStatus::GeneralError;
use tracing::error;

use crate::exit::{Outcome, abort};

pub async fn configmap_exists(client: &Client, namespace: &str, name: &str) -> Outcome<bool> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    match api.get_opt(name).await {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(error) => {
            error!("Failed to query ConfigMap '{name}' in namespace '{namespace}': {error}");
            abort(GeneralError)
        }
    }
}

pub async fn create_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
    data: BTreeMap<String, String>,
) -> Outcome {
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
        error!("Failed to create ConfigMap '{name}' in namespace '{namespace}': {error}");
        return abort(GeneralError);
    }
    Ok(())
}

pub async fn delete_configmap(client: &Client, namespace: &str, name: &str) -> Outcome {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    if let Err(error) = api.delete(name, &DeleteParams::default()).await {
        error!("Failed to delete ConfigMap '{name}' in namespace '{namespace}': {error}");
        return abort(GeneralError);
    }
    Ok(())
}
