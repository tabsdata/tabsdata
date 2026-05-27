//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Api;
use kube::api::{DeleteParams, PostParams};

use crate::base::error::kubernetes::{KubeConfigMapError, KubernetesError};
use crate::kube::api::label_tabsdata;
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
) -> Result<(), KubernetesError> {
    let api: Api<ConfigMap> = Api::namespaced(context.client().clone(), context.namespace());
    let mut metadata = ObjectMeta {
        name: Some(name.to_owned()),
        namespace: Some(context.namespace().to_owned()),
        ..Default::default()
    };
    label_tabsdata(&mut metadata);
    let configmap = ConfigMap {
        metadata,
        data: Some(data),
        ..Default::default()
    };
    api.create(&PostParams::default(), &configmap)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeConfigMapError::CreateFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
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
