//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::core::v1::Service;
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams};

use crate::base::constants::HTTP_STATUS_NOT_FOUND;
use crate::base::error::kubernetes::{KubeServiceError, KubernetesError};
use crate::kube::api::{LabelFilter, label_tabsdata, tabsdata_selector};
use crate::kube::context::KubeContext;

pub async fn upsert_service(
    context: &KubeContext,
    service: &Service,
) -> Result<Service, KubernetesError> {
    let api: Api<Service> = Api::namespaced(context.client().clone(), context.namespace());
    let mut service = service.clone();
    label_tabsdata(&mut service.metadata, &[]);
    let name = service
        .metadata
        .name
        .clone()
        .ok_or(KubeServiceError::MissingName)?;
    match api.get_opt(&name).await {
        Ok(Some(existing)) => {
            service.metadata.resource_version = existing.metadata.resource_version;
            if let Some(spec) = service.spec.as_mut()
                && let Some(cluster_ip) = existing.spec.and_then(|spec| spec.cluster_ip)
            {
                spec.cluster_ip = Some(cluster_ip);
            }
            api.replace(&name, &PostParams::default(), &service)
                .await
                .map_err(|error| {
                    KubeServiceError::ReplaceFailed {
                        namespace: context.namespace().to_owned(),
                        name: name.clone(),
                        cause: error.to_string(),
                    }
                    .into()
                })
        }
        Ok(None) => api
            .create(&PostParams::default(), &service)
            .await
            .map_err(|error| {
                KubeServiceError::ApplyFailed {
                    namespace: context.namespace().to_owned(),
                    name: name.clone(),
                    cause: error.to_string(),
                }
                .into()
            }),
        Err(error) => Err(KubeServiceError::QueryFailed {
            namespace: context.namespace().to_owned(),
            name,
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn delete_service(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<Service> = Api::namespaced(context.client().clone(), context.namespace());
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == HTTP_STATUS_NOT_FOUND => Ok(()),
        Err(error) => Err(KubeServiceError::DeleteFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn delete_services(context: &KubeContext) -> Result<bool, KubernetesError> {
    let api: Api<Service> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(None);
    let parameters = ListParams::default().labels(&selector.to_string());
    let present = api
        .list(&parameters)
        .await
        .map_err(|error| KubeServiceError::ListFailed {
            namespace: context.namespace().to_owned(),
            cause: error.to_string(),
        })?
        .items;
    for service in &present {
        if let Some(name) = &service.metadata.name {
            delete_service(context, name).await?;
        }
    }
    Ok(!present.is_empty())
}

pub async fn list_services(
    context: &KubeContext,
    filter: Option<LabelFilter<'_>>,
) -> Result<Vec<Service>, KubernetesError> {
    let api: Api<Service> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(filter);
    let parameters = ListParams::default().labels(&selector.to_string());
    api.list(&parameters)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeServiceError::ListFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_services_everywhere(
    context: &KubeContext,
    filter: Option<LabelFilter<'_>>,
) -> Result<Vec<Service>, KubernetesError> {
    let api: Api<Service> = Api::all(context.client().clone());
    let selector = tabsdata_selector(filter);
    let parameters = ListParams::default().labels(&selector.to_string());
    api.list(&parameters)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeServiceError::ListEverywhereFailed {
                cause: error.to_string(),
            }
            .into()
        })
}
