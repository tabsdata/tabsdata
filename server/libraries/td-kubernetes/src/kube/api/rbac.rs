//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::core::v1::ServiceAccount;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use kube::api::{DeleteParams, ListParams, PostParams};
use kube::core::NamespaceResourceScope;
use kube::{Api, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

use crate::base::constants::HTTP_STATUS_NOT_FOUND;
use crate::base::error::kubernetes::{KubeRbacError, KubernetesError};
use crate::kube::api::{LabelFilter, label_tabsdata, tabsdata_selector};
use crate::kube::context::KubeContext;

fn resource_name<K>(resource: &K) -> Result<String, KubeRbacError>
where
    K: Resource<DynamicType = ()>,
{
    resource
        .meta()
        .name
        .clone()
        .ok_or(KubeRbacError::MissingName)
}

async fn upsert<K>(
    context: &KubeContext,
    resource: &K,
    apply_failed: fn(String, String, String) -> KubeRbacError,
    stamp: Option<&str>,
) -> Result<K, KubernetesError>
where
    K: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + Debug
        + Serialize
        + DeserializeOwned,
{
    let api: Api<K> = Api::namespaced(context.client().clone(), context.namespace());
    let mut resource = resource.clone();
    label_tabsdata(context, resource.meta_mut(), &[], stamp).await?;
    let name = resource_name(&resource)?;
    let namespace = context.namespace().to_owned();
    match api.get_opt(&name).await {
        Ok(Some(existing)) => {
            resource.meta_mut().resource_version = existing.meta().resource_version.clone();
            api.replace(&name, &PostParams::default(), &resource)
                .await
                .map_err(|error| apply_failed(namespace, name, error.to_string()).into())
        }
        Ok(None) => api
            .create(&PostParams::default(), &resource)
            .await
            .map_err(|error| apply_failed(namespace, name, error.to_string()).into()),
        Err(error) => Err(KubeRbacError::QueryFailed {
            namespace,
            name,
            cause: error.to_string(),
        }
        .into()),
    }
}

async fn delete_named<K>(context: &KubeContext, name: &str) -> Result<(), KubernetesError>
where
    K: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + Debug
        + DeserializeOwned,
{
    let api: Api<K> = Api::namespaced(context.client().clone(), context.namespace());
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == HTTP_STATUS_NOT_FOUND => Ok(()),
        Err(error) => Err(KubeRbacError::DeleteFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

async fn delete_all<K>(context: &KubeContext) -> Result<usize, KubernetesError>
where
    K: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + Debug
        + DeserializeOwned,
{
    let api: Api<K> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(None);
    let parameters = ListParams::default().labels(&selector.to_string());
    let present = api
        .list(&parameters)
        .await
        .map_err(|error| KubeRbacError::ListFailed {
            namespace: context.namespace().to_owned(),
            cause: error.to_string(),
        })?
        .items;
    let mut deleted = 0usize;
    for resource in &present {
        if let Some(name) = &resource.meta().name {
            delete_named::<K>(context, name).await?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

pub async fn upsert_service_account(
    context: &KubeContext,
    account: &ServiceAccount,
    stamp: Option<&str>,
) -> Result<ServiceAccount, KubernetesError> {
    upsert(
        context,
        account,
        |namespace, name, cause| KubeRbacError::ServiceAccountApplyFailed {
            namespace,
            name,
            cause,
        },
        stamp,
    )
    .await
}

pub async fn upsert_role(
    context: &KubeContext,
    role: &Role,
    stamp: Option<&str>,
) -> Result<Role, KubernetesError> {
    upsert(
        context,
        role,
        |namespace, name, cause| KubeRbacError::RoleApplyFailed {
            namespace,
            name,
            cause,
        },
        stamp,
    )
    .await
}

pub async fn upsert_role_binding(
    context: &KubeContext,
    binding: &RoleBinding,
    stamp: Option<&str>,
) -> Result<RoleBinding, KubernetesError> {
    upsert(
        context,
        binding,
        |namespace, name, cause| KubeRbacError::RoleBindingApplyFailed {
            namespace,
            name,
            cause,
        },
        stamp,
    )
    .await
}

pub async fn delete_authorizations(context: &KubeContext) -> Result<usize, KubernetesError> {
    let budgets = delete_all::<PodDisruptionBudget>(context).await?;
    let bindings = delete_all::<RoleBinding>(context).await?;
    let roles = delete_all::<Role>(context).await?;
    let accounts = delete_all::<ServiceAccount>(context).await?;
    Ok(budgets + bindings + roles + accounts)
}

pub async fn list_roles(
    context: &KubeContext,
    filter: Option<LabelFilter<'_>>,
) -> Result<Vec<Role>, KubernetesError> {
    let api: Api<Role> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(filter);
    let parameters = ListParams::default().labels(&selector.to_string());
    api.list(&parameters)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeRbacError::ListFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
