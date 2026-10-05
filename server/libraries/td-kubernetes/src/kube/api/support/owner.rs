//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::Resource;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use crate::base::constants::{KUBE_METADATA_FIELD_NAME, KUBE_METADATA_FIELD_UID};
use crate::base::error::naming::NamingError;

pub fn owner_ref_for_pod(pod: &Pod) -> Result<OwnerReference, NamingError> {
    let name = pod
        .metadata
        .name
        .clone()
        .ok_or_else(|| NamingError::PodMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_NAME.to_owned(),
        })?;
    let uid = pod
        .metadata
        .uid
        .clone()
        .ok_or_else(|| NamingError::PodMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_UID.to_owned(),
        })?;
    Ok(OwnerReference {
        api_version: <Pod as Resource>::API_VERSION.to_owned(),
        kind: <Pod as Resource>::KIND.to_owned(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}

pub fn owner_ref_for_job(job: &Job) -> Result<OwnerReference, NamingError> {
    let name = job
        .metadata
        .name
        .clone()
        .ok_or_else(|| NamingError::JobMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_NAME.to_owned(),
        })?;
    let uid = job
        .metadata
        .uid
        .clone()
        .ok_or_else(|| NamingError::JobMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_UID.to_owned(),
        })?;
    Ok(OwnerReference {
        api_version: <Job as Resource>::API_VERSION.to_owned(),
        kind: <Job as Resource>::KIND.to_owned(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}

pub fn owner_ref_for_deployment(deployment: &Deployment) -> Result<OwnerReference, NamingError> {
    let name = deployment.metadata.name.clone().ok_or_else(|| {
        NamingError::DeploymentMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_NAME.to_owned(),
        }
    })?;
    let uid = deployment.metadata.uid.clone().ok_or_else(|| {
        NamingError::DeploymentMissingOwnerMetadata {
            field: KUBE_METADATA_FIELD_UID.to_owned(),
        }
    })?;
    Ok(OwnerReference {
        api_version: <Deployment as Resource>::API_VERSION.to_owned(),
        kind: <Deployment as Resource>::KIND.to_owned(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}

pub fn owner_ref_for_cronjob(cronjob: &CronJob) -> Result<OwnerReference, NamingError> {
    let name =
        cronjob
            .metadata
            .name
            .clone()
            .ok_or_else(|| NamingError::CronJobMissingOwnerMetadata {
                field: KUBE_METADATA_FIELD_NAME.to_owned(),
            })?;
    let uid =
        cronjob
            .metadata
            .uid
            .clone()
            .ok_or_else(|| NamingError::CronJobMissingOwnerMetadata {
                field: KUBE_METADATA_FIELD_UID.to_owned(),
            })?;
    Ok(OwnerReference {
        api_version: <CronJob as Resource>::API_VERSION.to_owned(),
        kind: <CronJob as Resource>::KIND.to_owned(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}
