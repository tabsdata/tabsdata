//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Pod;

use crate::base::error::pod::PodError;

pub fn deserialize_pod_manifest(yaml: &str) -> Result<Pod, PodError> {
    serde_yaml::from_str(yaml).map_err(|error| PodError::ParseFailed(error.to_string()))
}

pub fn serialize_pod_manifest(pod: &Pod) -> Result<String, PodError> {
    serde_yaml::to_string(pod).map_err(|error| PodError::SerializeFailed(error.to_string()))
}

#[cfg(test)]
mod tests;
