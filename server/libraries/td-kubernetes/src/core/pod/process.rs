//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::core::v1::Pod;

use crate::core::pod::ValidationError;

pub fn validate(pod: &Pod) -> Vec<ValidationError> {
    let _ = pod;
    Vec::new()
}
