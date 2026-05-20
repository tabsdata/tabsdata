//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Pod;

use crate::instance::profile::bundle::Axis;
use crate::pod::{ValidationError, kminus as kminus_pod};
use crate::topology::{PROVIDER_AXIS_NAME, PROVIDER_VALUE_K4S, PROVIDER_VALUE_K8S, Topology};

pub struct KminusTopology;

const PROVIDER_VALUES: &[&str] = &[PROVIDER_VALUE_K8S, PROVIDER_VALUE_K4S];

const AXES: &[Axis] = &[Axis {
    name: PROVIDER_AXIS_NAME,
    preferred: PROVIDER_VALUE_K4S,
    known: PROVIDER_VALUES,
}];

impl Topology for KminusTopology {
    fn axes(&self) -> &'static [Axis] {
        AXES
    }

    fn validate_regular_pod(&self, pod: &Pod) -> Vec<ValidationError> {
        kminus_pod::validate(pod)
    }
}
