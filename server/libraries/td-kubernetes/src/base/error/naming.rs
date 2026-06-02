//
// Copyright 2026 Tabs Data Inc.
//

use strum::{AsRefStr, Display};
use td_error::td_error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display)]
#[strum(serialize_all = "lowercase")]
pub enum NameSegment {
    Component,
    Name,
    Runid,
}

#[td_error]
pub enum NamingError {
    #[error(
        "Pod template '{template}' is missing the '{label}' label required for resource naming."
    )]
    PodMissingComponentLabel { template: String, label: String } = 0,
    #[error(
        "Job template '{template}' is missing the '{label}' label required for resource naming."
    )]
    JobMissingComponentLabel { template: String, label: String } = 1,
    #[error("Pod created from template '{template}' is missing metadata field '{field}'.")]
    PodMissingOwnerMetadata { template: String, field: String } = 2,
    #[error("Job created from template '{template}' is missing metadata field '{field}'.")]
    JobMissingOwnerMetadata { template: String, field: String } = 3,
    #[error("Name segment '{segment}' value '{value}' is invalid: {reason}.")]
    InvalidNameSegment {
        segment: NameSegment,
        value: String,
        reason: String,
    } = 4,
    #[error("Assembled kube identifier '{identifier}' exceeds {limit} characters.")]
    AssembledIdentifierTooLong { identifier: String, limit: usize } = 5,
    #[error("Assembled kube identifier '{identifier}' is not valid.")]
    AssembledIdentifierInvalid { identifier: String } = 6,
    #[error(
        "Kube identifier '{identifier}' has {qsegments} dash-separated segments; expected 2 or 3."
    )]
    InvalidSegmentCount {
        identifier: String,
        qsegments: usize,
    } = 7,
    #[error(
        "Assembled configmap name '{name}' is not a valid DNS-1123 subdomain \
         (lowercase alphanumeric, '-', '.' only)."
    )]
    InvalidConfigmapName { name: String } = 8,
}
