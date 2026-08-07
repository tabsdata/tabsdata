//
// Copyright 2026 Tabsdata Inc.
//

use strum::{AsRefStr, Display};
use td_error::td_error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display)]
#[strum(serialize_all = "lowercase")]
pub enum NameSegment {
    Component,
    Name,
    WorkerId,
}

#[td_error]
pub enum NamingError {
    #[error("Pod is missing metadata field '{field}'.")]
    PodMissingOwnerMetadata { field: String } = 2,
    #[error("Job is missing metadata field '{field}'.")]
    JobMissingOwnerMetadata { field: String } = 3,
    #[error("CronJob is missing metadata field '{field}'.")]
    CronJobMissingOwnerMetadata { field: String } = 9,
    #[error("Deployment is missing metadata field '{field}'.")]
    DeploymentMissingOwnerMetadata { field: String } = 10,
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
