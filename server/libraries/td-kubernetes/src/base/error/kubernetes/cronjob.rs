//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeCronJobError {
    #[error("Could not apply cronjob in namespace '{namespace}': {cause}.")]
    ApplyFailed { namespace: String, cause: String } = 5000,
    #[error("Could not await cronjob '{name}' in namespace '{namespace}': {cause}.")]
    AwaitFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5004,
    #[error(
        "Could not delete cronjobs matching '{label_selector}' in namespace '{namespace}': {cause}."
    )]
    DeleteCollectionFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5001,
    #[error(
        "Could not list cronjobs matching '{label_selector}' in namespace '{namespace}': {cause}."
    )]
    ListFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5002,
    #[error("Could not delete cronjob '{name}' in namespace '{namespace}': {cause}.")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5003,
}
