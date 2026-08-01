//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeRbacError {
    #[error("Could not apply service account '{name}' in namespace '{namespace}': {cause}.")]
    ServiceAccountApplyFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5000,
    #[error("Could not apply role '{name}' in namespace '{namespace}': {cause}.")]
    RoleApplyFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
    #[error("Could not apply role binding '{name}' in namespace '{namespace}': {cause}.")]
    RoleBindingApplyFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5002,
    #[error("Could not query '{name}' in namespace '{namespace}': {cause}.")]
    QueryFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5003,
    #[error("Could not delete '{name}' in namespace '{namespace}': {cause}.")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5004,
    #[error("Could not list authorization resources in namespace '{namespace}': {cause}.")]
    ListFailed { namespace: String, cause: String } = 5005,
    #[error("Authorization manifest declares no metadata.name.")]
    MissingName = 5006,
}
