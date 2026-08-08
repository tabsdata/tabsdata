//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeDisruptionError {
    #[error("Could not apply pod disruption budget '{name}': {cause}.")]
    UpsertFailed { name: String, cause: String } = 5000,
    #[error("Could not delete the pod disruption budgets: {cause}.")]
    DeleteFailed { cause: String } = 5001,
    #[error("Pod disruption budget manifest declares no metadata.name.")]
    MissingName = 5002,
}
