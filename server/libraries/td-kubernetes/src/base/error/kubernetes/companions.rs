//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeCompanionsError {
    #[error(
        "Creating the workload '{name}' in namespace '{namespace}' did not run to \
         completion: {cause}."
    )]
    CreationAbandoned {
        namespace: String,
        name: String,
        cause: String,
    } = 5000,
    #[error(
        "Creating the workload '{name}' in namespace '{namespace}' failed, and the \
         partially created workload could not be removed: {cause}. It holds only some \
         of its configmaps and secrets, so it will never start; recovery removes it \
         once it exceeds its dispatch grace."
    )]
    CompensationFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
}
