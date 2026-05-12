//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum ProviderError {
    #[error("Argument '--{argument}' is required for provider '{provider}'")]
    ArgumentRequired { argument: String, provider: String } = 0,
    #[error("Arguments '--{first}' and '--{second}' are mutually exclusive")]
    MutuallyExclusiveArguments { first: String, second: String } = 1,
    #[error("Argument '--{argument}' is not valid for provider '{provider}'")]
    ArgumentNotValidForProvider { argument: String, provider: String } = 2,
    #[error("Instance name '{name}' is invalid: {reason}")]
    InvalidInstanceName { name: String, reason: String } = 3,
    #[error("Could not resolve the cluster of context '{0}' in the kubeconfig")]
    ContextClusterMissing(String) = 4,
    #[error("Folder '{0}' already exists")]
    InstanceFolderExists(String) = 100,
    #[error("Instance folder '{0}' does not exist")]
    InstanceFolderNotFound(String) = 1000,
    #[error("Folder '{folder}' is not a valid kminus instance (missing '{marker}')")]
    InvalidKminusInstance { folder: String, marker: String } = 1001,
    #[error("Could not locate the '{0}' binary: {1}")]
    BinaryNotFound(String, String) = 1002,
    #[error("Could not create instance folder '{0}': {1}")]
    InstanceFolderCreateFailed(String, String) = 5000,
    #[error("Could not start kminus cluster: {0}")]
    KminusStartFailed(String) = 5001,
    #[error("Could not stop kminus cluster: {0}")]
    KminusStopFailed(String) = 5002,
    #[error("Could not remove staging folder '{0}': {1}")]
    StagingFolderRemoveFailed(String, String) = 5003,
    #[error("Could not write staged file '{0}': {1}")]
    StagingWriteFailed(String, String) = 5004,
    #[error("Could not read staged file '{0}': {1}")]
    StagingReadFailed(String, String) = 5005,
}
