//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum ProviderError {
    #[error("Argument '--{argument}' is required for provider '{provider}'.")]
    ArgumentRequired { argument: String, provider: String } = 0,
    #[error("Arguments '--{first}' and '--{second}' are mutually exclusive.")]
    MutuallyExclusiveArguments { first: String, second: String } = 1,
    #[error("Argument '--{argument}' is not valid for provider '{provider}'.")]
    ArgumentNotValidForProvider { argument: String, provider: String } = 2,
    #[error("Instance name '{name}' is invalid: {reason}.")]
    InvalidInstanceName { name: String, reason: String } = 3,
    #[error("Could not resolve the cluster of context '{0}' in the kubeconfig.")]
    ContextClusterMissing(String) = 4,
    #[error(
        "No kubeconfig found. Set the KUBECONFIG environment variable, create ~/.kube/config, or pass --kubeconfig."
    )]
    KubeconfigNotFound = 5,
    #[error(
        "Unable to resolve a kubeconfig context. Pass --context or set a current-context in the kubeconfig."
    )]
    KubeconfigContextMissing = 6,
    #[error("A tabsdata instance already exists in namespace '{0}'.")]
    InstanceAlreadyExists(String) = 7,
    #[error(
        "No tabsdata instance found in namespace '{0}'. Run 'tdkserver server create ...' first."
    )]
    InstanceNotFound(String) = 8,
    #[error("Configmap '{0}' is missing the '{1}' entry.")]
    ConfigmapEntryMissing(String, String) = 9,
    #[error("Folder '{0}' already exists.")]
    InstanceFolderExists(String) = 100,
    #[error("Instance folder '{0}' does not exist.")]
    InstanceFolderNotFound(String) = 1000,
    #[error("Folder '{folder}' is not a valid kminus instance (missing '{marker}').")]
    InvalidKminusInstance { folder: String, marker: String } = 1001,
    #[error("Could not locate the '{0}' binary: {1}.")]
    BinaryNotFound(String, String) = 1002,
    #[error("Could not create instance folder '{0}': {1}.")]
    InstanceFolderCreateFailed(String, String) = 5000,
    #[error("Could not start kminus cluster: {0}.")]
    KminusStartFailed(String) = 5001,
    #[error(
        "Port {port} ({caption}) is already in use. Stop the service currently using it, or \
         configure the instance with different ports."
    )]
    PortInUse { port: u16, caption: String } = 5111,
    #[error(
        "Port {port} ({caption}) is already claimed by the service '{service}' of namespace \
         '{namespace}'. That service never obtained an external address, so the port is held \
         without serving. Delete that instance, or configure this one with different ports."
    )]
    PortClaimedByService {
        port: u16,
        caption: String,
        namespace: String,
        service: String,
    } = 5117,
    #[error(
        "Port {port} ({caption}) was still in use after stopping the instance; it did not release \
         in time."
    )]
    PortReleaseTimeout { port: u16, caption: String } = 5114,
    #[error("Could not render the report table: {0}.")]
    TableRenderFailed(String) = 5112,
    #[error("Could not render the document: {0}.")]
    DocumentRenderFailed(String) = 5113,
    #[error("Could not stop kminus cluster: {0}.")]
    KminusStopFailed(String) = 5002,
    #[error("Could not remove staging folder '{0}': {1}.")]
    StagingFolderRemoveFailed(String, String) = 5003,
    #[error("Could not write staged file '{0}': {1}.")]
    StagingWriteFailed(String, String) = 5004,
    #[error("Could not read staged file '{0}': {1}.")]
    StagingReadFailed(String, String) = 5005,
    #[error("Could not create staging folder '{0}': {1}.")]
    StagingFolderCreateFailed(String, String) = 5006,
    #[error(
        "Staged profile family '{family}' must be flat, but contains the subfolder '{subfolder}'."
    )]
    StagingFamilyNotFlat { family: String, subfolder: String } = 200,
    #[error("Could not resolve the Python scripts folder: {0}.")]
    PythonScriptsFolderResolveFailed(String) = 5100,
    #[error("Could not extract the Python scripts folder name from '{0}'.")]
    PythonScriptsFolderNameInvalid(String) = 5101,
    #[error("Could not create temporary apiserver config file: {0}.")]
    ApiserverTempConfigCreateFailed(String) = 5102,
    #[error("Could not write temporary apiserver config '{0}': {1}.")]
    ApiserverTempConfigWriteFailed(String, String) = 5103,
    #[error("Could not flush temporary apiserver config '{0}': {1}.")]
    ApiserverTempConfigFlushFailed(String, String) = 5104,
    #[error("Could not run '{command}': {cause}.")]
    ApiserverInvocationFailed { command: String, cause: String } = 5105,
    #[error("Command '{command}' failed with status {status}: {stderr}.")]
    ApiserverFailedStatus {
        command: String,
        status: String,
        stderr: String,
    } = 5106,
    #[error("Could not create runtimes folder '{0}': {1}.")]
    RuntimesFolderCreateFailed(String, String) = 5107,
    #[error("Could not create Python virtual environment templates under '{folder}': {cause}.")]
    PyvenvCreateFailed { folder: String, cause: String } = 5108,
    #[error("Could not create images folder '{0}': {1}.")]
    ImagesFolderCreateFailed(String, String) = 5109,
    #[error("Could not copy image tree from '{from}' to '{to}': {cause}.")]
    ImagesCopyFailed {
        from: String,
        to: String,
        cause: String,
    } = 5110,
    #[error(
        "Could not compress the Python virtual environments of '{folder}' into '{image}': {cause}."
    )]
    VenvsImageWriteFailed {
        folder: String,
        image: String,
        cause: String,
    } = 5115,
    #[error(
        "Namespace '{namespace}' is not covered by any fargate profile, so its pods would be \
         scheduled by '{scheduler}' instead of '{expected}' and would never start. Create a \
         profile covering it with: eksctl create fargateprofile --cluster <CLUSTER> \
         --region <REGION> --name fp-{namespace} --namespace '{namespace}'."
    )]
    FargateProfileMissing {
        namespace: String,
        scheduler: String,
        expected: String,
    } = 5116,
    #[error("Failed to handle {phase} secrets: {cause}.")]
    SecretHandlerFailed { phase: String, cause: String } = 5200,
    #[error("Argument --provider is required when no active context is set.")]
    ProviderArgumentRequired = 10,
    #[error(
        "No cluster specification supplied and no active context is set. \
         Pass the required arguments, or run 'tdkserver session pin ...' first."
    )]
    NoClusterSpecification = 11,
    #[error("Could not read the current process's open-files limit: {0}.")]
    NofileLimitReadFailed(String) = 5201,
    #[error(
        "The kminus provider requires at least {required} open files \
         (current soft limit is {soft}, hard limit is {hard}). Raise it with \
         `ulimit -n {required}` and rerun this command."
    )]
    NofileLimitTooLow { required: u64, soft: u64, hard: u64 } = 12,
    #[error("Provider '{0}' is not yet implemented.")]
    NotImplemented(String) = 6000,
    #[error("Command '{0}' is not yet implemented.")]
    CommandNotImplemented(String) = 6001,
    #[error("Could not determine the user home directory.")]
    HomeDirNotFound = 6002,
    #[error(
        "The instance '{name}' is in the '{found}' state, but this command requires \
         it to be in one of these states: {expected}."
    )]
    UnexpectedInstanceState {
        name: String,
        found: String,
        expected: String,
    } = 6003,
    #[error(
        "The instance '{name}' is not properly initialized ({reason}). \
         Run 'tdkserver init --instance {name} ...' first."
    )]
    InstanceNotInitialized { name: String, reason: String } = 6004,
    #[error("No staged instance profile was found at '{0}'.")]
    StagedProfileMissing(String) = 6006,
    #[error(
        "The cluster is not running. \
         Start it with `tdkserver cluster start ...` and try again."
    )]
    ClusterNotRunningKminus = 13,
    #[error(
        "The cluster is not running. tdkserver cannot be used to manage the cluster lifecycle \
         of '{provider}' clusters; start the cluster externally (e.g. via your cloud console or \
         `kubectl`) and try again."
    )]
    ClusterNotRunningExternal { provider: String } = 14,
}

#[cfg(test)]
mod tests;
