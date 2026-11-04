//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubePlatformError {
    #[error(
        "The context '{context}' reaches {found}, but the provider is '{provider}'. Pass \
         --context with a context for {provider}."
    )]
    ProviderMismatch {
        context: String,
        found: String,
        provider: String,
    } = 0,
    #[error(
        "The credentials of context '{context}' could not be obtained or used. {detail} Check \
         that '{context}' is the context meant for provider '{provider}'."
    )]
    CredentialsFailed {
        context: String,
        provider: String,
        detail: String,
    } = 4000,
    #[error("Could not confirm the identity of context '{context}' with the cluster. {cause}")]
    IdentityCheckFailed { context: String, cause: String } = 5000,
}
