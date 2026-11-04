//
// Copyright 2026 Tabsdata Inc.
//

use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use k8s_openapi::api::authentication::v1::SelfSubjectReview;
use k8s_openapi::api::authorization::v1::{
    NonResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
};
use k8s_openapi::api::core::v1::Node;
use kube::api::{Api, ListParams, PostParams};
use kube::client::AuthError;
use kube::{Client, Config};
use tokio::sync::OnceCell;

use crate::base::constants::{
    HTTP_STATUS_FORBIDDEN, HTTP_STATUS_NOT_FOUND, HTTP_STATUS_UNAUTHORIZED,
};
use crate::base::error::kubernetes::{KubeClientError, KubePlatformError, KubernetesError};
use crate::cwarn;
use crate::kube::api::support::client::build_kube_client;
use crate::kube::context::KubeContext;
use crate::runtime::provider::ClusterProvider;

const AUTH_PROVIDER_COMMAND_KEY: &str = "cmd-path";
const CREDENTIAL_OUTPUT_MAX_LINES: usize = 8;
const PROVIDER_ID_SEPARATOR: &str = "://";
const SENTENCE_ENDINGS: [char; 3] = ['.', '!', '?'];
const ACCESS_REVIEW_PATH: &str = "/version";
const ACCESS_REVIEW_VERB: &str = "get";
const NODE_PROBE_LIMIT: u32 = 10;

const SCHEME_AWS: &str = "aws";
const SCHEME_GCE: &str = "gce";
const SCHEME_AZURE: &str = "azure";
const SCHEME_KIND: &str = "kind";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewOutcome {
    Authenticated,
    Unsupported,
    Unauthenticated(String),
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeProbe {
    NoNodes,
    ProviderId(Option<String>),
    Forbidden(String),
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Undetermined {
    NoNodes,
    ListForbidden(String),
    ListFailed(String),
    UnparsableProviderId(String),
}

impl Display for Undetermined {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::NoNodes => "The cluster has no nodes yet.".to_owned(),
            Self::ListForbidden(message) => sentence(&format!(
                "The context is not allowed to list nodes: {message}"
            )),
            Self::ListFailed(message) => sentence(&format!("Nodes could not be listed: {message}")),
            Self::UnparsableProviderId(id) => {
                format!("The node provider ID '{id}' carries no scheme.")
            }
        };
        f.write_str(&text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlatformCheck {
    Match,
    Mismatch { scheme: Option<String> },
    Undetermined(Undetermined),
}

pub fn sentence(text: &str) -> String {
    let text = text.trim();
    if text.ends_with(SENTENCE_ENDINGS) {
        text.to_owned()
    } else {
        format!("{text}.")
    }
}

pub fn expected_scheme(provider: ClusterProvider) -> Option<&'static str> {
    match provider {
        ClusterProvider::Eks | ClusterProvider::Fargate => Some(SCHEME_AWS),
        ClusterProvider::Gke => Some(SCHEME_GCE),
        ClusterProvider::Aks => Some(SCHEME_AZURE),
        ClusterProvider::Docker | ClusterProvider::Kminus => None,
    }
}

pub fn provider_id_scheme(provider_id: &str) -> Option<&str> {
    provider_id
        .trim()
        .split_once(PROVIDER_ID_SEPARATOR)
        .map(|(scheme, _)| scheme)
        .filter(|scheme| !scheme.is_empty())
}

pub fn platform_label(scheme: &str) -> &'static str {
    match scheme {
        SCHEME_AWS => "AWS",
        SCHEME_GCE => "Google Cloud",
        SCHEME_AZURE => "Azure",
        SCHEME_KIND => "kind",
        _ => "an unrecognized platform",
    }
}

pub fn decide(provider: ClusterProvider, probe: &NodeProbe) -> PlatformCheck {
    let provider_id = match probe {
        NodeProbe::NoNodes => return PlatformCheck::Undetermined(Undetermined::NoNodes),
        NodeProbe::Forbidden(message) => {
            return PlatformCheck::Undetermined(Undetermined::ListForbidden(message.clone()));
        }
        NodeProbe::Failed(message) => {
            return PlatformCheck::Undetermined(Undetermined::ListFailed(message.clone()));
        }
        NodeProbe::ProviderId(provider_id) => provider_id
            .as_deref()
            .map(str::trim)
            .filter(|provider_id| !provider_id.is_empty()),
    };
    match (expected_scheme(provider), provider_id) {
        (None, None) => PlatformCheck::Match,
        (None, Some(provider_id)) => PlatformCheck::Mismatch {
            scheme: Some(
                provider_id_scheme(provider_id)
                    .unwrap_or(provider_id)
                    .to_owned(),
            ),
        },
        (Some(_), None) => PlatformCheck::Mismatch { scheme: None },
        (Some(expected), Some(provider_id)) => match provider_id_scheme(provider_id) {
            None => PlatformCheck::Undetermined(Undetermined::UnparsableProviderId(
                provider_id.to_owned(),
            )),
            Some(scheme) if scheme == expected => PlatformCheck::Match,
            Some(scheme) => PlatformCheck::Mismatch {
                scheme: Some(scheme.to_owned()),
            },
        },
    }
}

pub fn mismatch_error(
    context: &str,
    provider: ClusterProvider,
    scheme: Option<&str>,
) -> KubePlatformError {
    let found = match scheme {
        Some(scheme) => format!("a cluster run by '{scheme}' ({})", platform_label(scheme)),
        None => {
            let expected = expected_scheme(provider).unwrap_or_default();
            format!(
                "a cluster whose nodes carry no provider ID, so it is not run by '{expected}' ({})",
                platform_label(expected)
            )
        }
    };
    KubePlatformError::ProviderMismatch {
        context: context.to_owned(),
        found,
        provider: provider.as_str().to_owned(),
    }
}

pub fn credentials_error(
    context: &str,
    provider: ClusterProvider,
    error: KubernetesError,
) -> KubernetesError {
    match error {
        KubernetesError::Client(KubeClientError::AuthFailed(detail)) => {
            KubePlatformError::CredentialsFailed {
                context: context.to_owned(),
                provider: provider.as_str().to_owned(),
                detail,
            }
            .into()
        }
        other => other,
    }
}

pub fn probe_from_nodes(nodes: Vec<Node>) -> NodeProbe {
    if nodes.is_empty() {
        return NodeProbe::NoNodes;
    }
    NodeProbe::ProviderId(
        nodes
            .into_iter()
            .filter_map(|node| node.spec.and_then(|spec| spec.provider_id))
            .map(|provider_id| provider_id.trim().to_owned())
            .find(|provider_id| !provider_id.is_empty()),
    )
}

pub fn probe_from_error(error: &kube::Error) -> NodeProbe {
    match error {
        kube::Error::Api(status) if status.code == HTTP_STATUS_FORBIDDEN => {
            NodeProbe::Forbidden(status.message.clone())
        }
        other => NodeProbe::Failed(other.to_string()),
    }
}

pub fn review_outcome(result: Result<(), &kube::Error>, command: Option<&str>) -> ReviewOutcome {
    let Err(error) = result else {
        return ReviewOutcome::Authenticated;
    };
    if let Some(detail) = auth_failure(error, command) {
        return ReviewOutcome::Unauthenticated(detail);
    }
    match error {
        kube::Error::Api(status) if status.code == HTTP_STATUS_FORBIDDEN => {
            ReviewOutcome::Authenticated
        }
        kube::Error::Api(status) if status.code == HTTP_STATUS_NOT_FOUND => {
            ReviewOutcome::Unsupported
        }
        other => ReviewOutcome::Failed(other.to_string()),
    }
}

pub fn authentication_error(
    context: &str,
    provider: ClusterProvider,
    outcome: ReviewOutcome,
) -> Option<KubernetesError> {
    match outcome {
        ReviewOutcome::Authenticated => None,
        ReviewOutcome::Unauthenticated(detail) => Some(
            KubePlatformError::CredentialsFailed {
                context: context.to_owned(),
                provider: provider.as_str().to_owned(),
                detail,
            }
            .into(),
        ),
        ReviewOutcome::Unsupported => Some(
            KubePlatformError::IdentityCheckFailed {
                context: context.to_owned(),
                cause: "The cluster serves no self subject review API.".to_owned(),
            }
            .into(),
        ),
        ReviewOutcome::Failed(cause) => Some(
            KubePlatformError::IdentityCheckFailed {
                context: context.to_owned(),
                cause: sentence(&cause),
            }
            .into(),
        ),
    }
}

async fn review_self(client: &Client) -> Result<(), kube::Error> {
    let api: Api<SelfSubjectReview> = Api::all(client.clone());
    api.create(&PostParams::default(), &SelfSubjectReview::default())
        .await
        .map(|_| ())
}

async fn review_self_access(client: &Client) -> Result<(), kube::Error> {
    let api: Api<SelfSubjectAccessReview> = Api::all(client.clone());
    let review = SelfSubjectAccessReview {
        spec: SelfSubjectAccessReviewSpec {
            non_resource_attributes: Some(NonResourceAttributes {
                path: Some(ACCESS_REVIEW_PATH.to_owned()),
                verb: Some(ACCESS_REVIEW_VERB.to_owned()),
            }),
            ..SelfSubjectAccessReviewSpec::default()
        },
        ..SelfSubjectAccessReview::default()
    };
    api.create(&PostParams::default(), &review)
        .await
        .map(|_| ())
}

pub async fn authenticate(client: &Client, command: Option<&str>) -> ReviewOutcome {
    match review_outcome(review_self(client).await.as_ref().map(|_| ()), command) {
        ReviewOutcome::Unsupported => review_outcome(
            review_self_access(client).await.as_ref().map(|_| ()),
            command,
        ),
        outcome => outcome,
    }
}

pub async fn probe_nodes(client: &Client) -> NodeProbe {
    let api: Api<Node> = Api::all(client.clone());
    match api
        .list(&ListParams::default().limit(NODE_PROBE_LIMIT))
        .await
    {
        Ok(nodes) => probe_from_nodes(nodes.items),
        Err(error) => probe_from_error(&error),
    }
}

pub fn credential_command(config: &Config) -> Option<String> {
    let auth_info = &config.auth_info;
    auth_info
        .exec
        .as_ref()
        .and_then(|exec| exec.command.clone())
        .or_else(|| {
            auth_info
                .auth_provider
                .as_ref()
                .and_then(|provider| provider.config.get(AUTH_PROVIDER_COMMAND_KEY).cloned())
        })
}

pub fn auth_failure(error: &kube::Error, command: Option<&str>) -> Option<String> {
    match error {
        kube::Error::Auth(auth) => Some(describe_auth_failure(auth, command)),
        kube::Error::Api(status) if status.code == HTTP_STATUS_UNAUTHORIZED => {
            Some(describe_rejection(&status.message))
        }
        kube::Error::Service(source) => {
            let mut cause: Option<&(dyn StdError + 'static)> = Some(source.as_ref());
            while let Some(current) = cause {
                if let Some(auth) = current.downcast_ref::<AuthError>() {
                    return Some(describe_auth_failure(auth, command));
                }
                cause = current.source();
            }
            None
        }
        _ => None,
    }
}

pub fn describe_rejection(message: &str) -> String {
    match message.trim() {
        "" => "The cluster rejected the credentials (401 Unauthorized).".to_owned(),
        message => format!("The cluster rejected the credentials (401 Unauthorized: {message})."),
    }
}

pub fn describe_auth_failure(error: &AuthError, command: Option<&str>) -> String {
    let command = match command {
        Some(command) => format!("The credential command '{command}'"),
        None => "The credential command".to_owned(),
    };
    let text = match error {
        AuthError::AuthExecRun { out, .. } => match credential_output(&out.stderr) {
            Some(message) => format!("{command} failed ({}): {message}", out.status),
            None => format!("{command} failed ({})", out.status),
        },
        AuthError::AuthExecStart(cause) => format!("{command} could not be run: {cause}"),
        other => format!("The credentials could not be obtained: {other}"),
    };
    sentence(&text)
}

pub fn credential_output(bytes: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if lines.is_empty() {
        return None;
    }
    let skipped = lines.len().saturating_sub(CREDENTIAL_OUTPUT_MAX_LINES);
    let kept = lines[skipped..].join(" ");
    Some(if skipped > 0 {
        format!("... {kept}")
    } else {
        kept
    })
}

#[derive(Clone)]
pub struct PlatformGate {
    provider: ClusterProvider,
    context: String,
    verified: Arc<OnceCell<()>>,
}

impl PlatformGate {
    pub fn new(provider: ClusterProvider, context: impl Into<String>) -> Self {
        Self {
            provider,
            context: context.into(),
            verified: Arc::new(OnceCell::new()),
        }
    }

    pub async fn client(&self, config: &Config) -> Result<Client, KubernetesError> {
        let client = build_kube_client(config)
            .map_err(|error| credentials_error(&self.context, self.provider, error))?;
        self.verified
            .get_or_try_init(|| self.verify(&client, config))
            .await?;
        Ok(client)
    }

    pub async fn kube_context(&self, config: &Config) -> Result<KubeContext, KubernetesError> {
        let client = self.client(config).await?;
        Ok(KubeContext::new(
            Arc::new(client),
            config.default_namespace.clone(),
        ))
    }

    async fn verify(&self, client: &Client, config: &Config) -> Result<(), KubernetesError> {
        let outcome = authenticate(client, credential_command(config).as_deref()).await;
        if let Some(error) = authentication_error(&self.context, self.provider, outcome) {
            return Err(error);
        }
        match decide(self.provider, &probe_nodes(client).await) {
            PlatformCheck::Match => Ok(()),
            PlatformCheck::Mismatch { scheme } => {
                Err(mismatch_error(&self.context, self.provider, scheme.as_deref()).into())
            }
            PlatformCheck::Undetermined(reason) => {
                cwarn!(
                    "Could not confirm that context '{}' reaches a cluster for provider '{}'. \
                     {reason} Continuing.",
                    self.context,
                    self.provider.as_str()
                );
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests;
