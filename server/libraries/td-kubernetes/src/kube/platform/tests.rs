//
// Copyright 2026 Tabsdata Inc.
//

use std::process::{ExitStatus, Output};

use k8s_openapi::api::core::v1::{Node, NodeSpec};
use kube::core::Status;
use td_error::TdDomainError;

use super::*;

const EKS_PROVIDER_ID: &str = "aws:///eu-west-1a/i-0123456789abcdef0";
const FARGATE_PROVIDER_ID: &str =
    "aws:///eu-west-1a/fargate-ip-10-0-1-23.eu-west-1.compute.internal";
const GKE_PROVIDER_ID: &str = "gce://project/europe-west1-b/gke-pool-1-abcd";
const AKS_PROVIDER_ID: &str = "azure:///subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachineScaleSets/aks/virtualMachines/0";
const KIND_PROVIDER_ID: &str = "kind://docker/kind/kind-control-plane";
const OPENSTACK_PROVIDER_ID: &str = "openstack:///4f1c1d3a";

const CLOUD_PROVIDERS: [ClusterProvider; 4] = [
    ClusterProvider::Eks,
    ClusterProvider::Fargate,
    ClusterProvider::Gke,
    ClusterProvider::Aks,
];

fn observed(provider_id: &str) -> NodeProbe {
    NodeProbe::ProviderId(Some(provider_id.to_owned()))
}

fn mismatch(scheme: &str) -> PlatformCheck {
    PlatformCheck::Mismatch {
        scheme: Some(scheme.to_owned()),
    }
}

const UNIDENTIFIED: PlatformCheck = PlatformCheck::Mismatch { scheme: None };

fn node(provider_id: Option<&str>) -> Node {
    Node {
        spec: Some(NodeSpec {
            provider_id: provider_id.map(str::to_owned),
            ..NodeSpec::default()
        }),
        ..Node::default()
    }
}

fn api_error(code: u16, message: &str) -> kube::Error {
    kube::Error::Api(Box::new(Status::failure(message, "").with_code(code)))
}

#[cfg(unix)]
fn exit_status(code: i32) -> ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    ExitStatus::from_raw(code << 8)
}

#[cfg(windows)]
fn exit_status(code: i32) -> ExitStatus {
    use std::os::windows::process::ExitStatusExt;
    ExitStatus::from_raw(code as u32)
}

fn exec_failure(stdout: &str, stderr: &str) -> AuthError {
    AuthError::AuthExecRun {
        cmd: "KUBERNETES_EXEC_INFO=\"{\\\"certificate-authority-data\\\":\\\"LS0tLS1CRUdJTi\\\"}\" \"gke-gcloud-auth-plugin\"".to_owned(),
        out: Box::new(Output {
            status: exit_status(1),
            stdout: stdout.as_bytes().to_vec(),
            stderr: stderr.as_bytes().to_vec(),
        }),
    }
}

#[test]
fn provider_id_scheme_reads_the_registered_identifier() {
    assert_eq!(provider_id_scheme(EKS_PROVIDER_ID), Some("aws"));
    assert_eq!(provider_id_scheme(FARGATE_PROVIDER_ID), Some("aws"));
    assert_eq!(provider_id_scheme(GKE_PROVIDER_ID), Some("gce"));
    assert_eq!(provider_id_scheme(AKS_PROVIDER_ID), Some("azure"));
    assert_eq!(provider_id_scheme(KIND_PROVIDER_ID), Some("kind"));
    assert_eq!(provider_id_scheme("  gce://p/z/n  "), Some("gce"));
}

#[test]
fn provider_id_scheme_rejects_identifiers_without_one() {
    assert_eq!(provider_id_scheme(""), None);
    assert_eq!(provider_id_scheme("i-0123456789abcdef0"), None);
    assert_eq!(provider_id_scheme(":///eu-west-1a/i-0"), None);
}

#[test]
fn expected_scheme_maps_each_provider() {
    assert_eq!(expected_scheme(ClusterProvider::Eks), Some("aws"));
    assert_eq!(expected_scheme(ClusterProvider::Fargate), Some("aws"));
    assert_eq!(expected_scheme(ClusterProvider::Gke), Some("gce"));
    assert_eq!(expected_scheme(ClusterProvider::Aks), Some("azure"));
    assert_eq!(expected_scheme(ClusterProvider::Docker), None);
    assert_eq!(expected_scheme(ClusterProvider::Kminus), None);
}

#[test]
fn matching_platforms_pass() {
    assert_eq!(
        decide(ClusterProvider::Eks, &observed(EKS_PROVIDER_ID)),
        PlatformCheck::Match
    );
    assert_eq!(
        decide(ClusterProvider::Fargate, &observed(FARGATE_PROVIDER_ID)),
        PlatformCheck::Match
    );
    assert_eq!(
        decide(ClusterProvider::Eks, &observed(FARGATE_PROVIDER_ID)),
        PlatformCheck::Match
    );
    assert_eq!(
        decide(ClusterProvider::Gke, &observed(GKE_PROVIDER_ID)),
        PlatformCheck::Match
    );
    assert_eq!(
        decide(ClusterProvider::Aks, &observed(AKS_PROVIDER_ID)),
        PlatformCheck::Match
    );
}

#[test]
fn another_cloud_is_a_mismatch() {
    assert_eq!(
        decide(ClusterProvider::Eks, &observed(GKE_PROVIDER_ID)),
        mismatch("gce")
    );
    assert_eq!(
        decide(ClusterProvider::Fargate, &observed(AKS_PROVIDER_ID)),
        mismatch("azure")
    );
    assert_eq!(
        decide(ClusterProvider::Gke, &observed(EKS_PROVIDER_ID)),
        mismatch("aws")
    );
    assert_eq!(
        decide(ClusterProvider::Aks, &observed(GKE_PROVIDER_ID)),
        mismatch("gce")
    );
}

#[test]
fn an_unknown_scheme_is_a_mismatch_for_every_cloud_provider() {
    for provider in CLOUD_PROVIDERS {
        assert_eq!(
            decide(provider, &observed(KIND_PROVIDER_ID)),
            mismatch("kind")
        );
        assert_eq!(
            decide(provider, &observed(OPENSTACK_PROVIDER_ID)),
            mismatch("openstack")
        );
    }
}

#[test]
fn docker_accepts_nodes_without_provider_id() {
    assert_eq!(
        decide(ClusterProvider::Docker, &NodeProbe::ProviderId(None)),
        PlatformCheck::Match
    );
    assert_eq!(
        decide(ClusterProvider::Docker, &observed("  ")),
        PlatformCheck::Match
    );
}

#[test]
fn docker_rejects_any_provider_id() {
    assert_eq!(
        decide(ClusterProvider::Docker, &observed(EKS_PROVIDER_ID)),
        mismatch("aws")
    );
    assert_eq!(
        decide(ClusterProvider::Docker, &observed(KIND_PROVIDER_ID)),
        mismatch("kind")
    );
    assert_eq!(
        decide(ClusterProvider::Docker, &observed("bare-identifier")),
        mismatch("bare-identifier")
    );
}

#[test]
fn missing_evidence_leaves_the_platform_undetermined() {
    for provider in CLOUD_PROVIDERS {
        assert_eq!(
            decide(provider, &NodeProbe::NoNodes),
            PlatformCheck::Undetermined(Undetermined::NoNodes)
        );
        assert_eq!(
            decide(provider, &observed("i-0123")),
            PlatformCheck::Undetermined(Undetermined::UnparsableProviderId("i-0123".to_owned()))
        );
        assert_eq!(
            decide(provider, &NodeProbe::Forbidden("Denied.".to_owned())),
            PlatformCheck::Undetermined(Undetermined::ListForbidden("Denied.".to_owned()))
        );
        assert_eq!(
            decide(provider, &NodeProbe::Failed("Timed out".to_owned())),
            PlatformCheck::Undetermined(Undetermined::ListFailed("Timed out".to_owned()))
        );
    }
    assert_eq!(
        decide(ClusterProvider::Docker, &NodeProbe::NoNodes),
        PlatformCheck::Undetermined(Undetermined::NoNodes)
    );
}

#[test]
fn undetermined_reasons_read_as_sentences() {
    let reasons = [
        Undetermined::NoNodes,
        Undetermined::ListForbidden("nodes is forbidden".to_owned()),
        Undetermined::ListFailed("Connection refused.".to_owned()),
        Undetermined::UnparsableProviderId("i-0123".to_owned()),
    ];
    for reason in reasons {
        let text = reason.to_string();
        assert!(text.starts_with(char::is_uppercase), "{text}");
        assert!(text.ends_with('.'), "{text}");
        assert!(!text.ends_with(".."), "{text}");
    }
}

#[test]
fn cloud_nodes_without_any_provider_id_are_a_mismatch() {
    for provider in CLOUD_PROVIDERS {
        assert_eq!(decide(provider, &NodeProbe::ProviderId(None)), UNIDENTIFIED);
        assert_eq!(decide(provider, &observed("")), UNIDENTIFIED);
        assert_eq!(
            decide(
                provider,
                &probe_from_nodes(vec![node(None), node(Some(" "))])
            ),
            UNIDENTIFIED
        );
    }
}

#[test]
fn the_first_node_with_a_provider_id_decides() {
    let nodes = vec![
        node(None),
        node(Some("")),
        node(Some(EKS_PROVIDER_ID)),
        node(Some(GKE_PROVIDER_ID)),
    ];
    assert_eq!(probe_from_nodes(nodes.clone()), observed(EKS_PROVIDER_ID));
    assert_eq!(
        decide(ClusterProvider::Eks, &probe_from_nodes(nodes.clone())),
        PlatformCheck::Match
    );
    assert_eq!(
        decide(ClusterProvider::Gke, &probe_from_nodes(nodes.clone())),
        mismatch("aws")
    );
    assert_eq!(
        decide(ClusterProvider::Docker, &probe_from_nodes(nodes)),
        mismatch("aws")
    );
    let unparsable = vec![
        node(None),
        node(Some("i-0123")),
        node(Some(EKS_PROVIDER_ID)),
    ];
    assert_eq!(
        decide(ClusterProvider::Eks, &probe_from_nodes(unparsable)),
        PlatformCheck::Undetermined(Undetermined::UnparsableProviderId("i-0123".to_owned()))
    );
}

#[test]
fn docker_accepts_listed_nodes_without_any_provider_id() {
    assert_eq!(
        decide(
            ClusterProvider::Docker,
            &probe_from_nodes(vec![node(None), node(Some("")), Node::default()])
        ),
        PlatformCheck::Match
    );
}

#[test]
fn probe_reads_the_listed_nodes() {
    assert_eq!(probe_from_nodes(Vec::new()), NodeProbe::NoNodes);
    assert_eq!(
        probe_from_nodes(vec![node(Some(GKE_PROVIDER_ID)), node(None)]),
        observed(GKE_PROVIDER_ID)
    );
    assert_eq!(
        probe_from_nodes(vec![node(Some("  gce://p/z/n  "))]),
        observed("gce://p/z/n")
    );
    assert_eq!(
        probe_from_nodes(vec![node(None)]),
        NodeProbe::ProviderId(None)
    );
    assert_eq!(
        probe_from_nodes(vec![Node::default()]),
        NodeProbe::ProviderId(None)
    );
}

#[test]
fn node_listing_errors_never_mean_authentication() {
    assert_eq!(
        probe_from_error(&api_error(403, "nodes is forbidden")),
        NodeProbe::Forbidden("nodes is forbidden".to_owned())
    );
    assert_eq!(
        probe_from_error(&api_error(401, "Unauthorized")),
        NodeProbe::Failed(api_error(401, "Unauthorized").to_string())
    );
    assert_eq!(
        probe_from_error(&api_error(500, "boom")),
        NodeProbe::Failed(api_error(500, "boom").to_string())
    );
}

#[test]
fn an_accepted_review_authenticates() {
    assert_eq!(review_outcome(Ok(()), None), ReviewOutcome::Authenticated);
}

#[test]
fn a_denied_review_still_proves_authentication() {
    let error = api_error(403, "selfsubjectreviews is forbidden");
    assert_eq!(
        review_outcome(Err(&error), None),
        ReviewOutcome::Authenticated
    );
}

#[test]
fn a_missing_review_api_asks_for_the_fallback() {
    let error = api_error(404, "the server could not find the requested resource");
    assert_eq!(
        review_outcome(Err(&error), None),
        ReviewOutcome::Unsupported
    );
}

#[test]
fn a_rejected_review_is_an_authentication_failure() {
    let error = api_error(401, "Unauthorized");
    assert_eq!(
        review_outcome(Err(&error), None),
        ReviewOutcome::Unauthenticated(
            "The cluster rejected the credentials (401 Unauthorized: Unauthorized).".to_owned()
        )
    );
}

#[test]
fn a_failing_credential_command_is_an_authentication_failure() {
    let error = kube::Error::Auth(exec_failure("", "Please run gcloud auth login.\n"));
    let ReviewOutcome::Unauthenticated(detail) =
        review_outcome(Err(&error), Some("gke-gcloud-auth-plugin"))
    else {
        panic!("expected an authentication failure");
    };
    assert!(detail.starts_with("The credential command 'gke-gcloud-auth-plugin' failed ("));
    assert!(detail.ends_with("Please run gcloud auth login."));
}

#[test]
fn other_review_failures_are_not_authentication_failures() {
    let error = api_error(500, "etcd is down");
    assert_eq!(
        review_outcome(Err(&error), None),
        ReviewOutcome::Failed(error.to_string())
    );
}

#[test]
fn authentication_errors_follow_the_review_outcome() {
    assert!(
        authentication_error("ctx", ClusterProvider::Gke, ReviewOutcome::Authenticated).is_none()
    );

    let Some(KubernetesError::Platform(credentials)) = authentication_error(
        "gke_project_zone_cluster",
        ClusterProvider::Eks,
        ReviewOutcome::Unauthenticated(
            "The cluster rejected the credentials (401 Unauthorized).".to_owned(),
        ),
    ) else {
        panic!("expected a platform error");
    };
    assert_eq!(
        credentials.to_string(),
        "The credentials of context 'gke_project_zone_cluster' could not be obtained or used. \
         The cluster rejected the credentials (401 Unauthorized). Check that \
         'gke_project_zone_cluster' is the context meant for provider 'eks'."
    );

    let Some(KubernetesError::Platform(failed)) = authentication_error(
        "ctx",
        ClusterProvider::Aks,
        ReviewOutcome::Failed("Connection refused".to_owned()),
    ) else {
        panic!("expected a platform error");
    };
    assert_eq!(
        failed.to_string(),
        "Could not confirm the identity of context 'ctx' with the cluster. Connection refused."
    );
    assert!(!matches!(
        failed,
        KubePlatformError::CredentialsFailed { .. }
    ));

    let Some(KubernetesError::Platform(unsupported)) =
        authentication_error("ctx", ClusterProvider::Aks, ReviewOutcome::Unsupported)
    else {
        panic!("expected a platform error");
    };
    assert_eq!(
        unsupported.to_string(),
        "Could not confirm the identity of context 'ctx' with the cluster. The cluster serves \
         no self subject review API."
    );
}

#[test]
fn rejection_names_the_unauthorized_answer() {
    assert_eq!(
        auth_failure(&api_error(401, "Unauthorized"), None).as_deref(),
        Some("The cluster rejected the credentials (401 Unauthorized: Unauthorized).")
    );
    assert_eq!(
        describe_rejection(" "),
        "The cluster rejected the credentials (401 Unauthorized)."
    );
    assert_eq!(auth_failure(&api_error(403, "Forbidden"), None), None);
}

#[test]
fn exec_failure_shows_the_command_and_its_stderr_only() {
    let error = exec_failure(
        "{\"token\":\"secret-token\"}",
        "\nF1103 10:00:00 error getting credentials: exit status 1\n\n",
    );
    let detail = describe_auth_failure(&error, Some("gke-gcloud-auth-plugin"));
    assert!(detail.starts_with("The credential command 'gke-gcloud-auth-plugin' failed ("));
    assert!(detail.contains("F1103 10:00:00 error getting credentials: exit status 1"));
    assert!(detail.ends_with("exit status 1."));
    assert!(!detail.contains("KUBERNETES_EXEC_INFO"));
    assert!(!detail.contains("certificate-authority-data"));
    assert!(!detail.contains("secret-token"));
}

#[test]
fn exec_failure_without_stderr_reports_the_exit_status() {
    let detail = describe_auth_failure(&exec_failure("", "  \n"), None);
    assert!(detail.starts_with("The credential command failed ("));
    assert!(detail.ends_with(")."));
    assert!(!detail.contains("KUBERNETES_EXEC_INFO"));
}

#[test]
fn exec_that_cannot_start_names_the_command() {
    let error = AuthError::AuthExecStart(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "No such file or directory",
    ));
    assert_eq!(
        describe_auth_failure(&error, Some("aws")),
        "The credential command 'aws' could not be run: No such file or directory."
    );
}

#[test]
fn other_auth_failures_read_as_sentences() {
    let detail = describe_auth_failure(&AuthError::ExecPluginFailed, None);
    assert_eq!(
        detail,
        "The credentials could not be obtained: exec-plugin response did not contain a status."
    );
    let wrapped = auth_failure(&kube::Error::Auth(AuthError::ExecPluginFailed), None);
    assert_eq!(wrapped.as_deref(), Some(detail.as_str()));
}

#[test]
fn credential_output_keeps_the_last_lines() {
    assert_eq!(credential_output(b""), None);
    assert_eq!(credential_output(b" \n\t\n"), None);
    assert_eq!(
        credential_output(b"  first  \n\nsecond\n").as_deref(),
        Some("first second")
    );
    let many: String = (1..=10).map(|line| format!("line {line}\n")).collect();
    assert_eq!(
        credential_output(many.as_bytes()).as_deref(),
        Some("... line 3 line 4 line 5 line 6 line 7 line 8 line 9 line 10")
    );
}

#[test]
fn sentence_adds_a_period_only_when_missing() {
    assert_eq!(sentence("Done"), "Done.");
    assert_eq!(sentence("Done. "), "Done.");
    assert_eq!(sentence("Done?"), "Done?");
}

#[test]
fn credentials_error_names_context_and_provider() {
    let error = credentials_error(
        "gke_project_zone_cluster",
        ClusterProvider::Eks,
        KubeClientError::AuthFailed(
            "The credential command 'aws' failed (exit status: 1).".to_owned(),
        )
        .into(),
    );
    let KubernetesError::Platform(inner) = error else {
        panic!("expected a platform error");
    };
    assert_eq!(
        inner.to_string(),
        "The credentials of context 'gke_project_zone_cluster' could not be obtained or used. \
         The credential command 'aws' failed (exit status: 1). Check that \
         'gke_project_zone_cluster' is the context meant for provider 'eks'."
    );
    assert_eq!(inner.domain(), "KubePlatformError");
}

#[test]
fn credentials_error_leaves_other_failures_alone() {
    let error = credentials_error(
        "ctx",
        ClusterProvider::Gke,
        KubeClientError::BuildFailed("Bad config".to_owned()).into(),
    );
    assert!(matches!(
        error,
        KubernetesError::Client(KubeClientError::BuildFailed(_))
    ));
}

#[test]
fn mismatch_error_names_the_platform_and_the_way_out() {
    assert_eq!(
        mismatch_error(
            "gke_project_zone_cluster",
            ClusterProvider::Eks,
            Some("gce")
        )
        .to_string(),
        "The context 'gke_project_zone_cluster' reaches a cluster run by 'gce' (Google Cloud), \
         but the provider is 'eks'. Pass --context with a context for eks."
    );
    assert_eq!(
        mismatch_error("docker-desktop", ClusterProvider::Aks, Some("openstack")).to_string(),
        "The context 'docker-desktop' reaches a cluster run by 'openstack' (an unrecognized \
         platform), but the provider is 'aks'. Pass --context with a context for aks."
    );
    assert_eq!(
        mismatch_error(
            "arn:aws:eks:eu-west-1:1:cluster/c",
            ClusterProvider::Docker,
            Some("aws")
        )
        .to_string(),
        "The context 'arn:aws:eks:eu-west-1:1:cluster/c' reaches a cluster run by 'aws' (AWS), \
         but the provider is 'docker'. Pass --context with a context for docker."
    );
    assert_eq!(
        mismatch_error("docker-desktop", ClusterProvider::Eks, None).to_string(),
        "The context 'docker-desktop' reaches a cluster whose nodes carry no provider ID, so it \
         is not run by 'aws' (AWS), but the provider is 'eks'. Pass --context with a context for \
         eks."
    );
    assert_eq!(
        mismatch_error("kind-kind", ClusterProvider::Gke, None).to_string(),
        "The context 'kind-kind' reaches a cluster whose nodes carry no provider ID, so it is \
         not run by 'gce' (Google Cloud), but the provider is 'gke'. Pass --context with a \
         context for gke."
    );
}
