//
// Copyright 2026 Tabs Data Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_kubernetes_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 5] = [
        Box::new(KubernetesError::NamespaceNotFound("dev".to_owned())),
        Box::new(KubernetesError::ClusterUnreachable("timeout".to_owned())),
        Box::new(KubernetesError::ClientBuildFailed("tls error".to_owned())),
        Box::new(KubernetesError::KubeconfigReadFailed(
            "/x".to_owned(),
            "permission".to_owned(),
        )),
        Box::new(KubernetesError::NamespaceQueryFailed(
            "dev".to_owned(),
            "401".to_owned(),
        )),
    ];
    for error in cases {
        assert_eq!(error.domain(), "KubernetesError");
    }
}

#[test]
fn display_messages_carry_constructor_args() {
    let display = KubernetesError::NamespaceNotFound("dev".to_owned()).to_string();
    assert!(display.contains("dev"));

    let display = KubernetesError::ClusterUnreachable("timeout".to_owned()).to_string();
    assert!(display.contains("timeout"));
    assert!(display.contains("not reachable"));

    let display = KubernetesError::KubeconfigReadFailed(
        "/etc/kubeconfig".to_owned(),
        "permission denied".to_owned(),
    )
    .to_string();
    assert!(display.contains("/etc/kubeconfig"));
    assert!(display.contains("permission denied"));
}

#[test]
fn variant_codes_track_assigned_discriminants() {
    assert_eq!(
        KubernetesError::NamespaceNotFound("dev".to_owned()).code(),
        "KubernetesError::1000"
    );
    assert_eq!(
        KubernetesError::ClusterUnreachable("x".to_owned()).code(),
        "KubernetesError::5000"
    );
    assert_eq!(
        KubernetesError::ClientBuildFailed("x".to_owned()).code(),
        "KubernetesError::5001"
    );
    assert_eq!(
        KubernetesError::KubeconfigDeleteFailed("p".to_owned(), "e".to_owned()).code(),
        "KubernetesError::5008"
    );
}
