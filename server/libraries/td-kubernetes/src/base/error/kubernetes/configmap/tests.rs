//
// Copyright 2026 Tabsdata Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_configmap_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 5] = [
        Box::new(KubeConfigMapError::AlreadyExists {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
        }),
        Box::new(KubeConfigMapError::NotFound {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
        }),
        Box::new(KubeConfigMapError::QueryFailed {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
            cause: "401 Unauthorized".to_owned(),
        }),
        Box::new(KubeConfigMapError::CreateFailed {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
            cause: "conflict".to_owned(),
        }),
        Box::new(KubeConfigMapError::DeleteFailed {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
            cause: "not found".to_owned(),
        }),
    ];
    for error in cases {
        assert_eq!(error.domain(), "KubeConfigMapError");
    }
}

#[test]
fn display_messages_carry_namespace_name_and_cause() {
    let error = KubeConfigMapError::QueryFailed {
        namespace: "tabsdata-ns".to_owned(),
        name: "td-config".to_owned(),
        cause: "boom".to_owned(),
    };
    let display = error.to_string();
    assert!(display.contains("tabsdata-ns"));
    assert!(display.contains("td-config"));
    assert!(display.contains("boom"));

    let error = KubeConfigMapError::AlreadyExists {
        namespace: "ns".to_owned(),
        name: "td".to_owned(),
    };
    let display = error.to_string();
    assert!(display.contains("ns"));
    assert!(display.contains("td"));
    assert!(display.contains("already exists"));
}

#[test]
fn variant_codes_track_assigned_discriminants() {
    assert_eq!(
        KubeConfigMapError::AlreadyExists {
            namespace: "ns".to_owned(),
            name: "td".to_owned(),
        }
        .code(),
        "KubeConfigMapError::0100"
    );
    assert_eq!(
        KubeConfigMapError::NotFound {
            namespace: "ns".to_owned(),
            name: "td".to_owned(),
        }
        .code(),
        "KubeConfigMapError::1000"
    );
    assert_eq!(
        KubeConfigMapError::QueryFailed {
            namespace: "ns".to_owned(),
            name: "td".to_owned(),
            cause: "x".to_owned(),
        }
        .code(),
        "KubeConfigMapError::5000"
    );
    assert_eq!(
        KubeConfigMapError::DeleteFailed {
            namespace: "ns".to_owned(),
            name: "td".to_owned(),
            cause: "x".to_owned(),
        }
        .code(),
        "KubeConfigMapError::5003"
    );
}
