//
// Copyright 2026 Tabs Data Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_configmap_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 5] = [
        Box::new(ConfigMapError::AlreadyExists {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
        }),
        Box::new(ConfigMapError::NotFound {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
        }),
        Box::new(ConfigMapError::QueryFailed {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
            cause: "401 Unauthorized".to_owned(),
        }),
        Box::new(ConfigMapError::CreateFailed {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
            cause: "conflict".to_owned(),
        }),
        Box::new(ConfigMapError::DeleteFailed {
            namespace: "default".to_owned(),
            name: "tabsdata".to_owned(),
            cause: "not found".to_owned(),
        }),
    ];
    for error in cases {
        assert_eq!(error.domain(), "ConfigMapError");
    }
}

#[test]
fn display_messages_carry_namespace_name_and_cause() {
    let error = ConfigMapError::QueryFailed {
        namespace: "tabsdata-ns".to_owned(),
        name: "td-config".to_owned(),
        cause: "boom".to_owned(),
    };
    let display = error.to_string();
    assert!(display.contains("tabsdata-ns"));
    assert!(display.contains("td-config"));
    assert!(display.contains("boom"));

    let error = ConfigMapError::AlreadyExists {
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
        ConfigMapError::AlreadyExists {
            namespace: "ns".to_owned(),
            name: "td".to_owned(),
        }
        .code(),
        "ConfigMapError::0100"
    );
    assert_eq!(
        ConfigMapError::NotFound {
            namespace: "ns".to_owned(),
            name: "td".to_owned(),
        }
        .code(),
        "ConfigMapError::1000"
    );
    assert_eq!(
        ConfigMapError::QueryFailed {
            namespace: "ns".to_owned(),
            name: "td".to_owned(),
            cause: "x".to_owned(),
        }
        .code(),
        "ConfigMapError::5000"
    );
    assert_eq!(
        ConfigMapError::DeleteFailed {
            namespace: "ns".to_owned(),
            name: "td".to_owned(),
            cause: "x".to_owned(),
        }
        .code(),
        "ConfigMapError::5002"
    );
}
