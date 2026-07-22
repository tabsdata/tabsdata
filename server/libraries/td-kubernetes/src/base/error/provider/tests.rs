//
// Copyright 2026 Tabsdata Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_provider_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 6] = [
        Box::new(ProviderError::ArgumentRequired {
            argument: "provider".to_owned(),
            provider: "docker".to_owned(),
        }),
        Box::new(ProviderError::MutuallyExclusiveArguments {
            first: "name".to_owned(),
            second: "path".to_owned(),
        }),
        Box::new(ProviderError::InstanceFolderExists("/tmp/td".to_owned())),
        Box::new(ProviderError::InstanceFolderNotFound("/tmp/td".to_owned())),
        Box::new(ProviderError::KminusStartFailed("boom".to_owned())),
        Box::new(ProviderError::StagingWriteFailed(
            "/tmp/x".to_owned(),
            "io".to_owned(),
        )),
    ];
    for error in cases {
        assert_eq!(error.domain(), "ProviderError");
    }
}

#[test]
fn struct_variant_display_carries_named_fields() {
    let error = ProviderError::ArgumentRequired {
        argument: "provider".to_owned(),
        provider: "docker".to_owned(),
    };
    let display = error.to_string();
    assert!(display.contains("--provider"));
    assert!(display.contains("docker"));

    let error = ProviderError::InvalidKminusInstance {
        folder: "/tmp/td".to_owned(),
        marker: "sysconfig.yaml".to_owned(),
    };
    let display = error.to_string();
    assert!(display.contains("/tmp/td"));
    assert!(display.contains("sysconfig.yaml"));
}

#[test]
fn tuple_variant_display_carries_positional_args() {
    let error = ProviderError::BinaryNotFound("km".to_owned(), "not in PATH".to_owned());
    let display = error.to_string();
    assert!(display.contains("km"));
    assert!(display.contains("not in PATH"));

    let error =
        ProviderError::StagingFolderRemoveFailed("/tmp/x".to_owned(), "permission".to_owned());
    let display = error.to_string();
    assert!(display.contains("/tmp/x"));
    assert!(display.contains("permission"));
}

#[test]
fn variant_codes_span_input_forbidden_notfound_and_internal_ranges() {
    assert_eq!(
        ProviderError::ArgumentRequired {
            argument: "x".to_owned(),
            provider: "y".to_owned(),
        }
        .code(),
        "ProviderError::0000"
    );
    assert_eq!(
        ProviderError::InstanceFolderExists("x".to_owned()).code(),
        "ProviderError::0100"
    );
    assert_eq!(
        ProviderError::InstanceFolderNotFound("x".to_owned()).code(),
        "ProviderError::1000"
    );
    assert_eq!(
        ProviderError::KminusStartFailed("x".to_owned()).code(),
        "ProviderError::5001"
    );
    assert_eq!(
        ProviderError::StagingReadFailed("x".to_owned(), "y".to_owned()).code(),
        "ProviderError::5005"
    );
}
