//
// Copyright 2026 Tabs Data Inc.
//

use td_error::TdDomainError;

use super::*;

fn version_mismatch() -> ManifestError {
    ManifestError::VersionMismatch {
        path: "pods/init/foo.yaml".to_owned(),
        found: "1.3.0".to_owned(),
        running: "1.4.0".to_owned(),
    }
}

#[test]
fn every_variant_reports_manifest_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 4] = [
        Box::new(version_mismatch()),
        Box::new(ManifestError::EntryMissing("pods/init/foo.yaml".to_owned())),
        Box::new(ManifestError::VersionParseFailed(
            "garbage".to_owned(),
            "expected semver".to_owned(),
        )),
        Box::new(ManifestError::ParseFailed("bad yaml".to_owned())),
    ];
    for error in cases {
        assert_eq!(error.domain(), "ManifestError");
    }
}

#[test]
fn display_messages_carry_constructor_args() {
    let display = version_mismatch().to_string();
    assert!(display.contains("pods/init/foo.yaml"));
    assert!(display.contains("1.3.0"));
    assert!(display.contains("1.4.0"));

    let display = ManifestError::EntryMissing("pods/init/x.yaml".to_owned()).to_string();
    assert!(display.contains("pods/init/x.yaml"));
    assert!(display.contains("missing"));

    let display = ManifestError::SerializeFailed("io error".to_owned()).to_string();
    assert!(display.contains("io error"));
}

#[test]
fn variant_codes_track_assigned_discriminants() {
    assert_eq!(version_mismatch().code(), "ManifestError::0000");
    assert_eq!(
        ManifestError::EntryMissing("x".to_owned()).code(),
        "ManifestError::1000"
    );
    assert_eq!(
        ManifestError::VersionParseFailed("x".to_owned(), "y".to_owned()).code(),
        "ManifestError::5000"
    );
    assert_eq!(
        ManifestError::ParseFailed("x".to_owned()).code(),
        "ManifestError::5001"
    );
    assert_eq!(
        ManifestError::SerializeFailed("x".to_owned()).code(),
        "ManifestError::5002"
    );
}
