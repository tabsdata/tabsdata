//
// Copyright 2026 Tabsdata Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_profile_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 5] = [
        Box::new(ProfileError::NotFound("dev".to_owned())),
        Box::new(ProfileError::InvalidPathComponent(
            "foo".to_owned(),
            "bar".to_owned(),
        )),
        Box::new(ProfileError::DuplicateFile("config.yaml".to_owned())),
        Box::new(ProfileError::KprofilesPathInvalid("/missing".to_owned())),
        Box::new(ProfileError::ResourceBinaryNotFound(
            "td_resource".to_owned(),
            "not in PATH".to_owned(),
        )),
    ];
    for error in cases {
        assert_eq!(error.domain(), "ProfileError");
    }
}

#[test]
fn display_messages_carry_constructor_args() {
    let error = ProfileError::NotFound("dev".to_owned());
    let display = error.to_string();
    assert!(display.contains("dev"));
    assert!(display.contains("not found"));

    let error = ProfileError::InvalidPathComponent(
        "foo_bar".to_owned(),
        "pods/init/foo_bar.yaml".to_owned(),
    );
    let display = error.to_string();
    assert!(display.contains("foo_bar"));
    assert!(display.contains("pods/init/foo_bar.yaml"));

    let error = ProfileError::DuplicateFile("config.yaml".to_owned());
    let display = error.to_string();
    assert!(display.contains("config.yaml"));
    assert!(display.contains("Duplicate"));

    let error = ProfileError::ReadFailed("/tmp/x".to_owned(), "permission denied".to_owned());
    let display = error.to_string();
    assert!(display.contains("/tmp/x"));
    assert!(display.contains("permission denied"));
}

#[test]
fn variant_codes_track_assigned_discriminants() {
    assert_eq!(
        ProfileError::NotFound("x".to_owned()).code(),
        "ProfileError::0000"
    );
    assert_eq!(
        ProfileError::DuplicateFile("x".to_owned()).code(),
        "ProfileError::0002"
    );
    assert_eq!(
        ProfileError::KprofilesPathInvalid("x".to_owned()).code(),
        "ProfileError::1000"
    );
    assert_eq!(
        ProfileError::ResourceBinaryNotFound("x".to_owned(), "y".to_owned()).code(),
        "ProfileError::5000"
    );
    assert_eq!(
        ProfileError::EntrySerializeFailed("x".to_owned(), "y".to_owned()).code(),
        "ProfileError::5006"
    );
}
