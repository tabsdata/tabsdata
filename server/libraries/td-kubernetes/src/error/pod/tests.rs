//
// Copyright 2026 Tabs Data Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_pod_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 4] = [
        Box::new(PodError::ParseFailed("missing field".to_owned())),
        Box::new(PodError::SerializeFailed("io".to_owned())),
        Box::new(PodError::TemplateRegisterFailed("syntax".to_owned())),
        Box::new(PodError::TemplateRenderFailed("undefined".to_owned())),
    ];
    for error in cases {
        assert_eq!(error.domain(), "PodError");
    }
}

#[test]
fn display_messages_carry_constructor_args() {
    let error = PodError::ParseFailed("missing kind at line 3".to_owned());
    let display = error.to_string();
    assert!(display.contains("missing kind at line 3"));
    assert!(display.starts_with("Could not parse"));

    let error = PodError::TemplateRenderFailed("variable 'image' not found".to_owned());
    let display = error.to_string();
    assert!(display.contains("variable 'image' not found"));
    assert!(display.starts_with("Could not render"));
}

#[test]
fn variant_codes_track_assigned_discriminants() {
    assert_eq!(
        PodError::ParseFailed("x".to_owned()).code(),
        "PodError::5000"
    );
    assert_eq!(
        PodError::SerializeFailed("x".to_owned()).code(),
        "PodError::5001"
    );
    assert_eq!(
        PodError::TemplateRegisterFailed("x".to_owned()).code(),
        "PodError::5002"
    );
    assert_eq!(
        PodError::TemplateRenderFailed("x".to_owned()).code(),
        "PodError::5003"
    );
}
