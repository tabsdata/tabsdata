//
// Copyright 2026 Tabsdata Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_pod_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 4] = [
        Box::new(PodYamlError::ParseFailed("missing field".to_owned())),
        Box::new(PodYamlError::SerializeFailed("io".to_owned())),
        Box::new(PodYamlError::TemplateRegisterFailed("syntax".to_owned())),
        Box::new(PodYamlError::TemplateRenderFailed("undefined".to_owned())),
    ];
    for error in cases {
        assert_eq!(error.domain(), "PodYamlError");
    }
}

#[test]
fn display_messages_carry_constructor_args() {
    let error = PodYamlError::ParseFailed("missing kind at line 3".to_owned());
    let display = error.to_string();
    assert!(display.contains("missing kind at line 3"));
    assert!(display.starts_with("Could not parse"));

    let error = PodYamlError::TemplateRenderFailed("variable 'image' not found".to_owned());
    let display = error.to_string();
    assert!(display.contains("variable 'image' not found"));
    assert!(display.starts_with("Could not render"));
}

#[test]
fn variant_codes_track_assigned_discriminants() {
    assert_eq!(
        PodYamlError::ParseFailed("x".to_owned()).code(),
        "PodYamlError::5000"
    );
    assert_eq!(
        PodYamlError::SerializeFailed("x".to_owned()).code(),
        "PodYamlError::5001"
    );
    assert_eq!(
        PodYamlError::TemplateRegisterFailed("x".to_owned()).code(),
        "PodYamlError::5002"
    );
    assert_eq!(
        PodYamlError::TemplateRenderFailed("x".to_owned()).code(),
        "PodYamlError::5003"
    );
}
