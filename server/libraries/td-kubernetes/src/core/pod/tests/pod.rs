//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn validation_error_new_stores_path_and_message() {
    let error = ValidationError::new(
        "spec.containers[0].securityContext.privileged",
        "privileged containers are not allowed",
    );
    assert_eq!(error.path, "spec.containers[0].securityContext.privileged");
    assert_eq!(error.message, "privileged containers are not allowed");
}

#[test]
fn validation_error_new_accepts_both_str_and_string() {
    let from_str = ValidationError::new("path", "msg");
    let from_string = ValidationError::new(String::from("path"), String::from("msg"));
    assert_eq!(from_str, from_string);
}

#[test]
fn validation_error_supports_equality() {
    let a = ValidationError::new("p", "m");
    let b = ValidationError::new("p", "m");
    let c = ValidationError::new("q", "m");
    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn validation_error_clones_independently() {
    let original = ValidationError::new("p", "m");
    let cloned = original.clone();
    assert_eq!(original, cloned);
}
