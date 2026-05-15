//
// Copyright 2026 Tabs Data Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_system_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 5] = [
        Box::new(SystemError::SubprocessSpawnFailed(
            "td_resource".to_owned(),
            "exec format error".to_owned(),
        )),
        Box::new(SystemError::SubprocessFailed {
            program: "td_resource".to_owned(),
            status: "exit 1".to_owned(),
            stderr: "boom".to_owned(),
        }),
        Box::new(SystemError::WriteFailed(
            "/tmp/x".to_owned(),
            "io".to_owned(),
        )),
        Box::new(SystemError::ReadFailed(
            "/tmp/x".to_owned(),
            "io".to_owned(),
        )),
        Box::new(SystemError::CreateDirectoryFailed(
            "/tmp/x".to_owned(),
            "io".to_owned(),
        )),
    ];
    for error in cases {
        assert_eq!(error.domain(), "SystemError");
    }
}

#[test]
fn display_messages_carry_constructor_args() {
    let error = SystemError::SubprocessSpawnFailed(
        "td_resource".to_owned(),
        "exec format error".to_owned(),
    );
    let display = error.to_string();
    assert!(display.contains("td_resource"));
    assert!(display.contains("exec format error"));

    let error = SystemError::SubprocessFailed {
        program: "kminus".to_owned(),
        status: "exit 2".to_owned(),
        stderr: "config invalid".to_owned(),
    };
    let display = error.to_string();
    assert!(display.contains("kminus"));
    assert!(display.contains("exit 2"));
    assert!(display.contains("config invalid"));

    let error = SystemError::SubprocessOutputNonUtf8("td_resource".to_owned());
    let display = error.to_string();
    assert!(display.contains("td_resource"));
    assert!(display.contains("UTF-8"));
}

#[test]
fn variant_codes_track_assigned_discriminants() {
    assert_eq!(
        SystemError::SubprocessSpawnFailed("x".to_owned(), "y".to_owned()).code(),
        "SystemError::5000"
    );
    assert_eq!(
        SystemError::SubprocessFailed {
            program: "x".to_owned(),
            status: "y".to_owned(),
            stderr: "z".to_owned(),
        }
        .code(),
        "SystemError::5001"
    );
    assert_eq!(
        SystemError::SubprocessOutputNonUtf8("x".to_owned()).code(),
        "SystemError::5002"
    );
    assert_eq!(
        SystemError::WriteFailed("x".to_owned(), "y".to_owned()).code(),
        "SystemError::5003"
    );
    assert_eq!(
        SystemError::CreateDirectoryFailed("x".to_owned(), "y".to_owned()).code(),
        "SystemError::5006"
    );
}
