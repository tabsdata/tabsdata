//
// Copyright 2026 Tabs Data Inc.
//

use crate::base::error::profile::ProfileError;

use super::*;

#[test]
fn command_error_new_carries_no_source() {
    let error = CommandError::new(ExitStatus::GeneralError);
    assert!(matches!(error.status(), ExitStatus::GeneralError));
    assert!(error.source().is_none());
}

#[test]
fn command_error_with_carries_source() {
    let typed = ProfileError::NotFound("dev".to_owned());
    let td_error = TdError::new(typed);
    let error = CommandError::with(ExitStatus::TabsDataError, td_error);
    assert!(matches!(error.status(), ExitStatus::TabsDataError));
    assert!(error.source().is_some());
}

#[test]
fn abort_returns_err_with_passed_status_and_no_source() {
    let result: CommandResult<()> = abort(ExitStatus::GeneralError);
    let error = result.expect_err("abort returns Err");
    assert!(matches!(error.status(), ExitStatus::GeneralError));
    assert!(error.source().is_none());
}

#[test]
fn abort_supports_each_status_variant() {
    for expected in [
        ExitStatus::NoAction,
        ExitStatus::Cancelled,
        ExitStatus::Aborted,
        ExitStatus::GeneralError,
        ExitStatus::TabsDataError,
    ] {
        let expected_code = expected.code();
        let result: CommandResult<()> = abort(expected);
        let error = result.expect_err("abort returns Err");
        // The returned status must match the requested one regardless of value.
        assert_eq!(error.status().code(), expected_code);
    }
}

#[test]
fn abort_with_returns_err_carrying_typed_source() {
    let result: CommandResult<()> = abort_with(
        ExitStatus::TabsDataError,
        ProfileError::NotFound("dev".to_owned()),
    );
    let error = result.expect_err("abort_with returns Err");
    assert!(matches!(error.status(), ExitStatus::TabsDataError));
    let source = error.source().expect("source preserved");
    assert_eq!(source.domain(), "ProfileError");
}

#[test]
fn abort_with_preserves_typed_error_code_and_message() {
    let result: CommandResult<()> = abort_with(
        ExitStatus::TabsDataError,
        ProfileError::DuplicateFile("config.yaml".to_owned()),
    );
    let error = result.expect_err("abort_with returns Err");
    let source = error.source().expect("source preserved");
    assert_eq!(source.domain(), "ProfileError");
    let display = source.to_string();
    assert!(display.contains("config.yaml"));
    assert!(display.contains("Duplicate file name"));
}
