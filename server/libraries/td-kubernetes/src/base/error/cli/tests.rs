//
// Copyright 2026 Tabs Data Inc.
//

use td_error::TdDomainError;

use super::*;

#[test]
fn every_variant_reports_cli_error_as_domain() {
    let cases: [Box<dyn TdDomainError>; 5] = [
        Box::new(CliError::ArgumentRequired {
            argument: "x".to_owned(),
            reason: "y".to_owned(),
        }),
        Box::new(CliError::InvalidCommandLine),
        Box::new(CliError::InvalidShellCommand),
        Box::new(CliError::ShellCommandFailed {
            command: "cd".to_owned(),
            cause: "no such directory".to_owned(),
        }),
        Box::new(CliError::ChangeDirectoryFailed("/tmp".to_owned())),
    ];
    for error in cases {
        assert_eq!(error.domain(), "CliError");
    }
}

#[test]
fn display_messages_carry_constructor_args() {
    let error = CliError::ShellCommandFailed {
        command: "cd".to_owned(),
        cause: "no such directory".to_owned(),
    };
    let display = error.to_string();
    assert!(display.contains("cd"));
    assert!(display.contains("no such directory"));

    let error = CliError::ShellLineEditorFailed("ctrl-c".to_owned());
    let display = error.to_string();
    assert!(display.contains("ctrl-c"));
    assert!(display.starts_with("Shell line editor failed"));

    let error = CliError::ChangeDirectoryFailed("/tmp".to_owned());
    let display = error.to_string();
    assert!(display.contains("/tmp"));
}

#[test]
fn unit_variants_have_static_messages() {
    assert_eq!(
        CliError::InvalidCommandLine.to_string(),
        "Invalid command line."
    );
    assert_eq!(
        CliError::InvalidShellCommand.to_string(),
        "Invalid shell command."
    );
}

#[test]
fn variant_codes_track_assigned_discriminants() {
    assert_eq!(
        CliError::ArgumentRequired {
            argument: "x".to_owned(),
            reason: "y".to_owned(),
        }
        .code(),
        "CliError::0000"
    );
    assert_eq!(CliError::InvalidCommandLine.code(), "CliError::0001");
    assert_eq!(CliError::InvalidShellCommand.code(), "CliError::0002");
    assert_eq!(
        CliError::ShellLineEditorFailed("x".to_owned()).code(),
        "CliError::5000"
    );
    assert_eq!(
        CliError::ShellCommandFailed {
            command: "x".to_owned(),
            cause: "y".to_owned(),
        }
        .code(),
        "CliError::5001"
    );
    assert_eq!(
        CliError::ChangeDirectoryFailed("/tmp".to_owned()).code(),
        "CliError::5002"
    );
}
