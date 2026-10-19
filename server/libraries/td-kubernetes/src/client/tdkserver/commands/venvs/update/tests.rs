//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;

use clap::Parser;
use indoc::indoc;

use super::*;

#[derive(Parser)]
struct Probe {
    #[command(flatten)]
    arguments: UpdateArguments,
}

fn named(arguments: &[&str]) -> Vec<Venv> {
    let arguments = ["tdkserver", "--requirements", PROBE]
        .iter()
        .chain(arguments);
    Probe::try_parse_from(arguments)
        .expect("the arguments parse")
        .arguments
        .name
}

const PROBE: &str = "requirements-probe.txt";

fn validate(contents: &str) -> CommandResult {
    validate_requirements(Path::new(PROBE), contents)
}

fn rejection(contents: &str) -> String {
    let error = validate(contents).expect_err("expected the requirements to be rejected");
    error
        .source()
        .expect("the rejection carries the cause")
        .to_string()
}

#[test]
fn an_empty_file_is_accepted() {
    assert!(validate("").is_ok());
}

#[test]
fn pinned_requirements_are_accepted() {
    assert!(
        validate(indoc! {"
        mimesis==19.1.0
        psycopg2-binary==2.9.11
    "})
        .is_ok()
    );
}

#[test]
fn the_license_header_and_blank_lines_are_accepted() {
    let contents = indoc! {"
        #
        # Copyright 2026 Tabsdata Inc.
        #

        mimesis==19.1.0
    "};
    assert!(validate(contents).is_ok());
}

#[test]
fn extras_and_markers_are_accepted() {
    let contents = "requests[security,tests]>=2.8.1; python_version > \"3.8\"\n";
    assert!(validate(contents).is_ok());
}

#[test]
fn unpinned_requirements_are_accepted() {
    assert!(validate("mimesis\n").is_ok());
}

#[test]
fn option_lines_are_accepted() {
    let contents = indoc! {"
        -r other.txt
        --index-url https://pypi.org/simple
        mimesis
    "};
    assert!(validate(contents).is_ok());
}

#[test]
fn trailing_comments_are_accepted() {
    assert!(validate("mimesis==19.1.0  # only for tests\n").is_ok());
}

#[test]
fn indented_requirements_are_accepted() {
    assert!(validate("   mimesis==19.1.0   \n").is_ok());
}

#[test]
fn a_malformed_requirement_is_rejected() {
    let message = rejection(indoc! {"
        mimesis==19.1.0
        this is >>> not a requirement
    "});
    assert!(message.contains(PROBE), "{message}");
    assert!(message.contains("line 2"), "{message}");
}

#[test]
fn an_unsupported_comparison_operator_is_rejected() {
    let message = rejection("mimesis=!=1.0\n");
    assert!(message.contains("line 1"), "{message}");
}

#[test]
fn the_reported_line_skips_comments_and_blanks() {
    let contents = indoc! {"
        #
        # Copyright 2026 Tabsdata Inc.
        #

        mimesis==19.1.0
        >>> broken
    "};
    let message = rejection(contents);
    assert!(message.contains("line 6"), "{message}");
}

#[test]
fn the_first_invalid_line_is_the_one_reported() {
    let message = rejection(indoc! {"
        mimesis
        >>> broken
        <<< also broken
    "});
    assert!(message.contains("line 2"), "{message}");
}

#[test]
fn no_name_selects_every_virtual_environment() {
    assert!(named(&[]).is_empty());
    assert_eq!(selected_venvs(&[]), VENVS);
}

#[test]
fn a_comma_separated_name_selects_each_virtual_environment() {
    assert_eq!(named(&["--name", "fn,td"]), [Venv::Fn, Venv::Td]);
    assert_eq!(selected_venvs(&[Venv::Fn, Venv::Td]), [Venv::Td, Venv::Fn]);
}

#[test]
fn a_repeated_name_selects_its_virtual_environment_once() {
    assert_eq!(
        named(&["--name", "fn", "--name", "fn"]),
        [Venv::Fn, Venv::Fn]
    );
    assert_eq!(selected_venvs(&[Venv::Fn, Venv::Fn]), [Venv::Fn]);
}

#[test]
fn an_unknown_name_is_rejected() {
    let arguments = ["tdkserver", "--requirements", PROBE, "--name", "fn,xx"];
    assert!(Probe::try_parse_from(arguments).is_err());
}
