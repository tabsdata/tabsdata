//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn arguments(line: &str) -> Vec<String> {
    line.split_whitespace().map(str::to_owned).collect()
}

#[test]
fn a_separate_value_is_masked() {
    assert_eq!(
        mask_command_line(arguments(
            "tdkserver quickstart --openai-key sk-proj-abc123"
        )),
        "tdkserver quickstart --openai-key *****"
    );
}

#[test]
fn a_joined_value_is_masked() {
    assert_eq!(
        mask_command_line(arguments(
            "tdkserver quickstart --openai-key=sk-proj-abc123"
        )),
        "tdkserver quickstart --openai-key=*****"
    );
}

#[test]
fn every_secret_option_is_masked() {
    assert_eq!(
        mask_command_line(arguments(
            "tdkserver quickstart --openai-key sk-1 --anthropic-key=sk-2 --yes"
        )),
        "tdkserver quickstart --openai-key ***** --anthropic-key=***** --yes"
    );
}

#[test]
fn an_ordinary_argument_survives() {
    let line = "tdkserver service start --name an-instance --provider kminus";
    assert_eq!(mask_command_line(arguments(line)), line);
}

#[test]
fn only_the_value_of_a_secret_option_is_hidden() {
    assert_eq!(
        mask_command_line(arguments(
            "tdkserver quickstart --openai-key sk-secret --port 2457"
        )),
        "tdkserver quickstart --openai-key ***** --port 2457"
    );
}

#[test]
fn a_secret_option_without_a_value_masks_nothing_after_it() {
    assert_eq!(
        mask_arguments(arguments("tdkserver quickstart --openai-key")),
        vec!["tdkserver", "quickstart", "--openai-key"]
    );
}

#[test]
fn an_option_that_merely_starts_alike_is_untouched() {
    let line = "tdkserver quickstart --openai-keyring value";
    assert_eq!(mask_command_line(arguments(line)), line);
}
