//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn rewrite(arguments: &[&str]) -> Vec<String> {
    help_command_form(arguments.iter().map(|argument| (*argument).to_owned()))
}

#[test]
fn test_help_flag_with_full_becomes_the_help_command() {
    assert_eq!(
        rewrite(&["tdkserver", "--help", "--full"]),
        ["tdkserver", "help", "--full"]
    );
    assert_eq!(
        rewrite(&["tdkserver", "-h", "--full"]),
        ["tdkserver", "help", "--full"]
    );
    assert_eq!(
        rewrite(&["tdkserver", "--help", "--full", "--format", "json"]),
        ["tdkserver", "help", "--full", "--format", "json"]
    );
}

#[test]
fn test_help_flag_without_full_is_left_to_clap() {
    assert_eq!(rewrite(&["tdkserver", "--help"]), ["tdkserver", "--help"]);
    assert_eq!(rewrite(&["tdkserver", "-h"]), ["tdkserver", "-h"]);
}

#[test]
fn test_the_help_command_is_left_untouched() {
    assert_eq!(
        rewrite(&["tdkserver", "help", "--full"]),
        ["tdkserver", "help", "--full"]
    );
}

#[test]
fn test_a_full_flag_of_another_command_is_left_untouched() {
    assert_eq!(
        rewrite(&["tdkserver", "init", "--full"]),
        ["tdkserver", "init", "--full"]
    );
}
