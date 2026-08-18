//
// Copyright 2026 Tabsdata Inc.
//

use clap::CommandFactory;

use super::*;
use crate::client::tdkserver::arguments::Arguments;

const SHELL_ONLY: [&str; 2] = ["exit", "quit"];
const COMMAND_LINE_ONLY: [&str; 1] = ["shell"];

fn command_names(command: clap::Command) -> Vec<String> {
    let mut names: Vec<String> = command
        .get_subcommands()
        .map(|subcommand| subcommand.get_name().to_owned())
        .collect();
    names.sort_unstable();
    names
}

#[test]
fn the_shell_offers_every_command_line_command() {
    let expected: Vec<String> = command_names(Arguments::command())
        .into_iter()
        .filter(|name| !COMMAND_LINE_ONLY.contains(&name.as_str()))
        .collect();
    let offered = command_names(ShellInput::command());
    for name in expected {
        assert!(
            offered.contains(&name),
            "the shell does not offer '{name}'; it offers {offered:?}"
        );
    }
}

#[test]
fn the_shell_offers_no_unknown_command() {
    let known = command_names(Arguments::command());
    for name in command_names(ShellInput::command()) {
        assert!(
            known.contains(&name) || SHELL_ONLY.contains(&name.as_str()),
            "the shell offers '{name}', which the command line does not have"
        );
    }
}

#[test]
fn the_shell_subcommands_match_the_command_line_ones() {
    let command_line = Arguments::command();
    let shell = ShellInput::command();
    for group in command_line.get_subcommands() {
        let name = group.get_name();
        if COMMAND_LINE_ONLY.contains(&name) || group.get_subcommands().next().is_none() {
            continue;
        }
        let Some(offered) = shell.find_subcommand(name) else {
            continue;
        };
        assert_eq!(
            command_names(group.clone()),
            command_names(offered.clone()),
            "the '{name}' subcommands differ between the command line and the shell"
        );
    }
}
