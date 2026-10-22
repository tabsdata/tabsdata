//
// Copyright 2026 Tabsdata Inc.
//

use clap::{Arg, ArgAction, Command};

use super::*;

fn plain(text: &str) -> String {
    let mut plain = String::new();
    let mut escape = false;
    for character in text.chars() {
        match (escape, character) {
            (false, '\x1b') => escape = true,
            (true, 'm') => escape = false,
            (true, _) => {}
            (false, _) => plain.push(character),
        }
    }
    plain
}

fn sample() -> Command {
    let mut command = Command::new("sample")
        .bin_name("tool")
        .about("Manage the samples.")
        .arg(
            Arg::new("name")
                .long("name")
                .short('n')
                .required(true)
                .help("Name of the sample."),
        )
        .arg(
            Arg::new("format")
                .long("format")
                .value_parser(["text", "json"])
                .default_value("text")
                .help("Output format."),
        )
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .action(ArgAction::SetTrue)
                .help("Say more."),
        )
        .subcommand(Command::new("list").about("List the samples."));
    command.build();
    command
}

#[test]
fn test_help_has_the_usage_description_and_panels() {
    let help = plain(&render_help(&sample(), 80));
    let lines: Vec<&str> = help.lines().collect();

    assert_eq!(lines[0], "");
    assert!(lines[1].starts_with(" Usage: tool "), "{help}");
    assert!(help.contains(" Manage the samples."), "{help}");
    assert!(help.contains("╭─ Options ─"), "{help}");
    assert!(help.contains("╭─ Commands ─"), "{help}");
    assert!(help.contains("│ list  List the samples."), "{help}");
}

#[test]
fn test_every_panel_line_has_the_full_width() {
    let help = plain(&render_help(&sample(), 80));

    for line in help
        .lines()
        .filter(|line| line.starts_with(['╭', '│', '╰']))
    {
        assert_eq!(display_width(line), 80, "{line}");
    }
}

#[test]
fn test_options_show_flags_values_defaults_and_requirement() {
    let help = plain(&render_help(&sample(), 100));

    let rows: Vec<&str> = help.lines().filter(|line| line.starts_with('│')).collect();
    let name = rows.iter().find(|line| line.contains("--name")).unwrap();
    assert!(name.starts_with("│ *  --name"), "{name}");
    assert!(name.contains("-n"), "{name}");
    assert!(name.contains("NAME"), "{name}");
    let format = rows.iter().find(|line| line.contains("--format")).unwrap();
    assert!(format.contains("[text|json]"), "{format}");
    assert!(format.contains("[default: text]"), "{format}");
    let verbose = rows.iter().find(|line| line.contains("--verbose")).unwrap();
    assert!(!verbose.contains("VERBOSE"), "{verbose}");
}

#[test]
fn test_long_help_wraps_inside_the_panel() {
    let mut command = Command::new("sample").arg(
        Arg::new("comment")
            .long("comment")
            .help("A comment long enough to need more than one line in a narrow panel."),
    );
    command.build();

    let help = plain(&render_help(&command, 50));
    let rows: Vec<&str> = help.lines().filter(|line| line.starts_with('│')).collect();

    assert!(rows.len() > 1, "{help}");
    assert!(rows.iter().all(|line| display_width(line) == 50), "{help}");
}

#[test]
fn test_the_whole_command_line_is_panelled() {
    let mut command = sample();
    panelled(&mut command);

    let help = plain(&command.render_long_help().to_string());
    assert!(help.contains("╭─ Options ─"), "{help}");
    let list = command.find_subcommand_mut("list").unwrap();
    let help = plain(&list.render_long_help().to_string());
    assert!(help.contains("Usage: tool list"), "{help}");
}

#[test]
fn test_a_multicall_grammar_names_each_command_once() {
    let mut grammar = Command::new("")
        .multicall(true)
        .subcommand(Command::new("start").about("Start it."));
    grammar.build();

    let start = grammar.find_subcommand("start").unwrap();
    let help = plain(&render_help(start, 80));
    assert!(help.contains(" Usage: start\n"), "{help}");
}
