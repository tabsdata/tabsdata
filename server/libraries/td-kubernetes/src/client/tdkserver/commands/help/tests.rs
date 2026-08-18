//
// Copyright 2026 Tabsdata Inc.
//

use clap::Parser;

use super::*;

fn tree() -> Value {
    serde_json::from_str(&build_full_help(HelpFormat::Json)).expect("full help is not valid json")
}

fn names(node: &Value) -> Vec<String> {
    node["commands"]
        .as_array()
        .map(|commands| {
            commands
                .iter()
                .map(|command| command["name"].as_str().unwrap_or_default().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn child<'a>(node: &'a Value, name: &str) -> &'a Value {
    node["commands"]
        .as_array()
        .expect("node has no commands")
        .iter()
        .find(|command| command["name"] == name)
        .unwrap_or_else(|| panic!("no '{name}' command"))
}

fn options(node: &Value, collected: &mut Vec<Value>) {
    if let Some(node_options) = node["options"].as_array() {
        collected.extend(node_options.iter().cloned());
    }
    for command in node["commands"].as_array().unwrap_or(&Vec::new()) {
        options(command, collected);
    }
}

#[test]
fn test_help_command_declares_full_and_format() {
    let mut root = Arguments::command();
    root.build();
    let help = root
        .find_subcommand(HELP_COMMAND)
        .expect("no 'help' command");
    for name in [FULL_ARGUMENT, FORMAT_ARGUMENT] {
        assert!(
            help.get_arguments()
                .any(|argument| argument.get_id() == name),
            "'help' does not declare '{name}'"
        );
    }
}

#[test]
fn test_help_format_rejects_unknown_formats() {
    let parsed = Arguments::try_parse_from([PROGRAM_NAME, HELP_COMMAND, "--format", "yaml"]);
    assert!(parsed.is_err());
}

#[test]
fn test_full_help_text_lists_nested_commands() {
    let text = build_full_help(HelpFormat::Text);
    assert!(text.contains("tdkserver quickstart"));
    assert!(text.contains("tdkserver info third-party"));
    assert!(text.contains("tdkserver config get"));
}

#[test]
fn test_full_help_json_is_a_command_tree() {
    let tree = tree();
    assert_eq!(tree["name"], PROGRAM_NAME);
    assert_eq!(tree["path"], PROGRAM_NAME);
    let top: Vec<String> = names(&tree);
    for command in [
        "quickstart",
        "init",
        "create",
        "start",
        "stop",
        "info",
        "shell",
    ] {
        assert!(top.contains(&command.to_owned()), "missing '{command}'");
    }
    let info = child(&tree, "info");
    assert_eq!(info["path"], "tdkserver info");
    assert!(names(info).contains(&"third-party".to_owned()));
}

#[test]
fn test_full_help_includes_the_shell_grammar() {
    let tree = tree();
    let shell = child(&tree, SHELL_COMMAND);
    let shell_commands = names(shell);
    for command in ["help", "exit", "quit", "quickstart", "create", "settings"] {
        assert!(
            shell_commands.contains(&command.to_owned()),
            "missing shell command '{command}'"
        );
    }
    let info = child(shell, "info");
    assert_eq!(info["path"], "tdkserver shell info");
    assert!(!names(info).is_empty());
}

#[test]
fn test_full_help_lists_the_help_command_once() {
    let tree = tree();
    assert!(names(&tree).contains(&HELP_COMMAND.to_owned()));
    assert!(!names(child(&tree, "info")).contains(&HELP_COMMAND.to_owned()));
    assert!(!build_full_help(HelpFormat::Text).contains("tdkserver info help"));
}

#[test]
fn test_full_help_marks_options_optional() {
    let text = build_full_help(HelpFormat::Text);
    assert!(text.contains("(optional)"));

    let mut collected = Vec::new();
    options(&tree(), &mut collected);
    let required: Vec<bool> = collected
        .iter()
        .filter_map(|option| option["required"].as_bool())
        .collect();
    assert!(required.contains(&false));
}

#[test]
fn test_full_help_describes_option_shapes() {
    let tree = tree();
    let quickstart = child(&tree, "quickstart");
    let options = quickstart["options"].as_array().expect("no options");

    let verbose = options
        .iter()
        .find(|option| option["opts"][0] == "--verbose")
        .expect("no --verbose option");
    assert_eq!(verbose["is_flag"], true);
    assert_eq!(verbose["type"], TYPE_BOOLEAN);
    assert_eq!(verbose["default"], Value::Null);

    let provider = options
        .iter()
        .find(|option| option["opts"][0] == "--provider")
        .expect("no --provider option");
    assert_eq!(provider["is_flag"], false);
    assert_eq!(provider["type"], TYPE_CHOICE);
    assert!(!provider["help"].as_str().unwrap_or_default().is_empty());

    let keep_progress = tree["options"]
        .as_array()
        .expect("no root options")
        .iter()
        .find(|option| option["opts"][0] == "--keep-progress")
        .expect("no --keep-progress option");
    assert_eq!(keep_progress["is_flag"], false);
    assert_eq!(keep_progress["default"], "true");
}

#[test]
fn test_full_help_declares_the_full_option() {
    let tree = tree();
    let help = child(&tree, HELP_COMMAND);
    let options = help["options"].as_array().expect("no 'help' options");
    for flag in ["--full", "--format"] {
        assert!(
            options.iter().any(|option| option["opts"][0] == flag),
            "'{flag}' is not declared on the help command"
        );
    }
}

#[test]
fn test_shell_help_accepts_cli_and_full() {
    let mut grammar = shell_grammar();
    grammar.build();
    let help = grammar
        .find_subcommand(HELP_COMMAND)
        .expect("no shell help");
    for name in [CLI_ARGUMENT, FULL_ARGUMENT, FORMAT_ARGUMENT] {
        assert!(
            help.get_arguments()
                .any(|argument| argument.get_id() == name),
            "shell help does not declare '{name}'"
        );
    }
}

#[test]
fn test_shell_help_requires_cli_for_full() {
    let mut grammar = shell_grammar();
    grammar.build();
    assert!(
        grammar
            .clone()
            .try_get_matches_from(["help", "--full"])
            .is_err()
    );
    assert!(
        grammar
            .clone()
            .try_get_matches_from(["help", "--cli", "--full"])
            .is_ok()
    );
    assert!(
        grammar
            .clone()
            .try_get_matches_from(["help", "--cli"])
            .is_ok()
    );
    assert!(grammar.try_get_matches_from(["help"]).is_ok());
}

#[test]
fn test_shell_help_lists_the_special_commands() {
    for (name, description) in SPECIAL_COMMANDS {
        assert!(!name.is_empty());
        assert!(!description.is_empty());
    }
    for escape in ["!<command>", "!cd <path>", "!set VAR=val", "!unset VAR"] {
        assert!(
            SPECIAL_COMMANDS.iter().any(|(name, _)| *name == escape),
            "the shell help does not list '{escape}'"
        );
    }
}
