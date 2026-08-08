//
// Copyright 2026 Tabsdata Inc.
//

use std::process::exit;

use clap::{Arg, ArgAction, Command, CommandFactory};
use serde::Serialize;
use serde_json::Value;

use crate::client::tdkserver::arguments::Arguments;
use crate::client::tdkserver::commands::shell::shell_grammar;

pub const PROGRAM_NAME: &str = "tdkserver";

pub const FULL_ARGUMENT: &str = "full";

pub const FULL_LONG_HELP: &str = "With --help, print the full help for every command and subcommand, then exit. \
     Optionally add '--format text|json' (default: text): \
     'tdkserver --help [--full [--format FORMAT]]'.";

const FULL_FLAG: &str = "--full";
const HELP_FLAGS: [&str; 2] = ["--help", "-h"];
const FORMAT_FLAGS: [&str; 2] = ["--format", "-o"];

const FORMAT_TEXT: &str = "text";
const FORMAT_JSON: &str = "json";

const TYPE_BOOLEAN: &str = "boolean";
const TYPE_CHOICE: &str = "choice";
const TYPE_TEXT: &str = "text";

const SHELL_COMMAND: &str = "shell";

/// The subcommand clap generates to print the help of any other subcommand. It
/// mirrors the whole tree, so listing it would duplicate every node.
const HELP_COMMAND: &str = "help";

const INVALID_FORMAT_STATUS: i32 = 2;

#[derive(Debug, Serialize)]
struct HelpArgument {
    name: String,
    required: bool,
}

#[derive(Debug, Serialize)]
struct HelpOption {
    opts: Vec<String>,
    help: String,
    is_flag: bool,
    required: bool,
    default: Option<Value>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Debug, Serialize)]
struct HelpNode {
    name: String,
    path: String,
    help: String,
    options: Vec<HelpOption>,
    arguments: Vec<HelpArgument>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commands: Option<Vec<HelpNode>>,
}

/// Print the full help and exit when the incoming arguments ask for it.
///
/// This runs before the license banner and the polars compatibility check, so
/// that '--format json' writes a parseable document and nothing else.
pub fn handle_full_help() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if !full_help_requested(&argv) {
        return;
    }
    match output_format(&argv) {
        Ok(format) => {
            println!("{}", build_full_help(format));
            exit(0);
        }
        Err(error) => {
            eprintln!("{error}");
            exit(INVALID_FORMAT_STATUS);
        }
    }
}

fn full_help_requested(argv: &[String]) -> bool {
    argv.iter()
        .any(|argument| HELP_FLAGS.contains(&argument.as_str()))
        && argv.iter().any(|argument| argument == FULL_FLAG)
}

fn output_format(argv: &[String]) -> Result<&'static str, String> {
    let mut format = FORMAT_TEXT;
    for (index, argument) in argv.iter().enumerate() {
        if !FORMAT_FLAGS.contains(&argument.as_str()) {
            continue;
        }
        match argv.get(index + 1).map(String::as_str) {
            Some(FORMAT_TEXT) => format = FORMAT_TEXT,
            Some(FORMAT_JSON) => format = FORMAT_JSON,
            Some(invalid) => {
                return Err(format!(
                    "Invalid --format '{invalid}'; expected 'text' or 'json'."
                ));
            }
            None => {}
        }
    }
    Ok(format)
}

fn build_full_help(format: &str) -> String {
    let mut root = Arguments::command();
    root.build();
    let tree = build_node(PROGRAM_NAME, &root, PROGRAM_NAME);
    if format == FORMAT_JSON {
        serde_json::to_string_pretty(&tree).unwrap_or_default()
    } else {
        let mut lines = Vec::new();
        render_text(&tree, &mut lines);
        lines.join("\n")
    }
}

fn build_node(name: &str, command: &Command, path: &str) -> HelpNode {
    let commands = if name == SHELL_COMMAND {
        let mut grammar = shell_grammar();
        grammar.build();
        Some(build_children(&grammar, path))
    } else if listed_subcommands(command).next().is_some() {
        Some(build_children(command, path))
    } else {
        None
    };
    HelpNode {
        name: name.to_owned(),
        path: path.to_owned(),
        help: command_help(command),
        options: build_options(command),
        arguments: build_arguments(command),
        commands,
    }
}

fn listed_subcommands(command: &Command) -> impl Iterator<Item = &Command> {
    command
        .get_subcommands()
        .filter(|subcommand| subcommand.get_name() != HELP_COMMAND)
}

fn build_children(command: &Command, path: &str) -> Vec<HelpNode> {
    let mut subcommands: Vec<&Command> = listed_subcommands(command).collect();
    subcommands.sort_by(|one, other| one.get_name().cmp(other.get_name()));
    subcommands
        .into_iter()
        .map(|subcommand| {
            let name = subcommand.get_name();
            build_node(name, subcommand, &format!("{path} {name}"))
        })
        .collect()
}

fn build_options(command: &Command) -> Vec<HelpOption> {
    command
        .get_arguments()
        .filter(|argument| !argument.is_positional())
        .map(|argument| {
            let is_flag = is_flag(argument);
            HelpOption {
                opts: option_flags(argument),
                help: argument_help(argument),
                is_flag,
                required: argument.is_required_set(),
                default: option_default(argument, is_flag),
                kind: Some(option_type(argument, is_flag).to_owned()),
            }
        })
        .collect()
}

fn build_arguments(command: &Command) -> Vec<HelpArgument> {
    command
        .get_positionals()
        .map(|argument| HelpArgument {
            name: argument.get_id().as_str().to_owned(),
            required: argument.is_required_set(),
        })
        .collect()
}

fn option_flags(argument: &Arg) -> Vec<String> {
    let mut flags = Vec::new();
    for short in argument.get_short_and_visible_aliases().unwrap_or_default() {
        flags.push(format!("-{short}"));
    }
    for long in argument.get_long_and_visible_aliases().unwrap_or_default() {
        flags.push(format!("--{long}"));
    }
    flags
}

fn option_default(argument: &Arg, is_flag: bool) -> Option<Value> {
    if is_flag {
        return None;
    }
    let mut defaults: Vec<Value> = argument
        .get_default_values()
        .iter()
        .map(|default| Value::String(default.to_string_lossy().into_owned()))
        .collect();
    match defaults.len() {
        0 => None,
        1 => defaults.pop(),
        _ => Some(Value::Array(defaults)),
    }
}

fn option_type(argument: &Arg, is_flag: bool) -> &'static str {
    if is_flag {
        TYPE_BOOLEAN
    } else if argument.get_possible_values().is_empty() {
        TYPE_TEXT
    } else {
        TYPE_CHOICE
    }
}

fn is_flag(argument: &Arg) -> bool {
    matches!(
        argument.get_action(),
        ArgAction::SetTrue
            | ArgAction::SetFalse
            | ArgAction::Count
            | ArgAction::Help
            | ArgAction::HelpShort
            | ArgAction::HelpLong
            | ArgAction::Version
    )
}

fn command_help(command: &Command) -> String {
    command
        .get_long_about()
        .or_else(|| command.get_about())
        .map(ToString::to_string)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn argument_help(argument: &Arg) -> String {
    argument
        .get_long_help()
        .or_else(|| argument.get_help())
        .map(ToString::to_string)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn requirement_tag(required: bool) -> &'static str {
    if required { "required" } else { "optional" }
}

fn render_text(node: &HelpNode, lines: &mut Vec<String>) {
    lines.push(node.path.clone());
    if !node.help.is_empty() {
        for line in node.help.lines() {
            lines.push(format!("    {line}").trim_end().to_owned());
        }
    }
    if !node.arguments.is_empty() {
        lines.push("    Arguments:".to_owned());
        for argument in &node.arguments {
            lines.push(format!(
                "      {} ({})",
                argument.name,
                requirement_tag(argument.required)
            ));
        }
    }
    if !node.options.is_empty() {
        lines.push("    Options:".to_owned());
        for option in &node.options {
            let flags = option.opts.join(", ");
            let tag = requirement_tag(option.required);
            let suffix = if option.help.is_empty() {
                String::new()
            } else {
                format!("  {}", option.help)
            };
            lines.push(
                format!("      {flags} ({tag}){suffix}")
                    .trim_end()
                    .to_owned(),
            );
        }
    }
    lines.push(String::new());
    for subcommand in node.commands.iter().flatten() {
        render_text(subcommand, lines);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(arguments: &[&str]) -> Vec<String> {
        arguments
            .iter()
            .map(|argument| (*argument).to_owned())
            .collect()
    }

    fn tree() -> Value {
        serde_json::from_str(&build_full_help(FORMAT_JSON)).expect("full help is not valid json")
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
    fn test_full_help_requires_the_help_flag() {
        assert!(full_help_requested(&argv(&["--help", "--full"])));
        assert!(full_help_requested(&argv(&["-h", "--full"])));
        assert!(!full_help_requested(&argv(&["--full"])));
        assert!(!full_help_requested(&argv(&["--help"])));
    }

    #[test]
    fn test_output_format_defaults_to_text() {
        assert_eq!(output_format(&argv(&["--help", "--full"])), Ok(FORMAT_TEXT));
        assert_eq!(
            output_format(&argv(&["--help", "--full", "--format", "json"])),
            Ok(FORMAT_JSON)
        );
        assert_eq!(
            output_format(&argv(&["--help", "--full", "-o", "json"])),
            Ok(FORMAT_JSON)
        );
    }

    #[test]
    fn test_output_format_rejects_unknown_formats() {
        assert!(output_format(&argv(&["--help", "--full", "--format", "yaml"])).is_err());
    }

    #[test]
    fn test_full_help_text_lists_nested_commands() {
        let text = build_full_help(FORMAT_TEXT);
        assert!(text.contains("tdkserver quickstart"));
        assert!(text.contains("tdkserver info third-party"));
        assert!(text.contains("tdkserver get-cfg"));
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
        for command in ["config", "secret", "server", "session", "exit"] {
            assert!(
                shell_commands.contains(&command.to_owned()),
                "missing shell command '{command}'"
            );
        }
        let config = child(shell, "config");
        assert_eq!(config["path"], "tdkserver shell config");
        assert!(!names(config).is_empty());
    }

    #[test]
    fn test_full_help_omits_the_generated_help_subcommand() {
        let tree = tree();
        assert!(!names(&tree).contains(&HELP_COMMAND.to_owned()));
        assert!(!names(child(&tree, "info")).contains(&HELP_COMMAND.to_owned()));
        assert!(!names(child(&tree, SHELL_COMMAND)).contains(&HELP_COMMAND.to_owned()));
        assert!(!build_full_help(FORMAT_TEXT).contains("tdkserver help "));
    }

    #[test]
    fn test_full_help_marks_options_required_or_optional() {
        let text = build_full_help(FORMAT_TEXT);
        assert!(text.contains("(optional)"));
        assert!(text.contains("(required)"));

        let mut collected = Vec::new();
        options(&tree(), &mut collected);
        let required: Vec<bool> = collected
            .iter()
            .filter_map(|option| option["required"].as_bool())
            .collect();
        assert!(required.contains(&true));
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
        let full = tree["options"]
            .as_array()
            .expect("no root options")
            .iter()
            .find(|option| option["opts"][0] == FULL_FLAG);
        assert!(full.is_some(), "--full is not declared on the root command");
    }
}
