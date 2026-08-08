//
// Copyright 2026 Tabsdata Inc.
//

use clap::{Arg, ArgAction, Command, CommandFactory, ValueEnum};
use clap_derive::Args;
use serde::Serialize;
use serde_json::Value;

use crate::client::tdkserver::arguments::{Arguments, GlobalArguments};
use crate::client::tdkserver::commands::shell::{SPECIAL_COMMANDS, shell_grammar};
use crate::core::system::exit::CommandResult;

pub const PROGRAM_NAME: &str = "tdkserver";

pub const FULL_ARGUMENT: &str = "full";

pub const FULL_LONG_HELP: &str =
    "Print the help of every command and subcommand, instead of the summary help.";

pub const FORMAT_ARGUMENT: &str = "format";

pub const FORMAT_LONG_HELP: &str =
    "Format of the full help: text or json. Valid only together with --full.";

pub const CLI_ARGUMENT: &str = "cli";

pub const CLI_LONG_HELP: &str =
    "Show the help of the tdkserver command line, instead of the help of this shell.";

const TYPE_BOOLEAN: &str = "boolean";
const TYPE_CHOICE: &str = "choice";
const TYPE_TEXT: &str = "text";

const SHELL_COMMAND: &str = "shell";

const HELP_COMMAND: &str = "help";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum HelpFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Args)]
pub struct HelpArguments {
    #[arg(
        long,
        name = FULL_ARGUMENT,
        required = false,
        default_value_t = false,
        display_order = 10,
        long_help = FULL_LONG_HELP
    )]
    pub full: bool,

    #[arg(
        long,
        name = FORMAT_ARGUMENT,
        required = false,
        value_enum,
        default_value_t = HelpFormat::Text,
        display_order = 20,
        long_help = FORMAT_LONG_HELP
    )]
    pub format: HelpFormat,
}

impl HelpArguments {
    pub fn summary() -> Self {
        Self {
            full: false,
            format: HelpFormat::Text,
        }
    }
}

pub async fn command_help(arguments: HelpArguments, _globals: &GlobalArguments) -> CommandResult {
    if arguments.full {
        println!("{}", build_full_help(arguments.format));
    } else {
        Arguments::command().print_help().ok();
        println!();
    }
    Ok(())
}

#[derive(Debug, Clone, Args)]
pub struct ShellHelpArguments {
    #[arg(
        long,
        name = CLI_ARGUMENT,
        required = false,
        default_value_t = false,
        display_order = 10,
        long_help = CLI_LONG_HELP
    )]
    pub cli: bool,

    #[arg(
        long,
        name = FULL_ARGUMENT,
        required = false,
        default_value_t = false,
        requires = CLI_ARGUMENT,
        display_order = 20,
        long_help = FULL_LONG_HELP
    )]
    pub full: bool,

    #[arg(
        long,
        name = FORMAT_ARGUMENT,
        required = false,
        value_enum,
        default_value_t = HelpFormat::Text,
        requires = FULL_ARGUMENT,
        display_order = 30,
        long_help = FORMAT_LONG_HELP
    )]
    pub format: HelpFormat,
}

pub async fn command_shell_help(
    arguments: ShellHelpArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    if !arguments.cli {
        shell_grammar().print_help().ok();
        println!();
        show_special_commands();
        return Ok(());
    }
    command_help(
        HelpArguments {
            full: arguments.full,
            format: arguments.format,
        },
        globals,
    )
    .await
}

fn show_special_commands() {
    let width = SPECIAL_COMMANDS
        .iter()
        .map(|(name, _)| name.chars().count())
        .max()
        .unwrap_or(0);
    println!("Special commands:");
    for (name, description) in SPECIAL_COMMANDS {
        println!("  {name:<width$}  {description}");
    }
    println!();
}

#[derive(Debug, Serialize)]
struct NodeArgument {
    name: String,
    required: bool,
}

#[derive(Debug, Serialize)]
struct NodeOption {
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
    options: Vec<NodeOption>,
    arguments: Vec<NodeArgument>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commands: Option<Vec<HelpNode>>,
}

fn build_full_help(format: HelpFormat) -> String {
    let mut root = Arguments::command();
    root.build();
    let tree = build_node(PROGRAM_NAME, &root, PROGRAM_NAME);
    if format == HelpFormat::Json {
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
        help: node_help(command),
        options: build_options(command),
        arguments: build_arguments(command),
        commands,
    }
}

fn listed_subcommands(command: &Command) -> impl Iterator<Item = &Command> {
    command
        .get_subcommands()
        .filter(|subcommand| subcommand.get_name() != HELP_COMMAND || declares_full(subcommand))
}

fn declares_full(command: &Command) -> bool {
    command
        .get_arguments()
        .any(|argument| argument.get_id() == FULL_ARGUMENT)
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

fn build_options(command: &Command) -> Vec<NodeOption> {
    command
        .get_arguments()
        .filter(|argument| !argument.is_positional())
        .map(|argument| {
            let is_flag = is_flag(argument);
            NodeOption {
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

fn build_arguments(command: &Command) -> Vec<NodeArgument> {
    command
        .get_positionals()
        .map(|argument| NodeArgument {
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

fn node_help(command: &Command) -> String {
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
mod tests;
