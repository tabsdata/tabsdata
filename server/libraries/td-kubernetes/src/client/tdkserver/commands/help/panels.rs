//
// Copyright 2026 Tabsdata Inc.
//

use std::io::{IsTerminal, Write, stderr};

use clap::error::ErrorKind;
use clap::{Arg, ArgAction, Command, Error};
use terminal_size::{Width, terminal_size};
use textwrap::core::display_width;
use textwrap::{Options, WordSeparator, WordSplitter, wrap};

const DEFAULT_WIDTH: usize = 100;
const MIN_HELP_WIDTH: usize = 20;
const GAP: &str = "  ";
const USAGE_PREFIX: &str = "Usage: ";
const HELP_FLAG_HELP: &str = "Show this message and exit.";
const VERSION_FLAG_HELP: &str = "Show the version and exit.";
const ERROR_PREFIX: &str = "error: ";
const TIP_PREFIX: &str = "tip: ";
const MORE_INFORMATION: &str = "For more information";

const RESET: &str = "\x1b[0m";
const USAGE: &str = "\x1b[1;38;2;57;170;241m";
const PATH: &str = "\x1b[1;38;2;214;214;214m";
const OPTION: &str = "\x1b[1;38;2;57;170;241m";
const SWITCH: &str = "\x1b[1;38;2;106;179;92m";
const METAVAR: &str = "\x1b[1;38;2;204;178;51m";
const METAVAR_MARK: &str = "\x1b[38;2;143;143;143m";
const REQUIRED: &str = "\x1b[38;2;239;130;130m";
const BORDER: &str = "\x1b[38;2;106;179;92m";
const PANEL_TITLE: &str = "\x1b[1;38;2;106;179;92m";
const TEXT: &str = "\x1b[38;2;214;214;214m";
const DEFAULT: &str = "\x1b[38;2;143;143;143m";
const MUTED: &str = "\x1b[38;2;143;143;143m";
const ERROR_BORDER: &str = "\x1b[38;2;239;130;130m";

struct Cell {
    text: String,
    style: String,
}

impl Cell {
    fn plain(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            style: String::new(),
        }
    }

    fn styled(text: impl Into<String>, style: &str) -> Self {
        let text = text.into();
        let style = if text.is_empty() {
            String::new()
        } else {
            paint(&text, style)
        };
        Self { text, style }
    }

    fn width(&self) -> usize {
        display_width(&self.text)
    }

    fn render(&self) -> &str {
        if self.style.is_empty() {
            &self.text
        } else {
            &self.style
        }
    }
}

struct Row {
    cells: Vec<Cell>,
    help: Vec<Cell>,
}

pub fn panelled(command: &mut Command) {
    panel_tree(command, help_width());
}

fn panel_tree(command: &mut Command, width: usize) {
    let help = render_help(command, width);
    *command = std::mem::take(command).override_help(help);
    for subcommand in command.get_subcommands_mut() {
        panel_tree(subcommand, width);
    }
}

fn help_width() -> usize {
    terminal_size()
        .map(|(Width(columns), _)| usize::from(columns))
        .unwrap_or(DEFAULT_WIDTH)
}

pub fn render_help(command: &Command, width: usize) -> String {
    // clap trims leading whitespace from the help; a no-op escape keeps the
    // blank line and the indent that open it.
    let mut lines = vec![RESET.to_owned(), usage_line(command), String::new()];
    if let Some(description) = description(command) {
        for line in wrapped(&description, width.saturating_sub(2)) {
            lines.push(format!(" {}", paint(&line, TEXT)));
        }
        lines.push(String::new());
    }
    let arguments = argument_rows(command);
    if !arguments.is_empty() {
        lines.extend(panel("Arguments", &arguments, width));
    }
    let options = option_rows(command);
    if !options.is_empty() {
        lines.extend(panel("Options", &options, width));
    }
    let commands = command_rows(command);
    if !commands.is_empty() {
        lines.extend(panel("Commands", &commands, width));
    }
    lines.join("\n")
}

fn usage_line(command: &Command) -> String {
    styled_usage(&command.clone().render_usage().to_string())
}

fn usage_words(usage: &str) -> impl Iterator<Item = &str> {
    usage
        .trim()
        .trim_start_matches(USAGE_PREFIX)
        .split_whitespace()
}

fn is_command_word(word: &str) -> bool {
    !word.starts_with(['[', '<', '-'])
        && !word.chars().any(|character| character.is_ascii_uppercase())
}

fn styled_usage(usage: &str) -> String {
    let words: Vec<String> = usage_words(usage)
        .map(|word| {
            if is_command_word(word) {
                paint(word, PATH)
            } else {
                placeholders(word)
            }
        })
        .collect();
    format!(
        " {} {}",
        paint(USAGE_PREFIX.trim_end(), USAGE),
        words.join(" ")
    )
}

fn usage_path(usage: &str) -> String {
    usage_words(usage)
        .filter(|word| is_command_word(word))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn is_usage_error(error: &Error) -> bool {
    !matches!(
        error.kind(),
        ErrorKind::DisplayHelp
            | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            | ErrorKind::DisplayVersion
    )
}

pub fn print_error(error: &Error) {
    if !is_usage_error(error) {
        let _ = error.print();
        return;
    }
    let rendered = render_error(error, help_width());
    let mut output = stderr();
    let text = if output.is_terminal() {
        rendered
    } else {
        strip_styles(&rendered)
    };
    let _ = writeln!(output, "{text}");
}

pub fn exit_with(error: Error) -> ! {
    print_error(&error);
    std::process::exit(error.exit_code())
}

pub fn render_error(error: &Error, width: usize) -> String {
    let rendered = error.render().to_string();
    let usage = rendered
        .lines()
        .find(|line| line.trim_start().starts_with(USAGE_PREFIX));
    let messages: Vec<String> = rendered
        .lines()
        .map(str::trim)
        .filter(|line| {
            !line.is_empty()
                && !line.starts_with(USAGE_PREFIX)
                && !line.starts_with(MORE_INFORMATION)
        })
        .map(|line| {
            let line = line.strip_prefix(ERROR_PREFIX).unwrap_or(line);
            let line = line.strip_prefix(TIP_PREFIX).unwrap_or(line);
            capitalized(line)
        })
        .collect();
    let mut lines = vec![String::new()];
    if let Some(usage) = usage {
        let path = usage_path(usage);
        lines.push(styled_usage(usage));
        lines.push(String::new());
        lines.push(format!(
            " {}{}{}",
            paint("Try ", MUTED),
            paint(&format!("'{path} -h'"), OPTION),
            paint(" for help", MUTED)
        ));
    }
    lines.push(error_border(width));
    let inner = width.saturating_sub(4);
    for message in &messages {
        for line in wrapped(message, inner) {
            let fill = inner.saturating_sub(display_width(&line));
            lines.push(format!(
                "{} {}{} {}",
                paint("│", ERROR_BORDER),
                quoted_options(&line),
                " ".repeat(fill),
                paint("│", ERROR_BORDER)
            ));
        }
    }
    lines.push(paint(
        &format!("╰{}╯", "─".repeat(width.saturating_sub(2))),
        ERROR_BORDER,
    ));
    lines.push(String::new());
    lines.join("\n")
}

fn capitalized(text: &str) -> String {
    let mut characters = text.chars();
    match characters.next() {
        Some(first) => first.to_uppercase().chain(characters).collect(),
        None => String::new(),
    }
}

fn quoted_options(text: &str) -> String {
    let mut styled = String::new();
    for (index, part) in text.split('\'').enumerate() {
        if index > 0 {
            styled.push('\'');
        }
        if index % 2 == 1 && part.starts_with('-') {
            styled.push_str(&paint(part, OPTION));
        } else {
            styled.push_str(part);
        }
    }
    styled
}

fn error_border(width: usize) -> String {
    let label = " Error ";
    let rest = width.saturating_sub(display_width(label) + 3);
    paint(&format!("╭─{label}{}╮", "─".repeat(rest)), ERROR_BORDER)
}

fn strip_styles(text: &str) -> String {
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

fn placeholders(text: &str) -> String {
    let mut styled = String::new();
    let mut word = String::new();
    for character in text.chars() {
        if character.is_ascii_uppercase() || (character == '_' && !word.is_empty()) {
            word.push(character);
            continue;
        }
        if !word.is_empty() {
            styled.push_str(&paint(&word, OPTION));
            word.clear();
        }
        styled.push(character);
    }
    if !word.is_empty() {
        styled.push_str(&paint(&word, OPTION));
    }
    styled
}

fn description(command: &Command) -> Option<String> {
    command
        .get_long_about()
        .or_else(|| command.get_about())
        .map(|about| about.to_string().trim().to_owned())
        .filter(|about| !about.is_empty())
}

fn argument_rows(command: &Command) -> Vec<Row> {
    command
        .get_positionals()
        .filter(|argument| !argument.is_hide_set())
        .map(|argument| Row {
            cells: vec![
                required_cell(argument),
                Cell::styled(positional_name(argument), OPTION),
            ],
            help: help_cells(argument),
        })
        .collect()
}

fn option_rows(command: &Command) -> Vec<Row> {
    let mut arguments: Vec<&Arg> = command
        .get_arguments()
        .filter(|argument| !argument.is_positional() && !argument.is_hide_set())
        .collect();
    arguments.sort_by_key(|argument| argument.get_display_order());
    arguments
        .into_iter()
        .map(|argument| Row {
            cells: vec![
                required_cell(argument),
                Cell::styled(long_flags(argument), OPTION),
                Cell::styled(short_flag(argument), SWITCH),
                metavar_cell(argument),
            ],
            help: help_cells(argument),
        })
        .collect()
}

fn command_rows(command: &Command) -> Vec<Row> {
    let mut subcommands: Vec<&Command> = command
        .get_subcommands()
        .filter(|subcommand| !subcommand.is_hide_set())
        .collect();
    subcommands.sort_by_key(|subcommand| subcommand.get_display_order());
    subcommands
        .into_iter()
        .map(|subcommand| Row {
            cells: vec![Cell::styled(subcommand.get_name(), OPTION)],
            help: vec![Cell::plain(
                subcommand
                    .get_about()
                    .map(|about| about.to_string())
                    .unwrap_or_default(),
            )],
        })
        .collect()
}

fn required_cell(argument: &Arg) -> Cell {
    if argument.is_required_set() {
        Cell::styled("*", REQUIRED)
    } else {
        Cell::plain("")
    }
}

fn positional_name(argument: &Arg) -> String {
    argument
        .get_value_names()
        .and_then(|names| names.first().map(ToString::to_string))
        .map(|name| metavar_name(&name))
        .unwrap_or_else(|| metavar_name(argument.get_id().as_str()))
}

fn long_flags(argument: &Arg) -> String {
    argument
        .get_long_and_visible_aliases()
        .unwrap_or_default()
        .into_iter()
        .map(|long| format!("--{long}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn short_flag(argument: &Arg) -> String {
    argument
        .get_short()
        .map(|short| format!("-{short}"))
        .unwrap_or_default()
}

fn takes_value(argument: &Arg) -> bool {
    matches!(argument.get_action(), ArgAction::Set | ArgAction::Append)
}

fn metavar_cell(argument: &Arg) -> Cell {
    if !takes_value(argument) {
        return Cell::plain("");
    }
    let choices: Vec<String> = argument
        .get_possible_values()
        .iter()
        .filter(|value| !value.is_hide_set())
        .map(|value| value.get_name().to_owned())
        .collect();
    if choices.is_empty() {
        let name = argument
            .get_value_names()
            .map(|names| {
                names
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_else(|| argument.get_id().to_string());
        return Cell::styled(metavar_name(&name), METAVAR);
    }
    let text = format!("[{}]", choices.join("|"));
    let mut style = paint("[", METAVAR_MARK);
    for (index, choice) in choices.iter().enumerate() {
        if index > 0 {
            style.push_str(&paint("|", METAVAR_MARK));
        }
        style.push_str(&paint(choice, METAVAR));
    }
    style.push_str(&paint("]", METAVAR_MARK));
    Cell { text, style }
}

fn metavar_name(name: &str) -> String {
    name.to_uppercase().replace('-', "_")
}

fn is_help_flag(argument: &Arg) -> bool {
    matches!(
        argument.get_action(),
        ArgAction::Help | ArgAction::HelpShort | ArgAction::HelpLong
    )
}

fn help_cells(argument: &Arg) -> Vec<Cell> {
    if is_help_flag(argument) {
        return vec![Cell::plain(HELP_FLAG_HELP)];
    }
    if matches!(argument.get_action(), ArgAction::Version) {
        return vec![Cell::plain(VERSION_FLAG_HELP)];
    }
    let help = argument
        .get_long_help()
        .or_else(|| argument.get_help())
        .map(|help| help.to_string().trim().to_owned())
        .unwrap_or_default();
    let mut cells = vec![Cell::plain(help)];
    if let Some(default) = default_value(argument) {
        cells.push(Cell::styled(format!("[default: {default}]"), DEFAULT));
    }
    cells
}

fn default_value(argument: &Arg) -> Option<String> {
    if !takes_value(argument) || argument.is_hide_default_value_set() {
        return None;
    }
    let defaults: Vec<String> = argument
        .get_default_values()
        .iter()
        .map(|default| default.to_string_lossy().into_owned())
        .collect();
    (!defaults.is_empty()).then(|| defaults.join(", "))
}

fn panel(title: &str, rows: &[Row], width: usize) -> Vec<String> {
    let inner = width.saturating_sub(4);
    let columns = rows.first().map(|row| row.cells.len()).unwrap_or(0);
    let widths: Vec<usize> = (0..columns)
        .map(|index| {
            rows.iter()
                .map(|row| row.cells[index].width())
                .max()
                .unwrap_or(0)
        })
        .collect();
    let used: usize = widths
        .iter()
        .filter(|width| **width > 0)
        .map(|width| width + GAP.len())
        .sum();
    let help_width = inner.saturating_sub(used).max(MIN_HELP_WIDTH);
    let mut lines = vec![top_border(title, width)];
    for row in rows {
        let lead: String = row
            .cells
            .iter()
            .zip(&widths)
            .filter(|(_, width)| **width > 0)
            .map(|(cell, width)| {
                format!("{}{}{GAP}", cell.render(), " ".repeat(width - cell.width()))
            })
            .collect();
        let lead_width = used;
        for (index, line) in help_lines(&row.help, help_width).into_iter().enumerate() {
            let prefix = if index == 0 {
                lead.clone()
            } else {
                " ".repeat(lead_width)
            };
            let fill = inner.saturating_sub(lead_width + line.0);
            lines.push(format!(
                "{} {prefix}{}{} {}",
                paint("│", BORDER),
                paint(&line.1, TEXT),
                " ".repeat(fill),
                paint("│", BORDER)
            ));
        }
    }
    lines.push(paint(
        &format!("╰{}╯", "─".repeat(width.saturating_sub(2))),
        BORDER,
    ));
    lines
}

fn top_border(title: &str, width: usize) -> String {
    let label = format!(" {title} ");
    let rest = width.saturating_sub(display_width(&label) + 3);
    format!(
        "{}{}{}",
        paint("╭─", BORDER),
        paint(&label, PANEL_TITLE),
        paint(&format!("{}╮", "─".repeat(rest)), BORDER)
    )
}

fn help_lines(cells: &[Cell], width: usize) -> Vec<(usize, String)> {
    let mut lines: Vec<(usize, String)> = Vec::new();
    let text = cells
        .iter()
        .filter(|cell| !cell.text.is_empty())
        .map(|cell| cell.text.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    for line in wrapped(&text, width) {
        lines.push((display_width(&line), line));
    }
    if lines.is_empty() {
        lines.push((0, String::new()));
    }
    for cell in cells.iter().filter(|cell| !cell.style.is_empty()) {
        for line in lines.iter_mut() {
            if line.1.contains(&cell.text) {
                line.1 = line.1.replacen(&cell.text, &cell.style, 1);
            }
        }
    }
    lines
}

fn wrapped(text: &str, width: usize) -> Vec<String> {
    let options = Options::new(width.max(1))
        .word_separator(WordSeparator::AsciiSpace)
        .word_splitter(WordSplitter::NoHyphenation);
    text.lines()
        .flat_map(|line| {
            if line.trim().is_empty() {
                vec![String::new()]
            } else {
                wrap(line, &options)
                    .into_iter()
                    .map(|part| part.into_owned())
                    .collect()
            }
        })
        .collect()
}

fn paint(text: &str, style: &str) -> String {
    format!("{style}{text}{RESET}")
}

#[cfg(test)]
mod tests;
