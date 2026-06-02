//
// Copyright 2026 Tabs Data Inc.
//

use std::borrow::Cow;
use std::path::PathBuf;
use std::process::Command as SystemCommand;

use clap::{CommandFactory, Parser};
use clap_derive::Subcommand;
use colored::Colorize;
use reedline::{
    FileBackedHistory, Prompt, PromptEditMode, PromptHistorySearch, PromptHistorySearchStatus,
    Reedline, Signal,
};
use td_common::env::TABSDATA_HOME_DIR;
use tracing::info;

use crate::client::tdkserver::arguments::GlobalArguments;

use super::autoconfirm::{AutoconfirmCommands, command_autoconfirm};
use super::config::{ConfigCommands, command_config};
use super::secret::{SecretCommands, command_secret};
use super::server::{ServerCommands, command_server};
use super::session::{SessionCommands, command_session};
use crate::cerror;
use crate::client::tdkpin::{TdkPin, tdkpin_path};
use crate::core::system::exit::Result;

const TOOL_NAME: &str = "tdkserver";
const HISTORY_FILE: &str = ".tdkserver.history";
const HISTORY_CAPACITY: usize = 10_000;

#[derive(Debug, Clone, Subcommand)]
enum ShellCommand {
    /// Exit the shell
    Exit,
    /// Exit the shell
    Quit,
    /// Manage the autoconfirm preference
    #[command(subcommand)]
    Autoconfirm(AutoconfirmCommands),
    /// Manage tabsdata configs
    #[command(subcommand)]
    Config(ConfigCommands),
    /// Manage tabsdata secrets
    #[command(subcommand)]
    Secret(SecretCommands),
    /// Manage tabsdata servers
    #[command(subcommand)]
    Server(ServerCommands),
    /// Manage the active tabsdata session
    #[command(subcommand)]
    Session(SessionCommands),
}

#[derive(Debug, Parser)]
#[command(name = "", multicall = true)]
struct ShellInput {
    #[command(subcommand)]
    command: ShellCommand,
}

pub async fn command_shell(globals: &GlobalArguments) -> Result {
    show_quick_reference();
    show_initial_active_context();

    let history_path = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(TABSDATA_HOME_DIR)
        .join(HISTORY_FILE);

    let mut line_editor = Reedline::create();
    if let Ok(history) = FileBackedHistory::with_file(HISTORY_CAPACITY, history_path) {
        line_editor = line_editor.with_history(Box::new(history));
    }

    let prompt = ShellPrompt::new(TOOL_NAME);

    loop {
        let signal = match tokio::task::block_in_place(|| line_editor.read_line(&prompt)) {
            Ok(signal) => signal,
            Err(error) => {
                cerror!("Shell error: {error}");
                break;
            }
        };

        let line = match signal {
            Signal::Success(line) => line,
            Signal::CtrlC => continue,
            Signal::CtrlD => break,
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(escape) = line.strip_prefix('!') {
            run_shell_escape(escape.trim());
            continue;
        }

        match line {
            "?" | ":?" => {
                show_quick_reference();
                continue;
            }
            ":help" | ":h" => {
                let _ = ShellInput::command().print_help();
                println!();
                continue;
            }
            ":exit" | ":quit" | ":e" | ":q" => break,
            _ => {}
        }

        let tokens = match shlex::split(line) {
            Some(tokens) if !tokens.is_empty() => tokens,
            _ => {
                cerror!("Invalid command line.");
                continue;
            }
        };

        match ShellInput::try_parse_from(tokens) {
            Ok(input) => match input.command {
                ShellCommand::Exit | ShellCommand::Quit => break,
                ShellCommand::Autoconfirm(command) => {
                    let _ = command_autoconfirm(command, globals).await;
                }
                ShellCommand::Config(command) => {
                    let _ = command_config(command, globals).await;
                }
                ShellCommand::Secret(command) => {
                    let _ = command_secret(command, globals).await;
                }
                ShellCommand::Server(command) => {
                    let _ = command_server(command, globals).await;
                }
                ShellCommand::Session(command) => {
                    let _ = command_session(command, globals).await;
                }
            },
            Err(error) => {
                let _ = error.print();
            }
        }
    }
    Ok(())
}

fn show_initial_active_context() {
    if let Some(session) = TdkPin::read().session {
        info!(
            "Active tabsdata session: {} (from {}).",
            session.display_name(),
            tdkpin_path().display()
        );
    } else {
        info!("No active tabsdata session is set.");
    }
}

fn run_shell_escape(command: &str) {
    if command.is_empty() {
        return;
    }
    let mut tokens = match shlex::split(command) {
        Some(tokens) if !tokens.is_empty() => tokens,
        _ => {
            cerror!("Invalid shell command.");
            return;
        }
    };
    let head = tokens.remove(0);
    match head.as_str() {
        "cd" => {
            let target = tokens
                .into_iter()
                .next()
                .or_else(|| dirs::home_dir().map(|path| path.to_string_lossy().into_owned()))
                .unwrap_or_else(|| ".".to_owned());
            if let Err(error) = std::env::set_current_dir(&target) {
                cerror!("cd: {error}");
            }
        }
        "set" | "export" => {
            for entry in tokens {
                if let Some((key, value)) = entry.split_once('=') {
                    unsafe { std::env::set_var(key, value) };
                } else {
                    let value =
                        std::env::var(&entry).unwrap_or_else(|_| format!("{entry}: not set"));
                    println!("{value}");
                }
            }
        }
        "unset" => {
            for key in tokens {
                unsafe { std::env::remove_var(key) };
            }
        }
        other => {
            let mut process = SystemCommand::new(other);
            process.args(tokens);
            match process.status() {
                Ok(_) => {}
                Err(error) => cerror!("{other}: {error}"),
            }
        }
    }
}

fn show_quick_reference() {
    let title = format!("Tabsdata Kubernetes Shell v{}", env!("CARGO_PKG_VERSION"));
    let entries: &[(&str, &str)] = &[
        ("help", "Show available commands"),
        ("exit / quit", "Leave the shell (also Ctrl+D)"),
        ("!<command>", "Run a system shell command"),
        ("!cd <path>", "Change working directory"),
        ("!set VAR=val", "Set an environment variable"),
        ("!unset VAR", "Remove an environment variable"),
        (":?", "Show this quick reference"),
        (
            ":help / :exit / :quit",
            "Built-in commands (also :h / :e / :q)",
        ),
    ];

    let key_width = entries
        .iter()
        .map(|(key, _)| key.chars().count())
        .max()
        .unwrap_or(0);
    let max_entry_visible = entries
        .iter()
        .map(|(_, description)| key_width + 2 + description.chars().count())
        .max()
        .unwrap_or(0);
    let footer = "Use ESC or Ctrl+C to abort input prompts";
    let inner_width = max_entry_visible.max(footer.chars().count());
    let outer_width = inner_width + 4;

    let border = |segment: &str| segment.truecolor(0x82, 0xC3, 0x69).to_string();

    let title_len = title.chars().count();
    let dash_run = outer_width
        .saturating_sub(title_len)
        .saturating_sub(6)
        .max(1);

    println!();
    println!(
        "{}{}{}{}",
        border("╭─ "),
        title.bold().truecolor(0x82, 0xC3, 0x69),
        border(&format!(" {}", "─".repeat(dash_run))),
        border("─╮"),
    );

    let blank_line = format!(
        "{} {} {}",
        border("│"),
        " ".repeat(inner_width),
        border("│"),
    );
    println!("{blank_line}");

    for (key, description) in entries {
        let key_pad = " ".repeat(key_width - key.chars().count());
        let entry_visible = key_width + 2 + description.chars().count();
        let trail_pad = " ".repeat(inner_width - entry_visible);
        println!(
            "{} {}{}  {}{} {}",
            border("│"),
            key.bold().cyan(),
            key_pad,
            description,
            trail_pad,
            border("│"),
        );
    }

    println!("{blank_line}");

    let footer_visible = footer.chars().count();
    let footer_pad = " ".repeat(inner_width - footer_visible);
    println!(
        "{} {}{}{}{}{}{} {}",
        border("│"),
        "Use ".dimmed(),
        "ESC".bold().cyan(),
        " or ".dimmed(),
        "Ctrl+C".bold().cyan(),
        " to abort input prompts".dimmed(),
        footer_pad,
        border("│"),
    );

    println!(
        "{}{}{}",
        border("╰"),
        border(&"─".repeat(outer_width - 2)),
        border("╯"),
    );
    println!();
}

struct ShellPrompt {
    tool_name: String,
}

impl ShellPrompt {
    fn new(tool_name: &str) -> Self {
        Self {
            tool_name: tool_name.to_owned(),
        }
    }
}

impl Prompt for ShellPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        let tool = format!("{} ❯", self.tool_name).bold().green().to_string();
        let label = match TdkPin::read().session {
            Some(session) => format!("[{}]", session.display_name())
                .truecolor(0xCC, 0xB2, 0x33)
                .to_string(),
            None => "[no tabsdata session]".dimmed().to_string(),
        };
        Cow::Owned(format!("{label} {tool} "))
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed("::: ")
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        let prefix = match history_search.status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };
        Cow::Owned(format!(
            "({}reverse-search: {}) ",
            prefix, history_search.term
        ))
    }
}
