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
use tracing::error;

use crate::client::tdkserver::create::{CreateArguments, command_create};

const TOOL_NAME: &str = "tdkserver";
const HISTORY_FILE: &str = ".tdkserver.history";
const HISTORY_CAPACITY: usize = 10_000;

#[derive(Debug, Clone, Subcommand)]
enum ShellCommand {
    /// Create a Tabsdata instance
    Create(CreateArguments),
    /// Exit the shell
    Exit,
    /// Exit the shell
    Quit,
}

#[derive(Debug, Parser)]
#[command(name = "", multicall = true)]
struct ShellInput {
    #[command(subcommand)]
    command: ShellCommand,
}

pub async fn command_shell() {
    show_quick_reference();

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
                error!("Shell error: {error}");
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
                error!("Invalid command line.");
                continue;
            }
        };

        match ShellInput::try_parse_from(tokens) {
            Ok(input) => match input.command {
                ShellCommand::Create(arguments) => command_create(arguments).await,
                ShellCommand::Exit | ShellCommand::Quit => break,
            },
            Err(error) => {
                let _ = error.print();
            }
        }
    }
}

fn run_shell_escape(command: &str) {
    if command.is_empty() {
        return;
    }
    let mut tokens = match shlex::split(command) {
        Some(tokens) if !tokens.is_empty() => tokens,
        _ => {
            error!("Invalid shell command.");
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
                error!("cd: {error}");
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
                Err(error) => error!("{other}: {error}"),
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
        ("?", "Show this quick reference"),
        (
            ":help / :exit / :quit",
            "Built-in commands (also :h / :e / :q)",
        ),
    ];

    let key_width = entries
        .iter()
        .map(|(key, _)| key.chars().count())
        .max()
        .unwrap_or(20);

    println!();
    println!("{}", title.bold().green());
    println!();
    for (key, description) in entries {
        let padding = key_width - key.chars().count();
        println!(
            "  {}{}  {}",
            key.bold().cyan(),
            " ".repeat(padding),
            description
        );
    }
    println!();
    println!(
        "  Use {} or {} to abort input prompts",
        "ESC".bold().cyan(),
        "Ctrl+C".bold().cyan()
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
        Cow::Owned(format!("{} ❯ ", self.tool_name))
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
