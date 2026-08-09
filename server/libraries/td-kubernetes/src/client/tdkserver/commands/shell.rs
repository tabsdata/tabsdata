//
// Copyright 2026 Tabsdata Inc.
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

use super::about::command_about;
use super::banner::command_banner;
use super::help::{ShellHelpArguments, command_shell_help};
use super::service::InstanceNameArguments;
use super::service::check::{CheckArguments, command_check};
use super::service::create::{CreateArguments, command_create};
use super::service::delete::{DeleteArguments, command_delete};
use super::service::get::{GetArguments, command_get};
use super::service::info::{InfoCommands, command_info};
use super::service::init::{InitArguments, command_init};
use super::service::inspect::command_inspect;
use super::service::list::command_list;
use super::service::put::{PutArguments, command_put};
use super::service::quickstart::{QuickstartArguments, command_quickstart};
use super::service::restart::{RestartArguments, command_restart};
use super::service::start::{StartArguments, command_start};
use super::service::status::command_status;
use super::service::stop::{StopArguments, command_stop};
use super::service::trash::{TrashArguments, command_trash};
use super::service::view::{ViewArguments, command_view};
use super::session::set::{SetArguments, command_set};
use super::session::settings::{SettingsArguments, command_settings};
use super::session::unset::{UnpinArguments, command_unset};
use crate::cerror;
use crate::client::session::settings::{SettingsContext, tdksettings_path};
use crate::core::system::exit::CommandResult;

const TOOL_NAME: &str = "tdkserver";
const HISTORY_FILE: &str = ".tdkserver.history";
const HISTORY_CAPACITY: usize = 10_000;

#[derive(Debug, Clone, Subcommand)]
enum ShellCommand {
    #[command(about = "Show the commands of this shell, or of the tdkserver command line")]
    Help(ShellHelpArguments),
    #[command(about = "Show Tabsdata banner")]
    Banner,
    #[command(about = "Show build metadata and system information")]
    About,
    #[command(
        subcommand,
        about = "Show the license, third-party dependencies and release notes of tabsdata",
        arg_required_else_help = true
    )]
    Info(InfoCommands),
    #[command(about = "Initialize, create and start a tabsdata instance in one step")]
    Quickstart(QuickstartArguments),
    #[command(about = "Produce an editable input config folder for a tabsdata instance")]
    Init(InitArguments),
    #[command(about = "Create a tabsdata instance")]
    Create(CreateArguments),
    #[command(about = "Delete the resources of a tabsdata instance")]
    Delete(DeleteArguments),
    #[command(about = "Start a tabsdata instance")]
    Start(StartArguments),
    #[command(about = "Stop a tabsdata instance")]
    Stop(StopArguments),
    #[command(about = "Restart a tabsdata instance")]
    Restart(RestartArguments),
    #[command(about = "Show the running workloads of a tabsdata instance")]
    Status(InstanceNameArguments),
    #[command(about = "Show the configuration and state of a tabsdata instance")]
    Inspect(InstanceNameArguments),
    #[command(about = "Trash a local kminus tabsdata instance")]
    Trash(TrashArguments),
    #[command(
        name = "check-cfg",
        about = "Check the values still to be provided in the configs of a tabsdata instance"
    )]
    Check(CheckArguments),
    #[command(
        name = "get-cfg",
        about = "Download the configmaps of a tabsdata instance for editing"
    )]
    Get(GetArguments),
    #[command(
        name = "put-cfg",
        about = "Replace the configmaps of a tabsdata instance from edited files"
    )]
    Put(PutArguments),
    #[command(
        name = "view-cfg",
        about = "View the configmaps of a tabsdata instance"
    )]
    View(ViewArguments),
    #[command(about = "Set a session setting used when commands omit it")]
    Set(SetArguments),
    #[command(about = "Unset a session setting")]
    Unset(UnpinArguments),
    #[command(about = "Show the tabsdata session settings")]
    Settings(SettingsArguments),
    #[command(about = "List registered tabsdata instances")]
    List,
    #[command(about = "Exit the shell")]
    Exit,
    #[command(about = "Exit the shell")]
    Quit,
}

#[derive(Debug, Parser)]
#[command(name = "", multicall = true, disable_help_subcommand = true)]
struct ShellInput {
    #[command(subcommand)]
    command: ShellCommand,
}

pub fn shell_grammar() -> clap::Command {
    ShellInput::command()
}

pub async fn command_shell(globals: &GlobalArguments) -> CommandResult {
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
                ShellCommand::Help(arguments) => {
                    let _ = command_shell_help(arguments, globals).await;
                }
                ShellCommand::Banner => {
                    let _ = command_banner(globals).await;
                }
                ShellCommand::About => {
                    let _ = command_about(globals).await;
                }
                ShellCommand::Info(command) => {
                    let _ = command_info(command, globals).await;
                }
                ShellCommand::Quickstart(arguments) => {
                    let _ = command_quickstart(arguments, globals).await;
                }
                ShellCommand::Init(arguments) => {
                    let _ = command_init(arguments, globals).await;
                }
                ShellCommand::Create(arguments) => {
                    let _ = command_create(arguments, globals).await;
                }
                ShellCommand::Delete(arguments) => {
                    let _ = command_delete(arguments, globals).await;
                }
                ShellCommand::Start(arguments) => {
                    let _ = command_start(arguments, globals).await;
                }
                ShellCommand::Stop(arguments) => {
                    let _ = command_stop(arguments, globals).await;
                }
                ShellCommand::Restart(arguments) => {
                    let _ = command_restart(arguments, globals).await;
                }
                ShellCommand::Status(arguments) => {
                    let _ = command_status(arguments, globals).await;
                }
                ShellCommand::Inspect(arguments) => {
                    let _ = command_inspect(arguments, globals).await;
                }
                ShellCommand::Trash(arguments) => {
                    let _ = command_trash(arguments, globals).await;
                }
                ShellCommand::Check(arguments) => {
                    let _ = command_check(arguments, globals).await;
                }
                ShellCommand::Get(arguments) => {
                    let _ = command_get(arguments, globals).await;
                }
                ShellCommand::Put(arguments) => {
                    let _ = command_put(arguments, globals).await;
                }
                ShellCommand::View(arguments) => {
                    let _ = command_view(arguments, globals).await;
                }
                ShellCommand::Set(arguments) => {
                    let _ = command_set(arguments, globals).await;
                }
                ShellCommand::Unset(arguments) => {
                    let _ = command_unset(arguments, globals).await;
                }
                ShellCommand::Settings(arguments) => {
                    let _ = command_settings(arguments, globals).await;
                }
                ShellCommand::List => {
                    let _ = command_list(globals).await;
                }
                ShellCommand::Exit | ShellCommand::Quit => break,
            },
            Err(error) => {
                let _ = error.print();
            }
        }
    }
    Ok(())
}

fn show_initial_active_context() {
    if let Some(name) = SettingsContext::settings_instance() {
        info!(
            "Instance of the tabsdata session settings: {name} (from {}).",
            tdksettings_path().display()
        );
    } else {
        info!("The tabsdata session settings hold no instance.");
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

/// Entered at the shell prompt but parsed before the command grammar, so they
/// never reach clap and would otherwise go unlisted by any help.
pub const SPECIAL_COMMANDS: [(&str, &str); 6] = [
    ("!<command>", "Run a system shell command"),
    ("!cd <path>", "Change working directory"),
    ("!set VAR=val", "Set an environment variable"),
    ("!unset VAR", "Remove an environment variable"),
    (":?", "Show the quick reference"),
    (
        ":help / :exit / :quit",
        "Built-in commands (also :h / :e / :q)",
    ),
];

fn show_quick_reference() {
    let title = format!("Tabsdata Kubernetes Shell v{}", env!("CARGO_PKG_VERSION"));
    let entries: Vec<(&str, &str)> = [
        ("help", "Show available commands"),
        ("help --cli", "Show the tdkserver command line help"),
        ("exit / quit", "Leave the shell (also Ctrl+D)"),
    ]
    .into_iter()
    .chain(SPECIAL_COMMANDS)
    .collect();
    let entries = entries.as_slice();

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
        let label = match SettingsContext::settings_instance() {
            Some(name) => format!("[{name}]").truecolor(0xCC, 0xB2, 0x33).to_string(),
            None => "[no tabsdata instance]".dimmed().to_string(),
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
