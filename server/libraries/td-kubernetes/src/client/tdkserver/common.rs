//
// Copyright 2026 Tabsdata Inc.
//

use std::io::{self, BufRead, IsTerminal, Write};
use std::sync::{Mutex, OnceLock, mpsc};
use std::thread;
use std::time::Duration;

use humantime::format_duration;

use td_common::status::ExitStatus;

use crate::base::constants::{VALUE_N, VALUE_NO, VALUE_Y, VALUE_YES};
use crate::core::system::exit::{CommandResult, abort};
use crate::{cerror, cwarn};

pub const PROMPT_TIMEOUT_SECONDS: i64 = -1;

pub const LONG_PROMPT_TIMEOUT_SECONDS: i64 = -1;

pub const PROMPT_TIMEOUT: Option<Duration> = timeout_of(PROMPT_TIMEOUT_SECONDS);

pub const LONG_PROMPT_TIMEOUT: Option<Duration> = timeout_of(LONG_PROMPT_TIMEOUT_SECONDS);

fn budget_phrase(timeout: Option<Duration>) -> String {
    match timeout {
        Some(budget) => format!("within {} seconds", budget.as_secs()),
        None => "while waiting forever".to_owned(),
    }
}

const fn timeout_of(seconds: i64) -> Option<Duration> {
    if seconds < 0 {
        None
    } else {
        Some(Duration::from_secs(seconds as u64))
    }
}

const YES_FLAG: &str = "--yes";

const MAX_INSTANCE_NAME_LENGTH: usize = 256;

const FORBIDDEN_CHARACTERS: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*'];

const RESERVED_WINDOWS_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

pub fn interactive() -> bool {
    io::stdin().is_terminal() && io::stdout().is_terminal()
}

fn require_interactive(what: &str) -> CommandResult<bool> {
    if interactive() {
        return Ok(true);
    }
    cerror!(
        "This command needs to ask {what}, but it is not running in a \
         terminal, so nothing can answer. Pass {YES_FLAG} to accept every \
         prompt up front when running unattended."
    );
    abort(ExitStatus::Aborted)
}

pub fn validate_instance_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Instance name cannot be empty.".to_owned());
    }
    if name.len() > MAX_INSTANCE_NAME_LENGTH {
        return Err(format!(
            "Instance name '{name}' is too long ({} characters). Maximum length is {MAX_INSTANCE_NAME_LENGTH}.",
            name.len()
        ));
    }
    if let Some(character) = name
        .chars()
        .find(|character| FORBIDDEN_CHARACTERS.contains(character))
    {
        return Err(format!(
            "Instance name '{name}' contains the reserved character '{character}'."
        ));
    }
    if let Some(character) = name.chars().find(|character| character.is_control()) {
        return Err(format!(
            "Instance name '{name}' contains a control character (codepoint U+{:04X}).",
            character as u32
        ));
    }
    if name.ends_with(' ') || name.ends_with('.') {
        return Err(format!(
            "Instance name '{name}' cannot end with a space or a dot."
        ));
    }
    let stem = name.split('.').next().unwrap_or(name);
    if RESERVED_WINDOWS_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(stem))
    {
        return Err(format!(
            "Instance name '{name}' uses a reserved Windows name."
        ));
    }
    Ok(())
}

pub fn parse_yes_no(value: &str) -> Option<bool> {
    match value.to_lowercase().as_str() {
        VALUE_Y | VALUE_YES => Some(true),
        VALUE_N | VALUE_NO => Some(false),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmation {
    Yes,
    No,
    TimedOut,
}

enum LineOutcome {
    Line(String),
    Closed,
}

static STDIN_LINES: OnceLock<Mutex<mpsc::Receiver<LineOutcome>>> = OnceLock::new();

fn stdin_lines() -> &'static Mutex<mpsc::Receiver<LineOutcome>> {
    STDIN_LINES.get_or_init(|| {
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let stdin = io::stdin();
            loop {
                let mut line = String::new();
                let outcome = match stdin.lock().read_line(&mut line) {
                    Ok(0) | Err(_) => LineOutcome::Closed,
                    Ok(_) => LineOutcome::Line(line),
                };
                let closed = matches!(outcome, LineOutcome::Closed);
                if sender.send(outcome).is_err() || closed {
                    break;
                }
            }
        });
        Mutex::new(receiver)
    })
}

fn prompt_yes_no_with_timeout(message: &str, timeout: Option<Duration>) -> Confirmation {
    let receiver = stdin_lines().lock().unwrap();
    let started = std::time::Instant::now();
    loop {
        print!("{message} [{VALUE_Y}/{VALUE_YES}/{VALUE_N}/{VALUE_NO}]: ");
        io::stdout().flush().ok();
        let outcome = match timeout {
            None => receiver
                .recv()
                .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            Some(budget) => {
                let Some(remaining) = budget.checked_sub(started.elapsed()) else {
                    return Confirmation::TimedOut;
                };
                receiver.recv_timeout(remaining)
            }
        };
        match outcome {
            Ok(LineOutcome::Line(line)) => {
                if let Some(answer) = parse_yes_no(line.trim()) {
                    return if answer {
                        Confirmation::Yes
                    } else {
                        Confirmation::No
                    };
                }
            }
            Ok(LineOutcome::Closed) => return Confirmation::No,
            Err(mpsc::RecvTimeoutError::Timeout) => return Confirmation::TimedOut,
            Err(mpsc::RecvTimeoutError::Disconnected) => return Confirmation::No,
        }
    }
}

pub fn confirm_name(skip: bool, name: &str) -> CommandResult<bool> {
    if skip {
        return Ok(true);
    }
    require_interactive("you to type the instance name")?;
    let receiver = stdin_lines().lock().unwrap();
    let started = std::time::Instant::now();
    loop {
        print!("Type the instance name '{name}' to confirm: ");
        io::stdout().flush().ok();
        let outcome = match PROMPT_TIMEOUT {
            None => receiver
                .recv()
                .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            Some(budget) => {
                let Some(remaining) = budget.checked_sub(started.elapsed()) else {
                    cerror!(
                        "This prompt was waiting for you to type the instance \
                         name, and nothing was entered {}; the operation was \
                         cancelled before it started, so it is safe to run \
                         again.",
                        budget_phrase(Some(budget))
                    );
                    return abort(ExitStatus::Aborted);
                };
                receiver.recv_timeout(remaining)
            }
        };
        match outcome {
            Ok(LineOutcome::Line(line)) => {
                let answer = line.trim();
                if answer == name {
                    return Ok(true);
                }
                cwarn!("'{answer}' is not '{name}'.");
            }
            Ok(LineOutcome::Closed) => return Ok(false),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                cerror!(
                    "This prompt was waiting for you to type the instance name, \
                     and nothing was entered {}; the operation was cancelled \
                     before it started, so it is safe to run again.",
                    budget_phrase(PROMPT_TIMEOUT)
                );
                return abort(ExitStatus::Aborted);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(false),
        }
    }
}

pub fn confirm(skip: bool, message: &str) -> CommandResult<bool> {
    confirm_within(skip, message, PROMPT_TIMEOUT)
}

pub async fn confirm_resume(what: &str, waited: Duration) -> bool {
    if !interactive() {
        cerror!(
            "The wait for {what} exceeded its budget and there is no terminal \
             to ask whether to keep waiting. Pass {YES_FLAG} to keep waiting \
             when running unattended."
        );
        return false;
    }
    let message = format!(
        "The wait for {what} exceeded {}. Continue waiting?",
        format_duration(Duration::from_secs(waited.as_secs()))
    );
    let asked = tokio::task::spawn_blocking(move || {
        let ask = || prompt_yes_no_with_timeout(&message, PROMPT_TIMEOUT);
        match td_common::logging::multi_progress() {
            Some(multi) => multi.suspend(ask),
            None => ask(),
        }
    })
    .await;
    match asked {
        Ok(Confirmation::Yes) => true,
        Ok(_) => {
            cerror!(
                "This prompt was waiting for your answer about whether to keep \
                 waiting, and nothing was entered {}; the operation was \
                 cancelled.",
                budget_phrase(PROMPT_TIMEOUT)
            );
            false
        }
        Err(_) => false,
    }
}

pub fn confirm_at_leisure(skip: bool, message: &str) -> CommandResult<bool> {
    confirm_within(skip, message, LONG_PROMPT_TIMEOUT)
}

fn confirm_within(skip: bool, message: &str, timeout: Option<Duration>) -> CommandResult<bool> {
    if skip {
        return Ok(true);
    }
    require_interactive("for confirmation")?;
    match prompt_yes_no_with_timeout(message, timeout) {
        Confirmation::Yes => Ok(true),
        Confirmation::No => Ok(false),
        Confirmation::TimedOut => {
            cerror!(
                "This prompt was waiting for your answer, and nothing was \
                 entered {}; the operation was cancelled before it started, so \
                 it is safe to run again.",
                budget_phrase(timeout)
            );
            abort(ExitStatus::Aborted)
        }
    }
}

#[cfg(test)]
mod tests;
