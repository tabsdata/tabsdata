//
// Copyright 2026 Tabs Data Inc.
//

use std::io::{self, BufRead, Write};
use std::sync::{Mutex, OnceLock, mpsc};
use std::thread;
use std::time::Duration;

use td_common::status::ExitStatus;

use crate::base::constants::{VALUE_N, VALUE_NO, VALUE_Y, VALUE_YES};
use crate::cerror;
use crate::core::system::exit::{CommandResult, abort};

pub const PROMPT_TIMEOUT: Duration = Duration::from_secs(60);

const MAX_INSTANCE_NAME_LENGTH: usize = 256;

const FORBIDDEN_CHARACTERS: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*'];

const RESERVED_WINDOWS_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

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

pub fn prompt_yes_no(message: &str) -> Confirmation {
    prompt_yes_no_with_timeout(message, PROMPT_TIMEOUT)
}

fn prompt_yes_no_with_timeout(message: &str, timeout: Duration) -> Confirmation {
    let receiver = stdin_lines().lock().unwrap();
    let started = std::time::Instant::now();
    loop {
        print!("{message} [{VALUE_Y}/{VALUE_YES}/{VALUE_N}/{VALUE_NO}]: ");
        io::stdout().flush().ok();
        let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
            return Confirmation::TimedOut;
        };
        match receiver.recv_timeout(remaining) {
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

pub fn confirm(skip: bool, message: &str) -> CommandResult<bool> {
    if skip {
        return Ok(true);
    }
    match prompt_yes_no(message) {
        Confirmation::Yes => Ok(true),
        Confirmation::No => Ok(false),
        Confirmation::TimedOut => {
            cerror!(
                "No response received within {} seconds; the operation was cancelled.",
                PROMPT_TIMEOUT.as_secs()
            );
            abort(ExitStatus::Aborted)
        }
    }
}

#[cfg(test)]
mod tests;
