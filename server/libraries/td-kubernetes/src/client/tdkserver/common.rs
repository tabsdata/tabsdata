//
// Copyright 2026 Tabs Data Inc.
//

use std::io::{self, BufRead, Write};

use crate::base::constants::{VALUE_N, VALUE_NO, VALUE_Y, VALUE_YES};

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

pub fn prompt_yes_no(message: &str) -> bool {
    let stdin = io::stdin();
    loop {
        print!("{message} [{VALUE_Y}/{VALUE_YES}/{VALUE_N}/{VALUE_NO}]: ");
        io::stdout().flush().ok();
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) | Err(_) => return false,
            Ok(_) => {}
        }
        if let Some(answer) = parse_yes_no(line.trim()) {
            return answer;
        }
    }
}

#[cfg(test)]
mod tests;
