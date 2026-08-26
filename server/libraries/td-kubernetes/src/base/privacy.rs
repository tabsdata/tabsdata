//
// Copyright 2026 Tabsdata Inc.
//

use crate::base::constants::{HIDDEN_VALUE, SECRET_OPTIONS};

pub fn mask_arguments(arguments: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut masked = Vec::new();
    let mut hide_value = false;
    for argument in arguments {
        if hide_value {
            hide_value = false;
            masked.push(HIDDEN_VALUE.to_owned());
            continue;
        }
        match secret_option(&argument) {
            Some(option) if argument == option => {
                hide_value = true;
                masked.push(argument);
            }
            Some(option) => masked.push(format!("{option}={HIDDEN_VALUE}")),
            None => masked.push(argument),
        }
    }
    masked
}

pub fn mask_command_line(arguments: impl IntoIterator<Item = String>) -> String {
    mask_arguments(arguments).join(" ")
}

fn secret_option(argument: &str) -> Option<&'static str> {
    SECRET_OPTIONS
        .iter()
        .copied()
        .find(|option| argument == *option || argument.starts_with(&format!("{option}=")))
}

#[cfg(test)]
mod tests;
