//
// Copyright 2026 Tabsdata Inc.
//

use std::env;
use std::env::{join_paths, split_paths, var_os};
use std::ffi::{OsStr, OsString};

pub const TD_BEFORE_PATH: &str = "TD_BEFORE_PATH";
pub const TD_AFTER_PATH: &str = "TD_AFTER_PATH";

const PATH: &str = "PATH";

pub fn extend_path() {
    if let Some(extended) =
        extended_path(var_os(PATH), var_os(TD_BEFORE_PATH), var_os(TD_AFTER_PATH))
    {
        unsafe { env::set_var(PATH, extended) };
    }
}

fn extended_path(
    path: Option<OsString>,
    before: Option<OsString>,
    after: Option<OsString>,
) -> Option<OsString> {
    let before = entries(before.as_deref());
    let after = entries(after.as_deref());
    if before.is_empty() && after.is_empty() {
        return None;
    }
    let current = entries(path.as_deref());
    let mut extended: Vec<OsString> = Vec::new();
    for entry in before.into_iter().chain(current).chain(after) {
        if !extended.contains(&entry) {
            extended.push(entry);
        }
    }
    join_paths(extended).ok()
}

fn entries(value: Option<&OsStr>) -> Vec<OsString> {
    match value {
        Some(value) => split_paths(value)
            .filter(|entry| !entry.as_os_str().is_empty())
            .map(|entry| entry.into_os_string())
            .collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests;
