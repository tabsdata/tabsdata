//
// Copyright 2026 Tabs Data Inc.
//

pub mod container;
pub mod io;
pub mod process;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub path: String,
    pub message: String,
}

impl ValidationError {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests;
