//
// Copyright 2024 Tabs Data Inc.
//

use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct RuntimeError {
    message: String,
}

impl RuntimeError {
    pub fn new(message: String) -> Self {
        RuntimeError { message }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Runtime error: {}", self.message)
    }
}

impl Error for RuntimeError {}
