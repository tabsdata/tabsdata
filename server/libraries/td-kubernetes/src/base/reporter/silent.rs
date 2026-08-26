//
// Copyright 2026 Tabsdata Inc.
//

use crate::base::reporter::{Reporter, StepBackend, StepHandle};

pub struct SilentReporter;

impl SilentReporter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SilentReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Reporter for SilentReporter {
    fn step(&self, message: &str) -> StepHandle {
        StepHandle::new(Box::new(SilentStep), message)
    }

    fn info(&self, _message: &str) {}
    fn warn(&self, _message: &str) {}
}

struct SilentStep;

impl StepBackend for SilentStep {
    fn substep(&self, _message: &str) -> Box<dyn StepBackend> {
        Box::new(SilentStep)
    }

    fn tick(&self, _message: &str) {}
    fn info(&self, _message: &str) {}
    fn warn(&self, _message: &str) {}
    fn done(self: Box<Self>, _message: &str) {}
    fn fail(self: Box<Self>) {}
}
