//
// Copyright 2026 Tabsdata Inc.
//

pub mod interactive;
pub mod log;
pub mod silent;

pub use interactive::InteractiveReporter;
pub use log::LogReporter;
pub use silent::SilentReporter;

use tracing::debug;

pub trait Reporter: Send + Sync {
    fn step(&self, message: &str) -> StepHandle;
    fn info(&self, message: &str);
    fn warn(&self, message: &str);
}

pub struct StepHandle {
    backend: Option<Box<dyn StepBackend>>,
    label: String,
}

impl StepHandle {
    pub(super) fn new(backend: Box<dyn StepBackend>, label: &str) -> Self {
        debug!("step starting: {label}");
        Self {
            backend: Some(backend),
            label: label.to_owned(),
        }
    }

    pub fn substep(&self, message: &str) -> StepHandle {
        match self.backend.as_ref() {
            Some(backend) => StepHandle::new(backend.substep(message), message),
            None => StepHandle {
                backend: None,
                label: message.to_owned(),
            },
        }
    }

    pub fn tick(&self, message: &str) {
        if let Some(backend) = self.backend.as_ref() {
            backend.tick(message);
        }
    }

    pub fn info(&self, message: &str) {
        if let Some(backend) = self.backend.as_ref() {
            backend.info(message);
        }
    }

    pub fn warn(&self, message: &str) {
        if let Some(backend) = self.backend.as_ref() {
            backend.warn(message);
        }
    }

    pub fn done(mut self, message: &str) {
        if let Some(backend) = self.backend.take() {
            debug!("step done: {} ({message})", self.label);
            backend.done(message);
        }
    }

    pub fn fail(mut self) {
        if let Some(backend) = self.backend.take() {
            debug!("step failed: {}", self.label);
            backend.fail();
        }
    }
}

impl Drop for StepHandle {
    fn drop(&mut self) {
        if let Some(backend) = self.backend.take() {
            debug!("step abandoned: {}", self.label);
            backend.fail();
        }
    }
}

pub(super) trait StepBackend: Send + Sync {
    fn substep(&self, message: &str) -> Box<dyn StepBackend>;
    fn tick(&self, message: &str);
    fn info(&self, message: &str);
    fn warn(&self, message: &str);
    fn done(self: Box<Self>, message: &str);
    fn fail(self: Box<Self>);
}

pub(super) const STEP_DONE_MARKER: &str = "\u{2714}";
pub(super) const STEP_START_MARKER: &str = "\u{25b6}";
pub(super) const STEP_FAIL_MARKER: &str = "\u{2718}";

#[cfg(test)]
mod tests;
