//
// Copyright 2026 Tabsdata Inc.
//

pub mod interactive;
pub mod log;
pub mod silent;

pub use interactive::InteractiveReporter;
pub use log::LogReporter;
pub use silent::SilentReporter;

pub trait Reporter: Send + Sync {
    fn step(&self, message: &str) -> StepHandle;
    fn info(&self, message: &str);
    fn warn(&self, message: &str);
}

pub struct StepHandle {
    backend: Option<Box<dyn StepBackend>>,
}

impl StepHandle {
    pub(super) fn new(backend: Box<dyn StepBackend>) -> Self {
        Self {
            backend: Some(backend),
        }
    }

    pub fn substep(&self, message: &str) -> StepHandle {
        match self.backend.as_ref() {
            Some(backend) => StepHandle::new(backend.substep(message)),
            None => StepHandle { backend: None },
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
            backend.done(message);
        }
    }

    pub fn fail(mut self) {
        if let Some(backend) = self.backend.take() {
            backend.fail();
        }
    }
}

impl Drop for StepHandle {
    fn drop(&mut self) {
        if let Some(backend) = self.backend.take() {
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
