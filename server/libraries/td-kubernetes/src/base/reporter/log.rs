//
// Copyright 2026 Tabsdata Inc.
//

use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::base::reporter::{Reporter, STEP_DONE_MARKER, StepBackend, StepHandle};

pub struct LogReporter;

impl LogReporter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LogReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Reporter for LogReporter {
    fn step(&self, message: &str) -> StepHandle {
        info!("\u{2192} {message}");
        StepHandle::new(
            Box::new(LogStep {
                started_at: Instant::now(),
                depth: 0,
            }),
            message,
        )
    }

    fn info(&self, message: &str) {
        info!("{message}");
    }

    fn warn(&self, message: &str) {
        warn!("{message}");
    }
}

struct LogStep {
    started_at: Instant,
    depth: usize,
}

impl StepBackend for LogStep {
    fn substep(&self, message: &str) -> Box<dyn StepBackend> {
        let indent = "  ".repeat(self.depth + 1);
        info!("{indent}\u{2192} {message}");
        Box::new(LogStep {
            started_at: Instant::now(),
            depth: self.depth + 1,
        })
    }

    fn tick(&self, message: &str) {
        let indent = "  ".repeat(self.depth);
        info!("{indent}\u{22ef} {message}");
    }

    fn info(&self, message: &str) {
        info!("{message}");
    }

    fn warn(&self, message: &str) {
        warn!("{message}");
    }

    fn done(self: Box<Self>, message: &str) {
        let elapsed =
            humantime::format_duration(Duration::from_secs(self.started_at.elapsed().as_secs()));
        let indent = "  ".repeat(self.depth);
        info!("{indent}{STEP_DONE_MARKER} {message} ({elapsed})");
    }

    fn fail(self: Box<Self>) {}
}
