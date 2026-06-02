//
// Copyright 2026 Tabs Data Inc.
//

use std::time::{Duration, Instant};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::base::reporter::{Reporter, STEP_DONE_MARKER, StepBackend, StepHandle};

pub struct InteractiveReporter {
    multi: MultiProgress,
}

impl InteractiveReporter {
    pub fn new() -> Self {
        Self {
            multi: td_common::logging::multi_progress().unwrap_or_default(),
        }
    }
}

impl Default for InteractiveReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Reporter for InteractiveReporter {
    fn step(&self, message: &str) -> StepHandle {
        StepHandle::new(Box::new(InteractiveStep::new(
            self.multi.clone(),
            message,
            0,
        )))
    }

    fn info(&self, message: &str) {
        let _ = self.multi.println(message);
    }

    fn warn(&self, message: &str) {
        let _ = self.multi.println(format!("warning: {message}"));
    }
}

struct InteractiveStep {
    multi: MultiProgress,
    bar: ProgressBar,
    started_at: Instant,
    depth: usize,
}

impl InteractiveStep {
    fn new(multi: MultiProgress, message: &str, depth: usize) -> Self {
        let bar = multi.add(ProgressBar::new_spinner());
        bar.set_style(style_for_depth(depth));
        bar.set_message(message.to_owned());
        bar.enable_steady_tick(Duration::from_millis(120));
        Self {
            multi,
            bar,
            started_at: Instant::now(),
            depth,
        }
    }
}

fn style_for_depth(depth: usize) -> ProgressStyle {
    let template = if depth == 0 {
        "{spinner:.cyan} {msg} {elapsed:.dim}".to_owned()
    } else {
        let indent = "  ".repeat(depth);
        format!("{indent}{{spinner:.dim}} {{msg}}")
    };
    ProgressStyle::with_template(&template).unwrap_or_else(|_| ProgressStyle::default_spinner())
}

impl StepBackend for InteractiveStep {
    fn substep(&self, message: &str) -> Box<dyn StepBackend> {
        Box::new(InteractiveStep::new(
            self.multi.clone(),
            message,
            self.depth + 1,
        ))
    }

    fn tick(&self, message: &str) {
        self.bar.set_message(message.to_owned());
    }

    fn info(&self, message: &str) {
        let _ = self.multi.println(message);
    }

    fn warn(&self, message: &str) {
        let _ = self.multi.println(format!("warning: {message}"));
    }

    fn done(self: Box<Self>, message: &str) {
        if self.depth == 0 {
            let elapsed = humantime::format_duration(Duration::from_secs(
                self.started_at.elapsed().as_secs(),
            ));
            let line = format!("{STEP_DONE_MARKER} {message} ({elapsed})");
            let _ = self.multi.println(&line);
        } else {
            let indent = "  ".repeat(self.depth);
            let line = format!("{indent}{STEP_DONE_MARKER} {message}");
            let _ = self.multi.println(&line);
        }
        self.bar.finish_and_clear();
    }

    fn fail(self: Box<Self>) {
        self.bar.finish_and_clear();
    }
}
