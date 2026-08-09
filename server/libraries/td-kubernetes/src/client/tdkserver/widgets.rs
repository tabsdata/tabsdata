//
// Copyright 2026 Tabsdata Inc.
//

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use terminal_size::{Width, terminal_size};
use textwrap::{Options, WordSeparator, fill};
use tm_workspace::workspace_root;

const BANNER: &str = include_str!(concat!(
    workspace_root!(),
    "/variant/assets/manifest/BANNER"
));

const BOX_FRAME: usize = 4;
const BORDER_GREEN: (u8, u8, u8) = (130, 195, 105);
const BODY_GREEN: (u8, u8, u8) = (194, 255, 38);
const BORDER_ORANGE: (u8, u8, u8) = (215, 95, 0);
const BODY_ORANGE: (u8, u8, u8) = (255, 175, 95);
const BORDER_YELLOW: (u8, u8, u8) = (215, 175, 0);
const BODY_YELLOW: (u8, u8, u8) = (255, 215, 95);

pub fn show_box(text: &str, min: usize, spaced: bool) {
    show_colored_box(text, min, BORDER_GREEN, BODY_GREEN, true, false, spaced)
}

pub fn show_notice(text: &str, min: usize, spaced: bool) {
    show_colored_box(text, min, BORDER_ORANGE, BODY_ORANGE, false, false, spaced)
}

pub fn show_hint(text: &str, min: usize, spaced: bool) {
    show_colored_box(text, min, BORDER_YELLOW, BODY_YELLOW, false, true, spaced)
}

fn show_colored_box(
    text: &str,
    min: usize,
    border: (u8, u8, u8),
    body: (u8, u8, u8),
    centered: bool,
    keep_lines: bool,
    spaced: bool,
) {
    #[cfg(not(windows))]
    let use_colors = supports_color::on(supports_color::Stream::Stdout).is_some();
    #[cfg(windows)]
    let use_colors = false;

    let total = terminal_size()
        .map(|(Width(width), _)| width as usize)
        .unwrap_or(min)
        .min(min);
    let width = total.saturating_sub(BOX_FRAME);
    let wrap_options = Options::new(width)
        .break_words(false)
        .word_splitter(textwrap::WordSplitter::NoHyphenation)
        .word_separator(WordSeparator::AsciiSpace);
    let wrapped_text = if keep_lines {
        text.trim_end()
            .lines()
            .map(|line| fill(line, wrap_options.clone()))
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        let trimmed_text: String = text
            .lines()
            .map(|line| line.trim())
            .collect::<Vec<_>>()
            .join("\n");
        fill(trimmed_text.trim(), wrap_options)
    };
    let width = wrapped_text
        .lines()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0)
        .min(width);
    let top_border = if use_colors {
        format!("╭{}╮", "─".repeat(width + 2))
            .truecolor(border.0, border.1, border.2)
            .bold()
            .to_string()
    } else {
        format!("╭{}╮", "─".repeat(width + 2))
    };
    let bottom_border = if use_colors {
        format!("╰{}╯", "─".repeat(width + 2))
            .truecolor(border.0, border.1, border.2)
            .bold()
            .to_string()
    } else {
        format!("╰{}╯", "─".repeat(width + 2))
    };
    if spaced {
        println!();
    }
    println!("{top_border}");
    for line in wrapped_text.lines() {
        let padded_line = if centered {
            format!("{line:^width$}")
        } else {
            format!("{line:<width$}")
        };
        if use_colors {
            let edge = "│".truecolor(border.0, border.1, border.2).bold();
            println!(
                "{} {} {}",
                edge,
                padded_line.truecolor(body.0, body.1, body.2),
                edge
            );
        } else {
            println!("│ {padded_line} │");
        }
    }
    println!("{bottom_border}\n");
}

pub fn show_banner() {
    show_colored_box(BANNER, 80, BORDER_GREEN, BODY_GREEN, true, false, false)
}

pub struct Spinner {
    multi: MultiProgress,
    bar: ProgressBar,
}

pub fn spinner(message: &str) -> Spinner {
    let multi = td_common::logging::multi_progress().unwrap_or_default();
    let bar = multi.add(ProgressBar::new_spinner());
    bar.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg} {elapsed:.dim}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    bar.set_message(message.to_owned());
    bar.enable_steady_tick(Duration::from_millis(120));
    Spinner { multi, bar }
}

const SPINNER_DONE_MARKER: &str = "\u{2714}";

impl Spinner {
    pub fn multi(&self) -> &MultiProgress {
        &self.multi
    }

    pub fn finish(self, done_message: &str) {
        let elapsed = humantime::format_duration(Duration::from_secs(self.bar.elapsed().as_secs()));
        let line = format!("{SPINNER_DONE_MARKER} {done_message} ({elapsed})");
        let _ = self.multi.println(&line);
        self.bar.finish_and_clear();
    }

    pub fn finish_and_clear(self) {
        self.bar.finish_and_clear();
    }
}

static KEEP_PROGRESS: AtomicBool = AtomicBool::new(true);

pub fn set_keep_progress(keep: bool) {
    KEEP_PROGRESS.store(keep, Ordering::Relaxed);
}

pub fn keep_progress() -> bool {
    KEEP_PROGRESS.load(Ordering::Relaxed)
}

pub struct PhaseSpinner {
    multi: MultiProgress,
    header: ProgressBar,
    resources: Mutex<Vec<ProgressBar>>,
}

pub fn phase_spinner(message: &str) -> PhaseSpinner {
    let multi = td_common::logging::multi_progress().unwrap_or_default();
    phase_spinner_in(&multi, message)
}

pub fn phase_spinner_in(parent: &MultiProgress, message: &str) -> PhaseSpinner {
    let multi = parent.clone();
    let header = multi.add(ProgressBar::new_spinner());
    header.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg} {elapsed:.dim}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    header.set_message(message.to_owned());
    header.enable_steady_tick(Duration::from_millis(120));
    PhaseSpinner {
        multi,
        header,
        resources: Mutex::new(Vec::new()),
    }
}

impl PhaseSpinner {
    pub fn add_resource(&self, name: &str) -> ProgressBar {
        let bar = self.multi.add(ProgressBar::new_spinner());
        bar.set_style(
            ProgressStyle::with_template("  {spinner:.dim} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        bar.set_message(name.to_owned());
        bar.enable_steady_tick(Duration::from_millis(120));
        if let Ok(mut bars) = self.resources.lock() {
            bars.push(bar.clone());
        }
        bar
    }

    fn flush_resources(&self) {
        let Ok(mut bars) = self.resources.lock() else {
            return;
        };
        for bar in bars.drain(..) {
            if keep_progress() {
                let _ = self.multi.println(format!("  {}", bar.message()));
            }
            bar.finish_and_clear();
        }
    }
}

impl Drop for PhaseSpinner {
    fn drop(&mut self) {
        self.flush_resources();
        if keep_progress() {
            let elapsed =
                humantime::format_duration(Duration::from_secs(self.header.elapsed().as_secs()));
            let line = format!(
                "{SPINNER_DONE_MARKER} {} ({elapsed})",
                self.header.message()
            );
            let _ = self.multi.println(&line);
        }
        self.header.finish_and_clear();
    }
}

#[cfg(test)]
mod tests;
