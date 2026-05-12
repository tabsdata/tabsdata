//
// Copyright 2026 Tabs Data Inc.
//

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

pub fn show_box(text: &str, min: usize) {
    #[cfg(not(windows))]
    let use_colors = supports_color::on(supports_color::Stream::Stdout).is_some();
    #[cfg(windows)]
    let use_colors = false;

    let width = terminal_size()
        .map(|(Width(width), _)| width as usize - 6)
        .unwrap_or(50)
        .min(min);
    let trimmed_text: String = text
        .lines()
        .map(|line| line.trim())
        .collect::<Vec<_>>()
        .join("\n");
    let wrap_options = Options::new(width)
        .break_words(false)
        .word_splitter(textwrap::WordSplitter::NoHyphenation)
        .word_separator(WordSeparator::AsciiSpace);
    let wrapped_text = fill(trimmed_text.trim(), wrap_options);
    let top_border = if use_colors {
        format!("╭{}╮", "─".repeat(width + 2))
            .blue()
            .bold()
            .to_string()
    } else {
        format!("╭{}╮", "─".repeat(width + 2))
    };
    let bottom_border = if use_colors {
        format!("╰{}╯", "─".repeat(width + 2))
            .blue()
            .bold()
            .to_string()
    } else {
        format!("╰{}╯", "─".repeat(width + 2))
    };
    println!("\n{top_border}");
    for line in wrapped_text.lines() {
        let padded_line = format!("{line:^width$}");
        if use_colors {
            println!(
                "{} {} {}",
                "│".blue().bold(),
                padded_line.truecolor(194, 255, 38).bold(),
                "│".blue().bold()
            );
        } else {
            println!("│ {padded_line} │");
        }
    }
    println!("{bottom_border}");
}

pub fn show_banner() {
    show_box(BANNER, 80);
}

pub fn spinner(message: &str) -> ProgressBar {
    let bar = ProgressBar::new_spinner();
    bar.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg} {elapsed:.dim}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    bar.set_message(message.to_owned());
    bar.enable_steady_tick(Duration::from_millis(120));
    bar
}

pub struct PhaseSpinner {
    multi: MultiProgress,
    header: ProgressBar,
}

pub fn phase_spinner(message: &str) -> PhaseSpinner {
    let multi = MultiProgress::new();
    let header = multi.add(ProgressBar::new_spinner());
    header.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg} {elapsed:.dim}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    header.set_message(message.to_owned());
    header.enable_steady_tick(Duration::from_millis(120));
    PhaseSpinner { multi, header }
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
        bar
    }
}

impl Drop for PhaseSpinner {
    fn drop(&mut self) {
        self.header.finish_and_clear();
        let _ = self.multi.clear();
    }
}

#[cfg(test)]
mod tests;
