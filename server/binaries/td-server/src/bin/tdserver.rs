//
// Copyright 2024 Tabs Data Inc.
//

use colored::*;
use std::fs;
use std::io::Write;
use tabsdatalib::bin::tdserver;
use td_attach::attach;
use td_build::version::TABSDATA_VERSION;
use td_common::env::{get_home_dir, TABSDATA_HOME_DIR};
use td_common::logging;
use td_common::logging::LogOutput;
use terminal_size::{terminal_size, Width};
use textwrap::fill;
use tm_workspace::workspace_root;
use tracing::Level;

const ACK: &str = ".ack";

const BANNER: &str = include_str!(concat!(
    workspace_root!(),
    "/variant/assets/manifest/BANNER"
));

fn check_banner() -> std::io::Result<()> {
    let ack = get_home_dir().join(TABSDATA_HOME_DIR).join(ACK);
    let ack_version = fs::read_to_string(&ack).unwrap_or_else(|_| "".to_string());
    if ack_version.trim() == TABSDATA_VERSION.trim() {
        return Ok(());
    }
    let _ = show_banner();
    if let Some(parent) = ack.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(&ack)?;
    file.write_all(TABSDATA_VERSION.trim().as_bytes())?;
    Ok(())
}

fn show_banner() -> Result<(), std::io::Error> {
    let width = terminal_size()
        .map(|(Width(w), _)| w as usize - 6)
        .unwrap_or(50)
        .min(80);
    let wrapped_text = fill(BANNER, width);
    let top_border = format!("╭{}╮", "─".repeat(width + 2)).blue().bold();
    let bottom_border = format!("╰{}╯", "─".repeat(width + 2)).blue().bold();
    println!("\n{}", top_border);
    for line in wrapped_text.lines() {
        let padded_line = format!("{:^width$}", line, width = width);
        println!(
            "{} {} {}",
            "│".blue().bold(),
            padded_line.truecolor(251, 175, 79).bold(),
            "│".blue().bold()
        );
    }
    println!("{}", bottom_border);
    Ok(())
}

#[tokio::main]
#[attach(signal = "tdserver")]
async fn main() {
    logging::start(Level::DEBUG, Some(LogOutput::StdOut), false);
    let _ = check_banner();
    tdserver::start().await;
}
