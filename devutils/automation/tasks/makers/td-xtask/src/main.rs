//
// Copyright 2024 Tabs Data Inc.
//

use clap::{Parser, command};
use clap_derive::Subcommand;
use std::fs::remove_file;
use std::io::Error;
#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::symlink;
#[cfg(target_os = "windows")]
use std::os::windows::fs::symlink_file as symlink;
use std::path::{Path, PathBuf};
use std::process::exit;
use td_build::version::TABSDATA_VERSION;

#[cfg(not(target_os = "windows"))]
const TARGET: &str = "./target/";
#[cfg(target_os = "windows")]
const TARGET: &str = ".\\target\\";

#[cfg(not(target_os = "windows"))]
const EXTENSION: &str = "";
#[cfg(target_os = "windows")]
const EXTENSION: &str = ".exe";

const FAILURE_EXIST_STATUS: i32 = 1;

const PROFILE_DEV: &str = "dev";
const PROFILE_DEBUG: &str = "debug";

#[derive(clap_derive::Parser)]
#[command(name = "Tabsdata X-task", version = TABSDATA_VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Link {
        #[arg(long, required = true)]
        profile: String,

        #[arg(long)]
        program: Option<String>,

        #[arg(long, value_delimiter = ';')]
        symlinks: Option<Vec<String>>,
    },
}

fn main() -> Result<(), Error> {
    let parser = Cli::parse();
    match &parser.command {
        Commands::Link {
            profile,
            program,
            symlinks,
        } => {
            if let Err(e) = link(profile.clone(), program.clone(), symlinks.clone()) {
                eprintln!("Error running link X-task: {e:?}");
                exit(FAILURE_EXIST_STATUS);
            }
        }
    }
    Ok(())
}

/// Cargo make task that creates symlinks for the provided program and symlinks. The actual values
/// are provided from Makefile.toml.
fn link(
    profile: String,
    program: Option<String>,
    symlinks: Option<Vec<String>>,
) -> Result<(), Error> {
    let profile_folder = if profile == PROFILE_DEV {
        PROFILE_DEBUG.to_string()
    } else {
        profile
    };
    println!("Using profile folder '{profile_folder}'");
    if program.is_none() {
        println!("No program provided; skipping linking");
        return Ok(());
    }
    if symlinks.is_none() {
        println!("No symlinks provided; skipping linking");
        return Ok(());
    }
    let program = program.unwrap();
    let symlinks = symlinks.unwrap();
    if program.is_empty() {
        println!("No program provided; skipping linking");
        return Ok(());
    }
    if symlinks.is_empty() {
        println!("No symlinks provided; skipping linking");
        return Ok(());
    }
    println!("Using program '{program}'");
    println!("Using symlinks '{}'", symlinks.join(", "));
    let target = format!("{TARGET}{profile_folder}");
    let program = format!("{program}{EXTENSION}");
    for link in symlinks {
        if link.trim().is_empty() {
            continue;
        }
        println!("Read symlink: '{link:?}'");
        let link = format!("{link}{EXTENSION}");
        let link_path = PathBuf::from(&target.clone()).join(&link);
        if link_path.exists() {
            println!("Deleting symlink: '{link_path:?}'");
            remove_file(&link_path)?;
        }
        println!("Creating symlink: '{:?}' to '{:?}'", &program, link_path);
        symlink(Path::new(&program), Path::new(&link_path))?;
    }
    Ok(())
}
