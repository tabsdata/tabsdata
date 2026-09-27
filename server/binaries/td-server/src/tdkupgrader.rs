//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde::Serialize;

use td_kubernetes::runtime::instance::upgrade::offline::upgrade_kprofile;
use td_kubernetes::runtime::instance::upgrade::replay::{
    Mismatch, replay_all, stale_templates, write_templates,
};

#[derive(Debug, Parser)]
#[command(
    name = "tdk_kupgrader",
    about = "Apply and verify upgraders offline, writing each outcome as JSON",
    disable_help_subcommand = true
)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Apply the upgrader of a version to a kprofile folder.
    Upgrade {
        #[arg(long, required = true)]
        kupgrade: PathBuf,

        #[arg(long, required = true)]
        kprofile: PathBuf,

        #[arg(long, required = true)]
        version: String,
    },
    /// Upgrade every provider's oldest template to the newest version, hop by
    /// hop, comparing each hop with that version's template.
    Replay {
        #[arg(long, required = true)]
        kupgrade: PathBuf,

        #[arg(long, required = true)]
        templates: PathBuf,

        /// Folder that keeps the upgraded outcome of each diverging hop.
        #[arg(long)]
        diverged: Option<PathBuf>,
    },
    /// Compare, or rewrite, the templates of a version with what its kprofile
    /// folder resolves to, for every supported provider it holds variants for.
    Templates {
        #[arg(long, required = true)]
        kprofile: PathBuf,

        #[arg(long, required = true)]
        templates: PathBuf,

        #[arg(long, required = true)]
        version: String,

        #[arg(long)]
        write: bool,
    },
}

#[derive(Serialize)]
struct Mismatches {
    mismatches: Vec<Mismatch>,
}

#[derive(Serialize)]
struct Failure {
    error: String,
}

async fn run(command: Command) -> Result<String, String> {
    let rendered = match command {
        Command::Upgrade {
            kupgrade,
            kprofile,
            version,
        } => {
            let upgraded = upgrade_kprofile(&kupgrade, &kprofile, &version)
                .await
                .map_err(|error| format!("{error:?}"))?;
            serde_json::to_string(&upgraded)
        }
        Command::Replay {
            kupgrade,
            templates,
            diverged,
        } => {
            let mismatches = replay_all(&kupgrade, &templates, diverged.as_deref())
                .await
                .map_err(|error| format!("{error:?}"))?;
            serde_json::to_string(&Mismatches { mismatches })
        }
        Command::Templates {
            kprofile,
            templates,
            version,
            write,
        } => {
            if write {
                write_templates(&kprofile, &templates, &version)
                    .map_err(|error| format!("{error:?}"))?;
            }
            let mismatches = stale_templates(&kprofile, &templates, &version)
                .map_err(|error| format!("{error:?}"))?;
            serde_json::to_string(&Mismatches { mismatches })
        }
    };
    rendered.map_err(|error| error.to_string())
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();
    match run(arguments.command).await {
        Ok(payload) => {
            println!("{payload}");
            ExitCode::SUCCESS
        }
        Err(error) => fail(&error),
    }
}

fn fail(error: &str) -> ExitCode {
    let failure = Failure {
        error: error.to_owned(),
    };
    match serde_json::to_string(&failure) {
        Ok(payload) => eprintln!("{payload}"),
        Err(_) => eprintln!("{{\"error\":\"The failure could not be serialized\"}}"),
    }
    ExitCode::FAILURE
}
