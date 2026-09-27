//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use serde::Serialize;

use td_kubernetes::runtime::instance::upgrade::offline::upgrade_kprofile;
use td_kubernetes::runtime::instance::upgrade::replay::{
    Mismatch, Sources, drifted_digests, materialize, materialize_scratch, record_digests,
    replay_all,
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

#[derive(Debug, Args)]
struct Repository {
    /// Repository root whose tags, patches and working kprofile give the templates.
    #[arg(long, required = true)]
    repository: PathBuf,

    /// Upgrade tree to use instead of the repository's own.
    #[arg(long)]
    kupgrade: Option<PathBuf>,
}

impl Repository {
    fn sources(&self) -> Sources {
        let sources = Sources::of(&self.repository);
        match &self.kupgrade {
            Some(kupgrade) => sources.with_kupgrade(kupgrade),
            None => sources,
        }
    }
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
        #[command(flatten)]
        repository: Repository,

        /// Folder that keeps the upgraded outcome of each diverging hop.
        #[arg(long)]
        diverged: Option<PathBuf>,
    },
    /// Compare the released templates with their recorded digests, recording
    /// the ones still missing when asked to.
    Digests {
        #[command(flatten)]
        repository: Repository,

        /// Record the digests of released templates that have none.
        #[arg(long)]
        record: bool,
    },
    /// Write the templates of every version to a folder.
    Templates {
        #[command(flatten)]
        repository: Repository,

        /// Folder that receives one templates folder per version and provider.
        #[arg(long, required = true)]
        output: PathBuf,
    },
}

#[derive(Serialize)]
struct Mismatches {
    mismatches: Vec<Mismatch>,
}

#[derive(Serialize)]
struct Recorded {
    recorded: usize,
    mismatches: Vec<Mismatch>,
}

#[derive(Serialize)]
struct Written {
    folder: PathBuf,
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
            repository,
            diverged,
        } => {
            let sources = repository.sources();
            let (_scratch, templates) =
                materialize_scratch(&sources).map_err(|error| format!("{error:?}"))?;
            let mismatches = replay_all(&sources, &templates, diverged.as_deref())
                .await
                .map_err(|error| format!("{error:?}"))?;
            serde_json::to_string(&Mismatches { mismatches })
        }
        Command::Digests { repository, record } => {
            let sources = repository.sources();
            let (_scratch, templates) =
                materialize_scratch(&sources).map_err(|error| format!("{error:?}"))?;
            let recorded = if record {
                record_digests(&sources, &templates).map_err(|error| format!("{error:?}"))?
            } else {
                0
            };
            let mismatches =
                drifted_digests(&sources, &templates).map_err(|error| format!("{error:?}"))?;
            serde_json::to_string(&Recorded {
                recorded,
                mismatches,
            })
        }
        Command::Templates { repository, output } => {
            let templates = materialize(&repository.sources(), &output)
                .map_err(|error| format!("{error:?}"))?;
            serde_json::to_string(&Written {
                folder: templates.folder,
            })
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
