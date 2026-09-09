//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use serde::Serialize;

use td_kubernetes::runtime::instance::upgrade::offline::{Upgraded, upgrade_kprofile};

#[derive(Debug, Parser)]
#[command(
    name = "tdk_kupgrader",
    about = "Apply an upgrader to a kprofile folder and write its outcome as JSON",
    disable_help_subcommand = true
)]
struct Arguments {
    #[arg(long, required = true)]
    kupgrade: PathBuf,

    #[arg(long, required = true)]
    kprofile: PathBuf,

    #[arg(long, required = true)]
    version: String,
}

#[derive(Serialize)]
struct Failure {
    error: String,
}

async fn run(arguments: &Arguments) -> Result<Upgraded, String> {
    upgrade_kprofile(&arguments.kupgrade, &arguments.kprofile, &arguments.version)
        .await
        .map_err(|error| format!("{error:?}"))
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();
    match run(&arguments).await {
        Ok(upgraded) => match serde_json::to_string(&upgraded) {
            Ok(payload) => {
                println!("{payload}");
                ExitCode::SUCCESS
            }
            Err(error) => fail(&error.to_string()),
        },
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
