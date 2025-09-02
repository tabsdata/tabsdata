//
// Copyright 2024 Tabs Data Inc.
//

use clap_derive::Args;
use getset::Getters;
use td_common::logging;
use td_common::status::ExitStatus;
use td_process::launcher::cli::Cli;
use td_process::launcher::config::Config;
use td_process::launcher::hooks;
use tracing::Level;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Getters)]
#[getset(get = "pub")]
struct HelloConfig {
    msg: String,
}

impl Default for HelloConfig {
    fn default() -> Self {
        HelloConfig {
            msg: "Hello, world!".to_string(),
        }
    }
}

impl Config for HelloConfig {}

#[derive(Debug, Clone, Args)]
struct HelloParams {
    #[arg(long)]
    msg: Option<String>,
}

impl HelloParams {
    fn msg(&self, config: HelloConfig) -> String {
        self.msg.clone().unwrap_or_else(|| config.msg().to_string())
    }
}

fn main() {
    hooks::panic();

    logging::start(Level::INFO, None, true);

    Cli::<HelloConfig, HelloParams>::exec_async(
        "helloworld",
        |config, params| async move {
            println!("{}", params.msg(config));
            ExitStatus::Success
        },
        None,
        None,
    );
}
