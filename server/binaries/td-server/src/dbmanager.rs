//
// Copyright 2026 Tabs Data Inc.
//

//! Standalone database-administration CLI: create / upgrade / check / report /
//! drop the schema components of a deployment using the same td-database `system`
//! machinery the apiserver uses at boot, but in its own binary so DB lifecycle can
//! be driven without starting the server. The connection target comes entirely
//! from the shared config's `database:` section; there are no override flags.

use std::env;
use std::process;

use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use td_common::attach::attach;
use td_common::status::ExitStatus;
use td_common::{about, logging};
use td_database::config::DatabaseDescriptor;
use td_database::error::DbError;
use td_process::launcher::cli::Cli;
use td_process::launcher::hooks;
use te_database as dbm;
use tracing::{error, info};

const VERSION: &str = env!("CARGO_PKG_VERSION");

// Keep the schema-component registrations in this binary's final link (see the
// macro docs); without the anchor the linker drops the inventory submissions and
// the registry comes up empty.
te_database::force_link!();

/// Slim configuration: only the `database:` section is relevant to DB admin.
/// Other apiserver config keys present in the file are ignored.
#[derive(Clone, Default, Serialize, Deserialize)]
struct DbManagerConfig {
    #[serde(default)]
    database: DatabaseDescriptor,
}

impl td_process::launcher::config::Config for DbManagerConfig {}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Command {
    /// Start the in-process database server (embedded only; no-op otherwise).
    Start,
    /// Start the in-process database server and stay in the foreground until a
    /// shutdown signal is received, then stop it (embedded only).
    Serve,
    /// Stop the in-process database server
    /// (embedded & local sidecar only; no-op otherwise).
    Stop,
    /// Provision and upgrade all configured schemas.
    Create,
    /// Upgrade all configured schemas to the current version.
    Upgrade,
    /// Verify all configured schemas are current.
    Check,
    /// Report each schema's recorded version.
    Status,
    /// Drop all configured schemas and their owner roles (destructive).
    Drop,
    /// Check that every configured schema is reachable on its serving pool.
    Ping,
}

#[derive(Debug, Clone, Args)]
struct DbManagerParams {
    /// The db-management command to run.
    #[clap(value_enum)]
    command: Command,
    /// Required to run the destructive `drop` command.
    #[clap(long)]
    force: bool,
}

/// Run `op` against a freshly-opened privileged (superuser) system pool, closing
/// it afterward whether the operation succeeds or fails. Only the commands that
/// need superuser DDL use it (`create` provisions schemas/roles, `drop` drops
/// them); `upgrade` runs as the schema owner, and `ping`/`check`/`status` reach
/// the serving pools as the app would, so they stay out of this path.
async fn with_system(
    descriptor: &DatabaseDescriptor,
    op: impl AsyncFnOnce(&dbm::SystemPool) -> Result<(), DbError>,
) -> Result<(), DbError> {
    let system = dbm::connect_system(descriptor).await?;
    let result = op(&system).await;
    system.close().await;
    result
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(terminate) => terminate,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn run() {
    Cli::<DbManagerConfig, DbManagerParams>::exec_async(
        td_apiserver::CONFIG_NAME,
        |config, params| async move {
            let descriptor = &config.database;
            if let Err(e) = dbm::validate_descriptor(descriptor) {
                error!("Invalid database configuration: {e:#?}");
                return ExitStatus::GeneralError;
            }
            if matches!(params.command, Command::Drop) && !params.force {
                error!("Refusing to drop schemas without --force");
                return ExitStatus::GeneralError;
            }

            let outcome: Result<(), DbError> = match params.command {
                // Server lifecycle: bring the in-process embedded server up
                // (detached, so it outlives this command) or stop it. No-op for
                // sidecar/remote, whose server is not ours to run.
                Command::Start => dbm::start_server(descriptor).await,
                Command::Serve => match dbm::start_server(descriptor).await {
                    Ok(()) => {
                        info!("Database server running; waiting for shutdown signal");
                        wait_for_shutdown().await;
                        info!("Shutdown signal received; stopping database server");
                        dbm::stop_server(descriptor).await
                    }
                    Err(e) => Err(e),
                },
                Command::Stop => dbm::stop_server(descriptor).await,
                // App ping: reach each component's serving pool from the
                // descriptor and ping it — the way the apiserver does — with no
                // privileged system pool.
                Command::Ping => dbm::ping_all(descriptor)
                    .await
                    .inspect(|()| info!("Database is reachable")),
                Command::Create => {
                    with_system(descriptor, async |system| {
                        info!("Creating database");
                        dbm::provision_all(descriptor, system).await?;
                        dbm::upgrade_all(descriptor).await?;
                        info!("Database created");
                        Ok(())
                    })
                    .await
                }
                // Upgrade runs as each schema's owner (DDL on its own schema), so
                // it needs no superuser system pool.
                Command::Upgrade => {
                    info!("Upgrading database");
                    dbm::upgrade_all(descriptor)
                        .await
                        .inspect(|()| info!("Database upgraded"))
                }
                // App check: validate the descriptor against the database it
                // connects to — the same correctness check the apiserver runs on
                // its serving pool — with no privileged system pool.
                Command::Check => dbm::check_all(descriptor)
                    .await
                    .inspect(|()| info!("Database is up to date")),
                // App status: read each component's recorded version from its
                // serving pool, no privileged system pool.
                Command::Status => dbm::status(descriptor).await.map(|statuses| {
                    for s in statuses {
                        info!(
                            component = s.key,
                            schema = %s.schema,
                            db_version = ?s.db_version,
                            current = s.current,
                            "schema status"
                        );
                    }
                }),
                Command::Drop => {
                    with_system(descriptor, async |system| {
                        info!("Dropping database");
                        dbm::drop_all(descriptor, system).await?;
                        info!("Database dropped");
                        Ok(())
                    })
                    .await
                }
            };

            match outcome {
                Ok(()) => ExitStatus::Success,
                Err(e) => {
                    error!("db-admin command failed: {e:#?}");
                    ExitStatus::GeneralError
                }
            }
        },
        None,
        None,
    );
}

#[attach(signal = "td_dbmanager")]
fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    logging::start();
    run();
}
