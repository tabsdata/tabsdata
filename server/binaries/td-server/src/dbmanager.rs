//
// Copyright 2026 Tabsdata Inc.
//

//! Standalone database-administration CLI, two command groups that never mix
//! configs: `server start|serve|stop` reads only the `database_server:` entry,
//! `db create|upgrade|check|status|drop|ping` reads the `database:` map
//! (`create`/`drop` also need the server entry for the privileged pool).
//!
//! Import rule: registry-dependent operations go through `te_database` (the
//! edition facade, aliased `dbm`); server lifecycle and config types come
//! from `td_database` directly (edition-agnostic).

use std::env;
use std::process;

use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use td_common::attach::attach;
use td_common::status::ExitStatus;
use td_common::{about, logging};
use td_database::config::{ComponentConfig, DatabaseComponents};
use td_database::error::DbError;
use td_database::server::{bootable_server, serve_server, start_server, stop_server};
use td_process::launcher::cli::Cli;
use td_process::launcher::hooks;
use te_database as dbm;
use tracing::{error, info, warn};

const VERSION: &str = env!("CARGO_PKG_VERSION");

// Without this anchor the linker drops the inventory submissions and the
// component registry comes up empty.
te_database::force_link!();

/// The two sections this binary consumes; other apiserver config keys in the
/// file are ignored.
#[derive(Clone, Default, Serialize, Deserialize)]
struct DbManagerConfig {
    #[serde(default)]
    database_server: Option<ComponentConfig>,
    #[serde(default)]
    database: DatabaseComponents,
}

impl td_process::launcher::config::Config for DbManagerConfig {}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    /// Run the database server (reads only `database_server:`).
    #[command(subcommand)]
    Server(ServerCommand),
    /// Manage the schema components (reads `database:`; `create`/`drop` also
    /// need `database_server:`).
    #[command(subcommand)]
    Db(DbCommand),
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum ServerCommand {
    /// Start the database server (no-op without a local `database_server:`).
    Start,
    /// Start the database server and stay in the foreground until a shutdown
    /// signal is received, then stop it (requires a local `database_server:`).
    Serve,
    /// Stop the database server (no-op without a local `database_server:`).
    Stop,
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum DbCommand {
    /// Provision and upgrade all configured schemas.
    Create,
    /// Upgrade all configured schemas to the current version.
    Upgrade,
    /// Verify all configured schemas are current.
    Check,
    /// Report each schema's recorded version.
    Status,
    /// Drop all configured schemas and their owner roles (destructive).
    Drop {
        /// Required to run this destructive command.
        #[clap(long)]
        force: bool,
    },
    /// Check that every configured schema is reachable on its serving pool.
    Ping,
}

#[derive(Debug, Clone, Args)]
struct DbManagerParams {
    // Optional so the bare `--config default|current` invocations (exclusive
    // flag, handled before the app runs) still parse; run() requires it.
    #[command(subcommand)]
    command: Option<Command>,
}

/// `serve` exists to BE the database process: anything less than a bootable
/// local server entry is a misconfiguration, not a no-op.
async fn serve(server: Option<&ComponentConfig>) -> Result<(), DbError> {
    bootable_server(server)?;
    let handle = serve_server(server).await?;
    info!("Database server running; waiting for shutdown signal");
    wait_for_shutdown().await;
    info!("Shutdown signal received; stopping database server");
    match handle {
        Some(handle) => handle.stop().await,
        None => stop_server(server).await,
    }
}

/// Provision every component's schema and owner role, then upgrade each.
async fn create(
    components: &DatabaseComponents,
    server: Option<&ComponentConfig>,
) -> Result<(), DbError> {
    if let Some(server) = server {
        warn_target_divergence(components, server);
    }
    with_server(server, async |admin| {
        info!("Creating database");
        dbm::provision_all(components, admin).await?;
        dbm::upgrade_all(components).await?;
        Ok(())
    })
    .await?;
    // Baseline *content* is seeded on the serving pools (not the privileged
    // pool), so it runs after the schema structure is provisioned and upgraded.
    dbm::seed_all(components).await?;
    info!("Database created");
    Ok(())
}

/// Runs as each schema's owner (DDL on its own schema) — no privileged pool.
/// Seeds are idempotent full syncs, so re-running them here delivers the new
/// release's baseline content to an existing database.
async fn upgrade(components: &DatabaseComponents) -> Result<(), DbError> {
    info!("Upgrading database");
    dbm::upgrade_all(components).await?;
    dbm::seed_all(components).await?;
    info!("Database upgraded");
    Ok(())
}

/// The same correctness check the apiserver runs on its serving pools.
async fn check(components: &DatabaseComponents) -> Result<(), DbError> {
    dbm::check_all(components)
        .await
        .inspect(|()| info!("Database is up to date"))
}

/// Report each component's recorded version.
async fn status(components: &DatabaseComponents) -> Result<(), DbError> {
    for s in dbm::status(components).await? {
        info!(
            component = s.key,
            schema = %s.schema,
            db_version = ?s.db_version,
            current = s.current,
            "schema status"
        );
    }
    Ok(())
}

async fn drop_components(
    components: &DatabaseComponents,
    server: Option<&ComponentConfig>,
) -> Result<(), DbError> {
    with_server(server, async |admin| {
        info!("Dropping database");
        dbm::drop_all(components, admin).await?;
        info!("Database dropped");
        Ok(())
    })
    .await
}

/// Ping each component's serving pool — the way the apiserver reaches it.
async fn ping(components: &DatabaseComponents) -> Result<(), DbError> {
    dbm::ping_all(components)
        .await
        .inspect(|()| info!("Database is reachable"))
}

/// Run `op` against a freshly-opened privileged pool on the
/// `database_server:` entry, closing it afterward.
async fn with_server(
    server: Option<&ComponentConfig>,
    op: impl AsyncFnOnce(&dbm::AdminPool) -> Result<(), DbError>,
) -> Result<(), DbError> {
    let server = server.ok_or(DbError::MissingServerEntry)?;
    let pool = dbm::connect_server(server).await?;
    let result = op(&pool).await;
    pool.close().await;
    result
}

/// A component pointing at a different server than the entry is legitimate
/// (e.g. a pooler), but usually a typo'd port — flag it.
fn warn_target_divergence(components: &DatabaseComponents, server: &ComponentConfig) {
    let Some(server_pg) = server.conn.postgres() else {
        return;
    };
    for (key, entry) in components {
        let Some(pg) = entry.conn.postgres() else {
            continue;
        };
        if pg.host != server_pg.host || pg.port != server_pg.port {
            warn!(
                "component '{key}' targets {}:{}, but `database_server:` is {}:{}; \
                 provisioning runs on the latter",
                pg.host, pg.port, server_pg.host, server_pg.port
            );
        }
    }
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
            let Some(command) = params.command else {
                error!("No command given; see --help for the command groups");
                return ExitStatus::GeneralError;
            };
            let components = &config.database;
            let server = config.database_server.as_ref();
            // Each group validates only the section it reads.
            if matches!(command, Command::Db(_))
                && let Err(e) = dbm::validate_components(components)
            {
                error!("Invalid database configuration: {e:#?}");
                return ExitStatus::GeneralError;
            }

            let outcome: Result<(), DbError> = match command {
                Command::Server(command) => match command {
                    ServerCommand::Start => start_server(server).await,
                    ServerCommand::Serve => serve(server).await,
                    ServerCommand::Stop => stop_server(server).await,
                },
                Command::Db(command) => match command {
                    DbCommand::Create => create(components, server).await,
                    DbCommand::Upgrade => upgrade(components).await,
                    DbCommand::Check => check(components).await,
                    DbCommand::Status => status(components).await,
                    DbCommand::Drop { force: false } => {
                        error!("Refusing to drop schemas without --force");
                        return ExitStatus::GeneralError;
                    }
                    DbCommand::Drop { force: true } => drop_components(components, server).await,
                    DbCommand::Ping => ping(components).await,
                },
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
