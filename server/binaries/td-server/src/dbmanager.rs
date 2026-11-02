//
// Copyright 2026 Tabsdata Inc.
//

//! Standalone database-administration CLI, two command groups that never mix
//! configs: `server start|serve|stop` reads only the `database_server:` entry,
//! `db create|upgrade|check|status|drop|ping|extensions` reads the
//! `database:` map (`create`/`upgrade`/`drop`/`extensions` also need the
//! server entry for the privileged pool).
//!
//! Import rule: the components come from the edition (`te_edition`), and the
//! operations over them go through `te_database` (the edition facade, aliased
//! `dbm`); server lifecycle and config types come from `td_database` directly
//! (edition-agnostic).

#![allow(linker_messages)]

use std::env;
use std::process;
use std::time::Instant;

use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use ta_edition::Edition;
use td_common::attach::attach;
use td_common::crypto::install_crypto_provider;
use td_common::status::ExitStatus;
use td_common::{about, logging};
use td_database::config::{ComponentConfig, DatabaseComponents};
use td_database::error::DbError;
use td_database::server::{bootable_server, serve_server, start_server, stop_server};
use td_process::launcher::cli::Cli;
use td_process::launcher::hooks;
use td_process::pathenv::extend_path;
use te_database as dbm;
use te_database::{Registry, UpgradeAction};
use tracing::{debug, error, info};

const VERSION: &str = env!("CARGO_PKG_VERSION");

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

#[derive(Debug, Clone, Subcommand)]
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
    /// List, add or remove the optional extensions Tabsdata uses when the
    /// database server offers them.
    #[command(subcommand)]
    Extensions(ExtensionsCommand),
}

#[derive(Debug, Clone, Subcommand)]
enum ExtensionsCommand {
    /// Show each optional extension: its state, versions and the Tabsdata
    /// objects that depend on it.
    List,
    /// Create the extension the server offers (if the database lacks it) and
    /// build the Tabsdata objects that depend on it.
    Add {
        /// The extension's name.
        name: String,
    },
    /// Drop the Tabsdata objects that depend on the extension, and the
    /// extension itself when Tabsdata created it.
    Remove {
        /// The extension's name.
        name: String,
    },
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
    registry: &Registry,
    components: &DatabaseComponents,
    server: Option<&ComponentConfig>,
) -> Result<(), DbError> {
    let started = Instant::now();
    let upgraded = with_server(server, async |admin| {
        info!(schemas = ?resolved_schemas(registry, components)?, "Creating database");
        let before = dbm::provision_all(registry, components, admin).await?;
        let upgraded = dbm::upgrade_all(registry, components).await?;
        report_objects(
            &before,
            &dbm::inspect_all(registry, components, admin).await?,
        );
        Ok(upgraded)
    })
    .await?;
    // Baseline *content* is seeded on the serving pools (not the privileged
    // pool), so it runs after the schema structure is provisioned and upgraded.
    dbm::seed_all(registry, components).await?;
    report_versions(&upgraded);
    info!(
        schemas = ?resolved_schemas(registry, components)?,
        elapsed_ms = started.elapsed().as_millis(),
        "Database created"
    );
    Ok(())
}

fn resolved_schemas(
    registry: &Registry,
    components: &DatabaseComponents,
) -> Result<Vec<String>, DbError> {
    registry
        .components()
        .map(|component| dbm::resolved_schema(components, component))
        .collect()
}

/// The DDL runs as each schema's owner (on its own schema). The privileged
/// pool verifies the target objects exist, ensures the cluster-global roles a
/// newer schema version may depend on (the owner cannot `CREATE ROLE`), and
/// reports the resulting state. Seeds are idempotent full syncs, so re-running
/// them here delivers the new release's baseline content to an existing
/// database.
async fn upgrade(
    registry: &Registry,
    components: &DatabaseComponents,
    server: Option<&ComponentConfig>,
) -> Result<(), DbError> {
    let started = Instant::now();
    let before = with_server(server, async |admin| {
        let found = dbm::inspect_all(registry, components, admin).await?;
        dbm::require_present(&found)?;
        dbm::ensure_roles_all(registry, components, admin).await?;
        Ok(found)
    })
    .await?;
    info!(schemas = ?resolved_schemas(registry, components)?, "Upgrading database");
    let upgraded = dbm::upgrade_all(registry, components).await?;
    dbm::seed_all(registry, components).await?;
    let after = with_server(server, async |admin| {
        dbm::inspect_all(registry, components, admin).await
    })
    .await?;
    report_objects(&before, &after);
    report_versions(&upgraded);
    info!(
        elapsed_ms = started.elapsed().as_millis(),
        "Database upgraded"
    );
    Ok(())
}

/// `db extensions …`: act on the optional extensions, then report every one
/// of them, human-readable in the log and as one JSON line on stdout.
async fn extensions(
    registry: &Registry,
    components: &DatabaseComponents,
    server: Option<&ComponentConfig>,
    command: ExtensionsCommand,
) -> Result<(), DbError> {
    with_server(server, async |admin| {
        let removed = match &command {
            ExtensionsCommand::List => None,
            ExtensionsCommand::Add { name } => {
                let (component, extension) = dbm::optional_extension(registry, name)?;
                let report = with_extension_pools(components, admin, component, async |pools| {
                    pools.add(extension).await
                })
                .await?;
                info!(
                    extension = extension.name,
                    version = report.installed_version.as_deref().unwrap_or_default(),
                    objects = report.dependents_summary(),
                    "Optional extension added"
                );
                None
            }
            ExtensionsCommand::Remove { name } => {
                let (component, extension) = dbm::optional_extension(registry, name)?;
                let outcome = with_extension_pools(components, admin, component, async |pools| {
                    pools.remove(extension).await
                })
                .await?;
                info!(
                    extension = extension.name,
                    dropped_objects = outcome.dropped_objects.len(),
                    dropped_extension = outcome.dropped_extension,
                    "Optional extension removed"
                );
                Some(outcome)
            }
        };
        let mut reports = Vec::new();
        for component in registry.components().filter(|c| !c.extensions().is_empty()) {
            reports.extend(
                with_extension_pools(components, admin, component, async |pools| {
                    pools.list(component.extensions()).await
                })
                .await?,
            );
        }
        report_extensions(&reports);
        let document = dbm::ExtensionsDocument {
            extensions: reports,
            removed,
        };
        println!(
            "{}{}",
            dbm::EXTENSIONS_REPORT_PREFIX,
            serde_json::json!(document)
        );
        Ok(())
    })
    .await
}

fn report_extensions(reports: &[dbm::ExtensionReport]) {
    for report in reports {
        info!(
            extension = report.name,
            purpose = report.purpose,
            state = report.state.as_str(),
            installed = report.installed_version.as_deref().unwrap_or("-"),
            offered = report.offered_version.as_deref().unwrap_or("-"),
            update_available = report.update_available,
            created_by_tabsdata = report.created_by_tabsdata,
            objects = report.dependents_summary(),
            "Optional extension"
        );
        if let Some(sql) = &report.dba_sql {
            info!(
                extension = report.name,
                "A database administrator runs: {sql};"
            );
        }
    }
}

/// The version transition each component took, once the run is done.
fn report_versions(upgraded: &[dbm::ComponentUpgrade]) {
    for component in upgraded {
        match &component.action {
            Some(UpgradeAction::Install { version }) => info!(
                component = component.key,
                schema = %component.schema,
                version,
                "schema installed"
            ),
            Some(UpgradeAction::Ladder { from, to, .. }) => info!(
                component = component.key,
                schema = %component.schema,
                from,
                to,
                "schema upgraded"
            ),
            Some(UpgradeAction::UpToDate { version }) => info!(
                component = component.key,
                schema = %component.schema,
                version,
                "schema already current"
            ),
            // A newer database never commits; a silent component is warned about at the source.
            Some(UpgradeAction::Newer { .. }) | None => {}
        }
    }
}

fn report_objects(before: &[dbm::ComponentObjects], after: &[dbm::ComponentObjects]) {
    for (was, is) in before.iter().zip(after.iter()) {
        info!(
            component = was.key,
            schema = %is.schema,
            state = state_of(was.schema_present, is.schema_present),
            "schema"
        );
        info!(
            component = was.key,
            owner = %is.owner,
            state = state_of(was.owner_present, is.owner_present),
            "owner role"
        );
        info!(
            component = was.key,
            in_project = %is.roles.in_project,
            across_projects = %is.roles.across_projects,
            state = state_of(was.roles_present, is.roles_present),
            "RLS roles"
        );
    }
}

fn state_of(was_present: bool, is_present: bool) -> &'static str {
    match (was_present, is_present) {
        (true, true) => "pre-existent",
        (false, true) => "created",
        (true, false) => "dropped",
        (false, false) => "missing",
    }
}

/// The same correctness check the apiserver runs on its serving pools.
async fn check(registry: &Registry, components: &DatabaseComponents) -> Result<(), DbError> {
    dbm::check_all(registry, components)
        .await
        .inspect(|()| info!("Database is up to date"))
}

/// Report each component's recorded version.
async fn status(registry: &Registry, components: &DatabaseComponents) -> Result<(), DbError> {
    for s in dbm::status(registry, components).await? {
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
    registry: &Registry,
    components: &DatabaseComponents,
    server: Option<&ComponentConfig>,
) -> Result<(), DbError> {
    with_server(server, async |admin| {
        info!("Dropping database");
        dbm::drop_all(registry, components, admin).await?;
        info!(schemas = ?resolved_schemas(registry, components)?, "Database dropped");
        Ok(())
    })
    .await
}

/// Ping each component's serving pool — the way the apiserver reaches it.
async fn ping(registry: &Registry, components: &DatabaseComponents) -> Result<(), DbError> {
    dbm::ping_all(registry, components)
        .await
        .inspect(|()| info!("Database is reachable"))
}

/// Run `op` against a freshly-opened privileged pool on the
/// `database_server:` entry, closing it afterward.
async fn with_server<T>(
    server: Option<&ComponentConfig>,
    op: impl AsyncFnOnce(&dbm::AdminPool) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let server = server.ok_or(DbError::MissingServerEntry)?;
    let pool = dbm::connect_server(server).await?;
    let result = op(&pool).await;
    pool.close().await;
    result
}

/// `op` on the privileged `admin` pool and a fresh owner pool of
/// `component`'s schema.
async fn with_extension_pools<T>(
    components: &DatabaseComponents,
    admin: &dbm::AdminPool,
    component: &dyn dbm::SchemaComponent,
    op: impl AsyncFnOnce(&dbm::ExtensionPools<'_>) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let owner = dbm::extension_owner_pool(components, component).await?;
    let pools = dbm::ExtensionPools {
        server: admin,
        owner: &owner,
    };
    let result = op(&pools).await;
    owner.close().await;
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

fn run(edition: Edition) {
    let registry = edition.database();
    Cli::<DbManagerConfig, DbManagerParams>::exec_async(
        td_apiserver::CONFIG_NAME,
        |config, params| async move {
            let Some(command) = params.command else {
                error!("No command given; see --help for the command groups");
                return ExitStatus::GeneralError;
            };
            let mut resolved = config.database.clone();
            if let Some(server) = config.database_server.as_ref() {
                dbm::propagate_server_target(&mut resolved, server);
            }
            let components = &resolved;
            let registry = &registry;
            let server = config.database_server.as_ref();
            // Each group validates only the section it reads.
            if matches!(command, Command::Db(_))
                && let Err(e) = dbm::validate_components(registry, components)
            {
                error!("Invalid database configuration: {e}");
                debug!("{e:#?}");
                return ExitStatus::GeneralError;
            }

            let outcome: Result<(), DbError> = match command {
                Command::Server(command) => match command {
                    ServerCommand::Start => start_server(server).await,
                    ServerCommand::Serve => serve(server).await,
                    ServerCommand::Stop => stop_server(server).await,
                },
                Command::Db(command) => match command {
                    DbCommand::Create => create(registry, components, server).await,
                    DbCommand::Upgrade => upgrade(registry, components, server).await,
                    DbCommand::Check => check(registry, components).await,
                    DbCommand::Status => status(registry, components).await,
                    DbCommand::Drop { force: false } => {
                        error!("Refusing to drop schemas without --force");
                        return ExitStatus::GeneralError;
                    }
                    DbCommand::Drop { force: true } => {
                        drop_components(registry, components, server).await
                    }
                    DbCommand::Ping => ping(registry, components).await,
                    DbCommand::Extensions(command) => {
                        extensions(registry, components, server, command).await
                    }
                },
            };

            match outcome {
                Ok(()) => ExitStatus::Success,
                Err(e) => {
                    error!("db-admin command failed: {e}");
                    debug!("{e:#?}");
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
    extend_path();
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    logging::start();
    install_crypto_provider();
    run(te_edition::edition());
}
