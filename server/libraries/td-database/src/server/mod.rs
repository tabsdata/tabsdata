//
// Copyright 2026 Tabsdata Inc.
//

//! The managed database server behind the `database_server:` entry. Nothing
//! here runs at connect time — the server is brought up out of band
//! (`td_dbmanager start`/`serve`) and addressed by URL.

mod credentials;
mod entry;

#[cfg(feature = "embedded-postgres")]
mod boot;
#[cfg(feature = "embedded-postgres")]
mod knobs;
#[cfg(feature = "embedded-postgres")]
mod secrets;

pub use entry::{bootable_server, is_local, server_connectable, validate_server};

pub(crate) use credentials::{EmbeddedPassword, resolve_password};

#[cfg(feature = "embedded-postgres")]
pub use knobs::{BootConfig, boot_config, database_dir};

#[cfg(feature = "embedded-postgres")]
pub use boot::ServerHandle;
#[cfg(all(feature = "embedded-postgres", any(test, feature = "test-utils")))]
pub(crate) use boot::boot_detached;
#[cfg(feature = "embedded-postgres")]
pub(crate) use secrets::{load_or_create_owner_password, load_owner_password, load_password};

use crate::config::ComponentConfig;
use crate::error::DbError;

/// No-op owning handle when embedded postgres is not built in: there is no
/// in-process server to own, so `stop` does nothing.
#[cfg(not(feature = "embedded-postgres"))]
pub struct ServerHandle;

#[cfg(not(feature = "embedded-postgres"))]
impl ServerHandle {
    pub async fn stop(self) -> Result<(), DbError> {
        Ok(())
    }
}

/// Start the local database server detached, on the entry's URL port, so it
/// outlives this call. Idempotent; a no-op without a local entry.
pub async fn start_server(server: Option<&ComponentConfig>) -> Result<(), DbError> {
    let Some(server) = local_server(server, "start")? else {
        return Ok(());
    };
    #[cfg(feature = "embedded-postgres")]
    {
        boot::start_detached(&boot_config(server)?).await
    }
    #[cfg(not(feature = "embedded-postgres"))]
    {
        let _ = server;
        tracing::info!("embedded postgres not built in; nothing to start");
        Ok(())
    }
}

/// Boot the local database server and return an owning handle, so the calling
/// process owns the postmaster for its lifetime and stops it on shutdown. For
/// the foreground `td_dbmanager serve` runtime, whose process IS the database
/// service: the server must die with it, not outlive it as a reusable daemon.
/// Returns `None` (logged) when there is no local server to run.
pub async fn serve_server(
    server: Option<&ComponentConfig>,
) -> Result<Option<ServerHandle>, DbError> {
    let Some(server) = local_server(server, "serve")? else {
        return Ok(None);
    };
    #[cfg(feature = "embedded-postgres")]
    {
        Ok(Some(boot::boot_owned(&boot_config(server)?).await?))
    }
    #[cfg(not(feature = "embedded-postgres"))]
    {
        let _ = server;
        tracing::info!("embedded postgres not built in; nothing to serve");
        Ok(Some(ServerHandle))
    }
}

/// Stop the local database server. Idempotent; same no-op rules as
/// [`start_server`].
pub async fn stop_server(server: Option<&ComponentConfig>) -> Result<(), DbError> {
    let Some(server) = local_server(server, "stop")? else {
        return Ok(());
    };
    #[cfg(feature = "embedded-postgres")]
    {
        boot::stop_detached(&boot_config(server)?).await
    }
    #[cfg(not(feature = "embedded-postgres"))]
    {
        let _ = server;
        tracing::info!("embedded postgres not built in; nothing to stop");
        Ok(())
    }
}

/// The server entry when the boot may act on it, `None` (logged) when there is
/// nothing local to run; shape errors still propagate.
fn local_server<'a>(
    server: Option<&'a ComponentConfig>,
    action: &str,
) -> Result<Option<&'a ComponentConfig>, DbError> {
    match bootable_server(server) {
        Ok(server) => Ok(Some(server)),
        Err(DbError::MissingServerEntry) => {
            tracing::info!("no `database_server:` configured; nothing to {action}");
            Ok(None)
        }
        Err(DbError::NonLocalServerUrl(_)) => {
            tracing::info!("`database_server:` is not local; nothing to {action}");
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
