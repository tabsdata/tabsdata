//
// Copyright 2026 Tabsdata Inc.
//

//! Reading and verifying the **recorded state** of a managed schema — its
//! `db_version`, Tabsdata major version, edition, and the PostgreSQL server floor
//! — from the `tabsdata_system` table.
//!
//! Plain reads against a pool already pinned to the schema, needing no privilege:
//! the serving [`TdPool`](crate::pool::TdPool) runs them as its runtime check, and
//! [`AdminPool`](crate::components::AdminPool) reuses them to decide what to upgrade.

use crate::components::sql::is_undefined_table_error;
use crate::dynamic::DynamicPool;
use crate::error::DbError;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::DatabaseKind;
use sea_query::{Expr, ExprTrait, Iden, Query};
use sqlx::Error;
use std::cmp::Ordering;
use td_schema::SchemaSource;
use td_schema::schema::backend::Backend;
use td_schema::{
    CURRENT_TABSDATA_VERSION, DB_EDITION_NAME, DB_VERSION_NAME, TABSDATA_VERSION_NAME,
    TabsdataSystem,
};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::debug;

/// Minimum supported PostgreSQL `server_version_num` (15.0). The schema build
/// emits `security_invoker` views (15+); older servers would mis-evaluate them
/// and silently bypass project isolation, so the floor is enforced explicitly.
const MIN_PG_VERSION_NUM: i32 = 150_000;

fn map_system_db_error(err: Error) -> DbError {
    match &err {
        Error::Database(database_err) => {
            if is_undefined_table_error(database_err.code().as_deref(), database_err.message()) {
                DbError::DatabaseSchemaDoesNotExist
            } else {
                DbError::SqlError(err)
            }
        }
        _ => DbError::SqlError(err),
    }
}

/// Read a single `value` from `tabsdata_system` keyed by `name`. Missing rows
/// return `Ok(None)`; only the read itself can fail.
async fn read_system_value(pool: &DynamicPool, name: &str) -> Result<Option<String>, DbError> {
    Query::select()
        .column(TabsdataSystem::Value)
        .from(TabsdataSystem::Table)
        .and_where(Expr::col(TabsdataSystem::Name).eq(name))
        .build_query_scalar()
        .fetch_optional(pool)
        .await
        .map_err(map_system_db_error)
}

/// The recorded `db_version`, or `None` when the schema is not yet installed —
/// either the `tabsdata_system` table is absent **or** its `db_version` row is.
/// The pool creates the (empty) ledger before build, so a missing row is the
/// fresh-install signal, not corruption; the pool writes the row after build/delta.
pub(crate) async fn db_version(pool: &DynamicPool) -> Result<Option<usize>, DbError> {
    let raw = match read_system_value(pool, DB_VERSION_NAME).await {
        Ok(value) => value,
        Err(DbError::DatabaseSchemaDoesNotExist) => return Ok(None),
        Err(e) => return Err(e),
    };
    match raw {
        Some(value) => value.parse::<usize>().map(Some).map_err(|_| {
            DbError::DatabaseCorrupted(format!(
                "'{DB_VERSION_NAME}' value '{value}' must be an integer"
            ))
        }),
        None => Ok(None),
    }
}

pub(crate) async fn check_db_version(
    pool: &DynamicPool,
    source: &dyn SchemaSource,
) -> Result<(), DbError> {
    let current = source.current_version();
    // An installed schema must carry the row; absence here is corruption — unlike
    // the lenient `db_version`, which reads absence as not-yet-installed.
    let version = db_version(pool).await?.ok_or_else(|| {
        DbError::DatabaseCorrupted(format!(
            "Missing '{DB_VERSION_NAME}' row in '{}'",
            TabsdataSystem::Table.to_string()
        ))
    })?;
    match version.cmp(&current) {
        Ordering::Equal => Ok(()),
        Ordering::Less => Err(DbError::DatabaseNeedsUpgrade(format!(
            "Tabsdata database version is '{version}', binary database version is '{current}'"
        ))),
        Ordering::Greater => Err(DbError::DatabaseIsNewer(version.to_string(), current)),
    }
}

/// Read the major Tabsdata version. Pre-2.0 databases lack this row; a missing
/// row maps to `1` so the startup gate can refuse them with a clear message.
async fn read_tabsdata_version(pool: &DynamicPool) -> Result<usize, DbError> {
    match read_system_value(pool, TABSDATA_VERSION_NAME).await? {
        None => Ok(1),
        Some(raw) => raw.parse::<usize>().map_err(|_| {
            DbError::DatabaseCorrupted(format!(
                "'{TABSDATA_VERSION_NAME}' value '{raw}' must be an integer"
            ))
        }),
    }
}

pub(crate) async fn check_tabsdata_version(pool: &DynamicPool) -> Result<(), DbError> {
    let version = read_tabsdata_version(pool).await?;
    if version == CURRENT_TABSDATA_VERSION {
        Ok(())
    } else {
        Err(DbError::IncompatibleMajorVersion(
            version,
            CURRENT_TABSDATA_VERSION,
        ))
    }
}

/// The recorded edition, or `None` when unset (used by both check and upgrade).
pub(crate) async fn edition(pool: &DynamicPool) -> Result<Option<String>, DbError> {
    read_system_value(pool, DB_EDITION_NAME).await
}

pub(crate) async fn check_tabsdata_edition(pool: &DynamicPool) -> Result<(), DbError> {
    let instance_edition = edition(pool).await?;
    let runtime_edition = TabsdataEdition;
    match instance_edition {
        None => Err(DbError::DatabaseNeedsUpgrade(format!(
            "instance edition is undefined, binary edition is '{}'",
            runtime_edition.label(),
        ))),
        Some(instance_edition) => {
            if !runtime_edition.is_compatible(&instance_edition) {
                Err(DbError::InvalidEdition(
                    instance_edition,
                    runtime_edition.label().to_string(),
                ))
            } else if runtime_edition.requires_upgrade(&instance_edition) {
                Err(DbError::DatabaseNeedsUpgrade(format!(
                    "instance edition is '{instance_edition}', binary edition is '{}'",
                    runtime_edition.label()
                )))
            } else {
                Ok(())
            }
        }
    }
}

/// Refuse to run on a PostgreSQL server older than [`MIN_PG_VERSION_NUM`]. A
/// plain read of `server_version_num`; relevant both at upgrade time (the build
/// needs 15+) and at runtime (the floor must still hold). PostgreSQL only.
pub(crate) async fn check_pg_min_version(pool: &DynamicPool) -> Result<(), DbError> {
    if !matches!(pool.kind().into(), Backend::Postgres) {
        return Ok(());
    }
    let version: Option<i32> = Query::select()
        .expr(Expr::cust("current_setting('server_version_num')::int"))
        .build_query_scalar()
        .fetch_optional(pool)
        .await
        .map_err(DbError::SqlError)?;
    let version = version.unwrap_or(0);
    if version < MIN_PG_VERSION_NUM {
        return Err(DbError::UnsupportedPostgresVersion(
            version,
            MIN_PG_VERSION_NUM,
        ));
    }
    debug!(
        server_version_num = version,
        "db.check: postgres version OK"
    );
    Ok(())
}
