//
// Copyright 2026 Tabsdata Inc.
//

//! The optional PostgreSQL extensions Tabsdata uses when the database has
//! them. Each schema component declares its own
//! ([`SchemaComponent::extensions`]); only those are ever touched, and only by
//! `td_dbmanager db extensions`: `db create` and `db upgrade` create neither an
//! extension nor the objects that depend on it. Tabsdata never installs an
//! extension's files; `add` runs `CREATE EXTENSION … SCHEMA` into the declaring
//! component's schema for one the server offers, on the privileged
//! `database_server:` entry, and builds the objects of that schema that depend
//! on it as the schema owner.
//!
//! An extension Tabsdata created is marked in the declaring component's
//! `tabsdata_system` table; `remove` drops only such an extension, and never
//! with `CASCADE`, so an extension a DBA created, or one another application
//! depends on, stays.

use crate::components::admin_pool::AdminPool;
use crate::components::{Registry, SchemaComponent};
use crate::connection::PgConnectionExt;
use crate::error::DbError;
use crate::generated_sql::generated;
use crate::sea_query::builder::SeaQueryExt;
use sea_query::extension::postgres::Extension;
use sea_query::{
    Alias, Expr, ExprTrait, Func, Iden, Index, JoinType, OnConflict, PostgresQueryBuilder, Query,
    SelectStatement,
};
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use td_schema::TabsdataSystem;
use tracing::{debug, info};

const MARKER_PREFIX: &str = "created_extension:";
const INSUFFICIENT_PRIVILEGE: &str = "42501";
const DEPENDENT_OBJECTS: &str = "2BP01";

/// An optional extension a component uses: the objects of its schema that
/// depend on it (`dependents`, by name) and the statements that build them
/// (`build`, given the schema the extension's objects are in, which may be
/// another one when a DBA created it).
#[derive(Debug, Clone, Copy)]
pub struct OptionalExtension {
    pub name: &'static str,
    pub purpose: &'static str,
    pub dependents: fn() -> Vec<String>,
    pub build: fn(&str) -> Vec<String>,
}

impl PartialEq for OptionalExtension {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for OptionalExtension {}

/// Every extension the registered components use, with its component.
pub fn optional_extensions(
    registry: &Registry,
) -> impl Iterator<Item = (&'static dyn SchemaComponent, &'static OptionalExtension)> + '_ {
    registry.components().flat_map(|component| {
        component
            .extensions()
            .iter()
            .map(move |extension| (component, extension))
    })
}

/// The allowlisted extension named `name`, with the component that uses it.
pub fn optional_extension(
    registry: &Registry,
    name: &str,
) -> Result<(&'static dyn SchemaComponent, &'static OptionalExtension), DbError> {
    optional_extensions(registry)
        .find(|(_, extension)| extension.name == name)
        .ok_or_else(|| {
            DbError::ExtensionNotAllowed(
                name.to_owned(),
                optional_extensions(registry)
                    .map(|(_, extension)| extension.name)
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        })
}

impl OptionalExtension {
    /// What a DBA runs when Tabsdata's role may not create it in `schema`.
    pub fn dba_sql(&self, schema: &str) -> String {
        Extension::create()
            .name(self.name)
            .if_not_exists()
            .schema(format!("\"{}\"", schema.replace('"', "\"\"")))
            .to_string(PostgresQueryBuilder)
    }

    fn marker(&self) -> String {
        format!("{MARKER_PREFIX}{}", self.name)
    }
}

/// Whether the index `name` of `schema` is valid, as one row: `NULL` when
/// there is no such index.
fn index_validity_query(schema: &str, name: &str) -> SelectStatement {
    Query::select()
        .expr(Expr::cust_with_values(
            "(SELECT i.indisvalid FROM pg_index i \
             JOIN pg_class c ON c.oid = i.indexrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2)",
            [schema.to_owned(), name.to_owned()],
        ))
        .to_owned()
}

/// Serializes the builds of one schema's extension objects.
const EXTENSION_OBJECTS_LOCK: &str = "extension_objects";

#[derive(Iden, Clone, Copy)]
enum PgCatalog {
    PgExtension,
    Extname,
    Extnamespace,
    PgNamespace,
    Oid,
    Nspname,
}

/// The schema `extension` is installed in; no row without it.
fn extension_namespace_query(extension: &str) -> SelectStatement {
    Query::select()
        .expr(Expr::col((PgCatalog::PgNamespace, PgCatalog::Nspname)).cast_as(Alias::new("text")))
        .from(PgCatalog::PgExtension)
        .join(
            JoinType::InnerJoin,
            PgCatalog::PgNamespace,
            Expr::col((PgCatalog::PgNamespace, PgCatalog::Oid))
                .equals((PgCatalog::PgExtension, PgCatalog::Extnamespace)),
        )
        .and_where(Expr::col((PgCatalog::PgExtension, PgCatalog::Extname)).eq(extension))
        .to_owned()
}

/// Where an optional extension stands on the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionState {
    /// Installed in the database.
    Enabled,
    /// Offered by the server, not installed.
    Available,
    /// Offered, but the connected role may not create it.
    Refused,
    /// The server does not have the extension's files.
    NotAvailable,
}

impl ExtensionState {
    pub fn as_str(self) -> &'static str {
        match self {
            ExtensionState::Enabled => "enabled",
            ExtensionState::Available => "available",
            ExtensionState::Refused => "refused",
            ExtensionState::NotAvailable => "not available",
        }
    }
}

/// One object of Tabsdata's schema that depends on an extension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependentObject {
    pub name: String,
    pub present: bool,
}

/// An optional extension as the database has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionReport {
    pub name: String,
    pub purpose: String,
    pub state: ExtensionState,
    pub installed_version: Option<String>,
    pub offered_version: Option<String>,
    /// Installed at an older version than the server offers.
    pub update_available: bool,
    pub created_by_tabsdata: bool,
    pub dependents: Vec<DependentObject>,
    /// What a DBA runs when the state is [`ExtensionState::Refused`].
    pub dba_sql: Option<String>,
}

impl ExtensionReport {
    /// `"2 of 3"` of its dependent objects present.
    pub fn dependents_summary(&self) -> String {
        let present = self
            .dependents
            .iter()
            .filter(|object| object.present)
            .count();
        format!("{present} of {}", self.dependents.len())
    }
}

/// What `remove` did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveOutcome {
    pub dropped_objects: Vec<String>,
    /// The extension itself was dropped: Tabsdata had created it.
    pub dropped_extension: bool,
}

/// The prefix of the stdout line on which `td_dbmanager db extensions` writes
/// its [`ExtensionsDocument`] as JSON, for its caller to read from the output.
pub const EXTENSIONS_REPORT_PREFIX: &str = "TD_DB_EXTENSIONS ";

/// The extensions after a `db extensions` step, and what a removal did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionsDocument {
    pub extensions: Vec<ExtensionReport>,
    pub removed: Option<RemoveOutcome>,
}

/// The two connections the extension steps need: the privileged server entry,
/// which creates and drops extensions, and the owner of the schema of the
/// component using them, which owns the dependent objects and the marker.
pub struct ExtensionPools<'a> {
    pub server: &'a AdminPool,
    pub owner: &'a AdminPool,
}

impl ExtensionPools<'_> {
    pub async fn inspect(&self, extension: &OptionalExtension) -> Result<ExtensionReport, DbError> {
        let installed_version = self.server.installed_version(extension).await?;
        let offered_version = self.server.offered_version(extension).await?;
        let state = match (&installed_version, &offered_version) {
            (Some(_), _) => ExtensionState::Enabled,
            (None, None) => ExtensionState::NotAvailable,
            (None, Some(_)) if self.creatable(extension).await? => ExtensionState::Available,
            (None, Some(_)) => ExtensionState::Refused,
        };
        let update_available = matches!(
            (&installed_version, &offered_version),
            (Some(installed), Some(offered)) if installed != offered
        );
        Ok(ExtensionReport {
            name: extension.name.to_owned(),
            purpose: extension.purpose.to_owned(),
            state,
            installed_version,
            offered_version,
            update_available,
            created_by_tabsdata: self.created_by_tabsdata(extension).await?,
            dependents: self.present_objects(extension).await?,
            dba_sql: (state == ExtensionState::Refused)
                .then(|| extension.dba_sql(self.owner.schema())),
        })
    }

    pub async fn list(
        &self,
        extensions: &[OptionalExtension],
    ) -> Result<Vec<ExtensionReport>, DbError> {
        let mut reports = Vec::with_capacity(extensions.len());
        for extension in extensions {
            reports.push(self.inspect(extension).await?);
        }
        Ok(reports)
    }

    /// Install `extension` if the database lacks it, marking it as created by
    /// Tabsdata, then build its dependent objects. On an extension already
    /// installed (by Tabsdata or a DBA) it only builds what is missing.
    pub async fn add(&self, extension: &OptionalExtension) -> Result<ExtensionReport, DbError> {
        if self.server.installed_version(extension).await?.is_none() {
            let Some(offered) = self.server.offered_version(extension).await? else {
                return Err(DbError::ExtensionNotAvailable(extension.name.to_owned()));
            };
            self.server
                .create_extension(extension, self.owner.schema())
                .await?;
            self.mark_created(extension, &offered).await?;
            info!(
                extension = extension.name,
                version = offered,
                "Optional extension created"
            );
        }
        self.reconcile(extension).await?;
        self.inspect(extension).await
    }

    /// Drop Tabsdata's objects that depend on `extension`, then the extension
    /// itself only when Tabsdata created it. Never `CASCADE`: an extension
    /// something else still depends on is refused, naming what.
    pub async fn remove(&self, extension: &OptionalExtension) -> Result<RemoveOutcome, DbError> {
        let mut dropped_objects = Vec::new();
        for (name, validity) in self.index_states(extension).await? {
            if validity.is_some() {
                self.owner.drop_index(&name).await?;
                dropped_objects.push(name);
            }
        }
        let installed = self.server.installed_version(extension).await?.is_some();
        let dropped_extension = installed && self.created_by_tabsdata(extension).await?;
        if dropped_extension {
            self.server.drop_extension(extension).await?;
            self.unmark(extension).await?;
            info!(extension = extension.name, "Optional extension dropped");
        } else if !installed {
            self.unmark(extension).await?;
        }
        Ok(RemoveOutcome {
            dropped_objects,
            dropped_extension,
        })
    }

    /// Build the objects of Tabsdata's schema that depend on `extension`, when
    /// it is installed. Idempotent.
    pub async fn reconcile(&self, extension: &OptionalExtension) -> Result<(), DbError> {
        if self.server.installed_version(extension).await?.is_none() {
            return Ok(());
        }
        self.owner.ensure_extension_objects(extension).await
    }

    async fn mark_created(
        &self,
        extension: &OptionalExtension,
        version: &str,
    ) -> Result<(), DbError> {
        Query::insert()
            .into_table(TabsdataSystem::Table)
            .columns([TabsdataSystem::Name, TabsdataSystem::Value])
            .values_panic([extension.marker().into(), version.into()])
            .on_conflict(
                OnConflict::column(TabsdataSystem::Name)
                    .update_column(TabsdataSystem::Value)
                    .to_owned(),
            )
            .build_query()
            .execute(&self.owner.pool)
            .await
            .map_err(DbError::SqlError)?;
        Ok(())
    }

    async fn unmark(&self, extension: &OptionalExtension) -> Result<(), DbError> {
        Query::delete()
            .from_table(TabsdataSystem::Table)
            .and_where(Expr::col(TabsdataSystem::Name).eq(extension.marker()))
            .build_query()
            .execute(&self.owner.pool)
            .await
            .map_err(DbError::SqlError)?;
        Ok(())
    }

    async fn created_by_tabsdata(&self, extension: &OptionalExtension) -> Result<bool, DbError> {
        let ledger: Option<bool> = Query::select()
            .expr(Expr::cust_with_values(
                "to_regclass(quote_ident($1) || '.tabsdata_system') IS NOT NULL",
                [self.owner.schema().to_string()],
            ))
            .build_query_scalar()
            .fetch_optional(&self.owner.pool)
            .await
            .map_err(DbError::SqlError)?;
        if ledger != Some(true) {
            return Ok(false);
        }
        let marked: Option<String> = Query::select()
            .column(TabsdataSystem::Value)
            .from(TabsdataSystem::Table)
            .and_where(Expr::col(TabsdataSystem::Name).eq(extension.marker()))
            .build_query_scalar()
            .fetch_optional(&self.owner.pool)
            .await
            .map_err(DbError::SqlError)?;
        Ok(marked.is_some())
    }

    /// Each dependent object, present only once it is usable: a failed
    /// concurrent build leaves an invalid index behind.
    async fn present_objects(
        &self,
        extension: &OptionalExtension,
    ) -> Result<Vec<DependentObject>, DbError> {
        Ok(self
            .index_states(extension)
            .await?
            .into_iter()
            .map(|(name, validity)| DependentObject {
                name,
                present: validity == Some(true),
            })
            .collect())
    }

    /// Each dependent index with whether it is valid; `None` when absent.
    async fn index_states(
        &self,
        extension: &OptionalExtension,
    ) -> Result<Vec<(String, Option<bool>)>, DbError> {
        let schema = self.owner.schema().to_string();
        let mut states = Vec::new();
        for name in (extension.dependents)() {
            let validity: Option<Option<bool>> = index_validity_query(&schema, &name)
                .build_query_scalar()
                .fetch_optional(&self.server.pool)
                .await
                .map_err(DbError::SqlError)?;
            states.push((name, validity.flatten()));
        }
        Ok(states)
    }

    /// Whether the server entry's role may create `extension`: a superuser,
    /// any role with `CREATE` on the database for a trusted extension, or a
    /// member of a managed service's extension-admin role.
    async fn creatable(&self, extension: &OptionalExtension) -> Result<bool, DbError> {
        let creatable: Option<bool> = Query::select()
            .expr(Expr::cust_with_values(
                "(SELECT rolsuper FROM pg_roles WHERE rolname = current_user) \
                 OR (EXISTS (SELECT FROM pg_available_extension_versions \
                     WHERE name = $1 AND trusted) \
                     AND has_database_privilege(current_database(), 'CREATE')) \
                 OR EXISTS (SELECT FROM pg_roles WHERE rolname IN \
                     ('rds_superuser', 'cloudsqlsuperuser', 'azure_pg_admin', 'alloydbsuperuser') \
                     AND pg_has_role(current_user, oid, 'MEMBER'))",
                [extension.name],
            ))
            .build_query_scalar()
            .fetch_optional(&self.server.pool)
            .await
            .map_err(DbError::SqlError)?;
        Ok(creatable.unwrap_or(false))
    }
}

impl AdminPool {
    /// Build the objects of this schema that depend on `extension` when it is
    /// installed. Idempotent.
    ///
    /// Each is a `CREATE INDEX CONCURRENTLY`, run outside any transaction so
    /// writes go on while it builds, on one session holding a lock that makes
    /// concurrent builders take turns. A failed concurrent build leaves an
    /// invalid index that `IF NOT EXISTS` would keep, so it is dropped first.
    async fn ensure_extension_objects(&self, extension: &OptionalExtension) -> Result<(), DbError> {
        let schema = self.schema().to_string();
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?
            .detach();
        Query::select()
            .expr(
                Func::cust("pg_advisory_lock").arg(Func::cust("hashtextextended").args([
                    Expr::val(format!("{schema}:{EXTENSION_OBJECTS_LOCK}")),
                    Expr::val(0),
                ])),
            )
            .build_query()
            .execute(&mut conn)
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        let namespace: Option<String> = extension_namespace_query(extension.name)
            .build_query_scalar()
            .fetch_optional(&mut conn)
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        if let Some(namespace) = namespace {
            for name in (extension.dependents)() {
                let validity: Option<Option<bool>> = index_validity_query(&schema, &name)
                    .build_query_scalar()
                    .fetch_optional(&mut conn)
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                if validity.flatten() == Some(false) {
                    let drop = Index::drop()
                        .name(name.as_str())
                        .if_exists()
                        .concurrently()
                        .to_string(PostgresQueryBuilder);
                    conn.execute_batch(&[drop])
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                    info!(index = name, "Invalid optional extension index dropped");
                }
            }
            conn.execute_batch(&(extension.build)(&namespace))
                .await
                .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
            info!(
                extension = extension.name,
                namespace, "Optional extension objects ensured"
            );
        } else {
            debug!(
                extension = extension.name,
                "Optional extension not installed; its objects are not built"
            );
        }
        conn.close()
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)
    }

    async fn installed_version(
        &self,
        extension: &OptionalExtension,
    ) -> Result<Option<String>, DbError> {
        Query::select()
            .expr(Expr::col("extversion"))
            .from("pg_extension")
            .and_where(Expr::col("extname").eq(extension.name))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::SqlError)
    }

    async fn offered_version(
        &self,
        extension: &OptionalExtension,
    ) -> Result<Option<String>, DbError> {
        Query::select()
            .expr(Expr::col("default_version"))
            .from("pg_available_extensions")
            .and_where(Expr::col("name").eq(extension.name))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::SqlError)
    }

    /// `CREATE EXTENSION IF NOT EXISTS … SCHEMA`, reporting a role that may
    /// not create it with the SQL a DBA runs instead.
    async fn create_extension(
        &self,
        extension: &OptionalExtension,
        schema: &str,
    ) -> Result<(), DbError> {
        sqlx::raw_sql(generated(extension.dba_sql(schema)))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|error| match sql_state(&error).as_deref() {
                Some(INSUFFICIENT_PRIVILEGE) => DbError::ExtensionRefused(
                    extension.name.to_owned(),
                    extension.dba_sql(schema),
                    error.to_string(),
                ),
                _ => DbError::SqlError(error),
            })
    }

    /// `DROP EXTENSION IF EXISTS … RESTRICT`, reporting what still depends on
    /// it.
    async fn drop_extension(&self, extension: &OptionalExtension) -> Result<(), DbError> {
        let drop = Extension::drop()
            .name(extension.name)
            .if_exists()
            .restrict()
            .to_string(PostgresQueryBuilder);
        sqlx::raw_sql(generated(drop))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|error| match sql_state(&error).as_deref() {
                Some(DEPENDENT_OBJECTS) => DbError::ExtensionInUse(
                    extension.name.to_owned(),
                    detail(&error).unwrap_or_else(|| error.to_string()),
                ),
                _ => DbError::SqlError(error),
            })
    }

    async fn drop_index(&self, name: &str) -> Result<(), DbError> {
        let drop = Index::drop()
            .name(name)
            .if_exists()
            .to_string(PostgresQueryBuilder);
        sqlx::raw_sql(generated(drop))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(DbError::SqlError)
    }
}

fn sql_state(error: &sqlx::Error) -> Option<String> {
    match error {
        sqlx::Error::Database(db) => db.code().map(|code| code.to_string()),
        _ => None,
    }
}

fn detail(error: &sqlx::Error) -> Option<String> {
    match error {
        sqlx::Error::Database(db) => db
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            .and_then(|pg| pg.detail().map(str::to_owned)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRGM: OptionalExtension = OptionalExtension {
        name: "pg_trgm",
        purpose: "test stand-in",
        dependents: Vec::new,
        build: |_| Vec::new(),
    };

    #[test]
    fn only_the_extensions_a_component_declares_are_touched() {
        assert!(matches!(
            optional_extension(&Registry::new(&[], &[]), "plpython3u"),
            Err(DbError::ExtensionNotAllowed(name, _)) if name == "plpython3u"
        ));
    }

    #[test]
    fn a_dba_runs_a_plain_idempotent_create_into_the_schema() {
        assert_eq!(
            TRGM.dba_sql("td_apiserver"),
            "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA \"td_apiserver\""
        );
    }

    #[test]
    fn the_extension_s_schema_is_read_from_the_catalogs() {
        let sql = extension_namespace_query("pg_trgm").to_string(PostgresQueryBuilder);
        assert!(sql.contains("JOIN \"pg_namespace\""), "{sql}");
        assert!(
            sql.contains("\"pg_extension\".\"extname\" = 'pg_trgm'"),
            "{sql}"
        );
    }
}
