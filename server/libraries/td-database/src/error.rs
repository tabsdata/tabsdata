//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum DbError {
    #[error(
        "Unsupported database engine: '{0}'. Tabsdata supports PostgreSQL \
         (`postgres://…`), SQLite (`file:…`), and MySQL (`mysql://…`). Set the \
         database `url:` (or the embedded-postgres knobs) in the configuration \
         to one of these engines."
    )]
    UnsupportedDatabase(String) = 0,
    #[error(
        "Invalid pool configuration: `max_connections` must be greater than 0 \
         (got {0}). Set `max_connections` to a positive value in the database \
         pool configuration."
    )]
    InvalidMaxConnections(u32) = 1,
    #[error(
        "Invalid pool configuration: `max_connections` ({0}) must be greater \
         than or equal to `min_connections` ({1}). Raise `max_connections` or \
         lower `min_connections` in the database pool configuration."
    )]
    InvalidConnections(u32, u32) = 2,
    #[error(
        "The Tabsdata database schema has not been created. Create it by running \
         `tdserver create` (or start the server with database auto-create \
         enabled)."
    )]
    DatabaseSchemaDoesNotExist = 5000,
    #[error(
        "The Tabsdata database schema is out of date and must be upgraded: {0}. \
         Back up the database, then run `tdserver upgrade` (or `tdserver upgrade \
         --instance <instance>` for a non-default instance)."
    )]
    DatabaseNeedsUpgrade(String) = 5001,
    #[error(
        "The Tabsdata database belongs to major version '{0}' and cannot be \
         opened by major version '{1}'. Cross-major upgrade is not supported: \
         back up the existing database, create a fresh one with this binary \
         (`tdserver create`), and re-ingest your data."
    )]
    IncompatibleMajorVersion(usize, usize) = 5017,
    #[error(
        "The Tabsdata database is at version '{0}', but this binary only supports \
         version '{1}' — the database was created or upgraded by a newer Tabsdata \
         release. Upgrade this binary to a version at least as new as the \
         database, or point it at a database created by this binary."
    )]
    DatabaseIsNewer(String, usize) = 5002,
    #[error(
        "The Tabsdata database is corrupted: {0}. Restore from a known-good \
         backup; if none is available, create a fresh database with `tdserver \
         create` and re-ingest your data."
    )]
    DatabaseCorrupted(String) = 5003,
    #[error(
        "No database location in the configuration. Set the database `url:` \
         (e.g. `postgres://user:pass@host:5432/db`, `mysql://…`, or \
         `file:/path/to/app.db`), or configure the embedded-postgres knobs."
    )]
    MissingDatabaseLocation = 5004,
    #[error(
        "Failed to connect to the database: {0}. Verify the database server is \
         running and reachable, and that the host, port, credentials, and \
         database name in the URL are correct (and that any firewall or TLS \
         settings allow the connection)."
    )]
    FailedToConnectToDatabase(#[from] sqlx::Error) = 5007,
    #[error(
        "Failed to create or upgrade the database schema: {0}. The connecting \
         (upgrader) role may lack CREATE/ALTER on the application schema, or a \
         previous run left the schema partially migrated. Check the role's \
         privileges and the server logs, then retry `tdserver upgrade`."
    )]
    FailedToCreateOrUpgradeDatabaseSchema(#[source] sqlx::Error) = 5008,
    #[error(
        "Database query failed: {0}. This is unexpected — check the server logs \
         and the database server's health; if it persists, report it with the \
         surrounding log context."
    )]
    SqlError(#[source] sqlx::Error) = 5009,
    #[error(
        "Invalid database connection URL: {0}. It must be a well-formed \
         `postgres://`, `mysql://`, or `file:` URL. Fix the database `url:` in \
         the configuration."
    )]
    DatabaseInvalidUrl(String) = 5013,
    #[error(
        "This Tabsdata instance is edition '{0}', but the binary is edition \
         '{1}'; a '{1}' binary cannot run a '{0}' instance. Run the instance \
         with a binary built for the '{0}' edition."
    )]
    InvalidEdition(String, String) = 5014,
    #[error(
        "Failed to create or upgrade the database edition marker: {0}. Check the \
         upgrader role's privileges and the server logs, then retry the \
         create/upgrade."
    )]
    FailedToCreateOrUpgradeDatabaseEdition(#[source] sqlx::Error) = 5015,
    #[error(
        "Cannot change the Tabsdata edition: {0}. Run the binary that matches the \
         instance's current edition; in-place edition changes are not performed \
         automatically."
    )]
    CannotUpgradeEdition(String) = 5016,
    #[error(
        "Failed to start the embedded PostgreSQL server: {0}. Ensure the data \
         directory is writable and not already in use, the chosen port is free, \
         and the OS has enough System V shared memory and semaphores (on macOS \
         inspect `sysctl kern.sysv` and clear orphaned segments with `ipcrm`). \
         Check the server logs for the postmaster's startup output."
    )]
    EmbeddedPostgres(String) = 5018,
    #[error(
        "Cannot switch the connection to the row-level-security app role '{0}'. \
         The role must exist and the connecting role must be a member of it. Run: \
         CREATE ROLE \"{0}\" NOBYPASSRLS; GRANT \"{0}\" TO <connecting role>; \
         GRANT USAGE ON SCHEMA public TO \"{0}\"; \
         GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO \"{0}\". \
         (Underlying error: {1})"
    )]
    AppRoleNotSwitchable(String, #[source] sqlx::Error) = 5019,
    #[error(
        "PostgreSQL server version is too old: server_version_num={0}, but project \
         isolation requires {1} or newer. Views are created `WITH (security_invoker = true)` \
         (PostgreSQL 15+) so row-level security is evaluated as the querying role rather \
         than the view owner; without it, views over RLS tables would bypass project \
         isolation. Upgrade the PostgreSQL server to 15 or newer."
    )]
    UnsupportedPostgresVersion(i32, i32) = 5020,
    #[error(
        "The serving PostgreSQL role bypasses row-level security (it is a \
         SUPERUSER or has BYPASSRLS). Serving connections switch to the app role \
         in `after_connect`; that role must be NOBYPASSRLS. Project isolation \
         rests entirely on RLS, so a bypassing role would let every project read \
         and write every other project's rows — with no error. Refusing to start. \
         Recreate the app role NOBYPASSRLS."
    )]
    ConnectedRoleBypassesRls = 5021,
    #[error(
        "The database upgrader role lacks a privilege schema create/upgrade \
         needs (it must be able to create and alter objects in the application \
         schema). Connect the upgrader as a role that owns — or has CREATE on — \
         that schema. (Missing: {0})"
    )]
    UpgraderLacksPermissions(String) = 5022,
}
