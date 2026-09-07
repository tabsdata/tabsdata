//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum DbError {
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
        "Missing pool configuration: `pool.{0}` must be set explicitly (with \
         `min_connections` and `max_connections`) in the database entry — \
         pool blocks are never inherited or defaulted."
    )]
    MissingPoolConfig(&'static str) = 3,
    #[error(
        "Missing pool configuration: the `{0}` component declares the `{1}` \
         pool, so its entry must carry an explicit `pool.{1}` block (with \
         `min_connections` and `max_connections`) — pool blocks are never \
         inherited or defaulted."
    )]
    MissingComponentPoolConfig(&'static str, &'static str) = 12,
    #[error(
        "The `{0}` component declares the `{1}` pool but no such pool was \
         connected — the declared pools and the connected pools must agree."
    )]
    MissingComponentPool(&'static str, crate::pool::PoolKind) = 4,
    #[error(
        "Unexpected pool configuration: the `{0}` component has no `{1}` \
         pool; remove the `pool.{1}` block from its entry."
    )]
    UnexpectedPoolConfig(&'static str, &'static str) = 5,
    #[error(
        "The Tabsdata database schema has not been created. Create it with \
         `tdkserver create` on a managed instance, or `td_dbmanager db create` \
         directly against an external database."
    )]
    DatabaseSchemaDoesNotExist = 5000,
    #[error(
        "The Tabsdata database schema is out of date and must be upgraded: {0}. \
         Back up the database, then run `td_dbmanager db upgrade` against it."
    )]
    DatabaseNeedsUpgrade(String) = 5001,
    #[error(
        "The Tabsdata database belongs to major version '{0}' and cannot be \
         opened by major version '{1}'. Cross-major upgrade is not supported: \
         back up the existing database, create a fresh one with this release \
         (`tdkserver create` or `td_dbmanager db create`), and re-ingest your \
         data."
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
         backup; if none is available, create a fresh database (`tdkserver \
         create` or `td_dbmanager db create`) and re-ingest your data."
    )]
    DatabaseCorrupted(String) = 5003,
    #[error(
        "No schema resolved for the PostgreSQL connection. Component \
         connections derive it from the database name (`<database>_<component>`) \
         or take an explicit `schema:`; this connection was built with neither. \
         Set `database:` on the entry, or an explicit `schema:`."
    )]
    MissingSchema = 5023,
    #[error(
        "Missing database credentials. Set both `username` and `password` in the \
         entry's connect configuration."
    )]
    MissingCredentials = 5029,
    #[error(
        "No `database_server:` section in the configuration. This operation \
         needs the privileged server connection (its target and admin \
         credentials)."
    )]
    MissingServerEntry = 5025,
    #[error(
        "Cannot create the database: {0} already present. Create provisions a \
         fresh database; run 'upgrade' to bring an existing one up to date, or \
         'drop --force' to remove it first."
    )]
    CreatePreconditionFailed(String) = 5044,
    #[error(
        "Cannot upgrade the database: {0} missing. Upgrade works on an existing \
         database; run 'create' to provision it first."
    )]
    UpgradePreconditionFailed(String) = 5045,
    #[error(
        "No configuration entry for database component '{0}'. Every registered \
         component needs its own entry under `database:` with its connect \
         options and credentials."
    )]
    MissingComponentEntry(String) = 5033,
    #[error(
        "The `database_server:` entry points at non-local host '{0}', so its \
         server is not ours to run. Point the `host:` at localhost to boot a \
         server here."
    )]
    NonLocalServerHost(String) = 5034,
    #[error(
        "The `database_server:` entry must not define a `schema:`. It is the \
         privileged server connection and has no home schema."
    )]
    ServerSchemaNotAllowed = 5036,
    #[error(
        "The `database_server:` entry must not define an `app_role:`. It is \
         the privileged server connection; each component's own entry carries \
         the app role it provisions and serves with."
    )]
    ServerAppRoleNotAllowed = 5042,
    #[error(
        "Identifier '{0}' exceeds PostgreSQL's 63-byte limit (it would be \
         silently truncated). Use a shorter database name, or set an explicit \
         `schema:` / `app_role:` on the entry."
    )]
    IdentifierTooLong(String) = 5043,
    #[error(
        "`${{embedded}}` is not valid for a component's `username`: the owner \
         role name is the schema name, which is already the default when the \
         field is omitted."
    )]
    EmbeddedUsernameNotAllowed = 5038,
    #[error(
        "The booted server's superuser is always 'postgres' — it cannot be \
         named '{0}'. Set the `database_server:` `username:` to 'postgres', \
         `${{embedded}}`, or omit it."
    )]
    ServerUsernameNotBootable(String) = 5039,
    #[error(
        "The database data directory cannot be resolved. Set the \
         TD_DATABASE_DIR environment variable (or TD_URI_REPOSITORY, whose \
         `database` subdirectory is the default)."
    )]
    MissingDatabaseDir = 5037,
    #[error(
        "A database server is already running on the data directory, but it \
         listens on port {running} while the `database_server:` URL configures \
         port {configured}. Align the URL with the running server, or stop the \
         server before starting it on the new port."
    )]
    ServerPortMismatch { running: u16, configured: u16 } = 5040,
    #[error(
        "The database server rejected the configured admin credentials: {0}. \
         The data directory was likely initialized with different credentials \
         (e.g. the secret changed after the first boot). Restore the original \
         credentials in the configuration, or re-initialize the data directory \
         (destroying its data) so the next boot bakes in the new ones."
    )]
    ServerAuthenticationFailed(String) = 5041,
    #[error(
        "Unknown database schema entry '{0}'. Add an entry named '{0}' to the \
         database configuration."
    )]
    UnknownSchemaEntry(String) = 5027,
    #[error(
        "No database schema components are registered. This binary is missing the \
         `te_database::force_link!()` anchor, so the linker dropped the component \
         registrations. Add the anchor at the binary's top level."
    )]
    NoComponentsRegistered = 5032,
    #[error(
        "Database components '{1}' and '{2}' both resolve to schema '{0}'. Each \
         component owns its own schema and owner role; give them distinct \
         `schema:` values, or distinct component keys under a shared database."
    )]
    DuplicateComponentSchema(String, String, String) = 5047,
    #[error(
        "Failed to connect to the database: {0}. Verify the database server is \
         running and reachable, and that the configured host, port, credentials, \
         and database name are correct (and that any firewall or TLS settings \
         allow the connection)."
    )]
    FailedToConnectToDatabase(#[from] sqlx::Error) = 5007,
    #[error(
        "Failed to create or upgrade the database schema: {0}. The connecting \
         (upgrader) role may lack CREATE/ALTER on the application schema, or a \
         previous run left the schema partially migrated. Check the role's \
         privileges and the server logs, then retry the upgrade \
         (`td_dbmanager db upgrade`)."
    )]
    FailedToCreateOrUpgradeDatabaseSchema(#[source] sqlx::Error) = 5008,
    #[error(
        "Database query failed: {0}. This is unexpected — check the server logs \
         and the database server's health; if it persists, report it with the \
         surrounding log context."
    )]
    SqlError(#[source] sqlx::Error) = 5009,
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
         The role must exist and the connecting role must be a member of it — \
         run `td_dbmanager db create` against this database, or grant it \
         manually: CREATE ROLE \"{0}\" NOBYPASSRLS; GRANT \"{0}\" TO \
         <connecting role>; GRANT USAGE ON SCHEMA \"{1}\" TO \"{0}\"; \
         GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{1}\" \
         TO \"{0}\". (Underlying error: {2})"
    )]
    AppRoleNotSwitchable(String, String, #[source] sqlx::Error) = 5019,
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
    #[error(
        "Another upgrader is creating or upgrading schema '{0}' right now. \
         Only one process may run a schema's delta ladder at a time; retry \
         once it has finished."
    )]
    ConcurrentUpgradeInProgress(String) = 5046,
}
