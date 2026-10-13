//
// Copyright 2026 Tabsdata Inc.
//

use crate::Dao;
use td_error::td_error;
use tracing::{debug, error, warn};

#[td_error]
pub enum SqlError {
    #[error("Could not find the requested {0}{1}")]
    CouldNotFindEntity(String, String) = 1000,
    #[error("A {0} already exists{1}")]
    EntityAlreadyExists(String, String) = 1,

    #[error("[{0}] A record with this value already exists{1}")]
    UniqueViolation(String, String, #[source] sqlx::Error) = 2,
    #[error("[{0}] Referenced record does not exist{1}")]
    ForeignKeyViolation(String, String, #[source] sqlx::Error) = 3,
    #[error("[{0}] A required value is missing{1}")]
    NotNullViolation(String, String, #[source] sqlx::Error) = 4,
    #[error("[{0}] A data validation constraint was violated{1}")]
    CheckViolation(String, String, #[source] sqlx::Error) = 5,

    #[error("[{0}] The requested entity was not found")]
    RowNotFound(String, #[source] sqlx::Error) = 1001,

    #[error("[{0}] SQL error: {1}")]
    AnySqlxError(String, #[source] sqlx::Error) = 5000,

    #[error("[{0}] The database was busy; retry the request")]
    Busy(String, #[source] sqlx::Error) = 8000,
    #[error("Error building SQL Query: {0}")]
    QueryError(#[from] sea_query::error::Error) = 5002,
    #[error(
        "Cannot SELECT Versions::New directly, relative versions must be found beforehand and converted to another form of Versions"
    )]
    SelectVersionsNew = 5003,
    #[error("DTO conversion error: {0}")]
    DtoConversionError(#[source] td_error::TdError) = 5004,
    #[error(
        "[{0}] Refusing to run a destructive operation with no WHERE clause; pass an explicit condition"
    )]
    EmptyConditionRefused(String) = 5005,
    #[error("[{0}] A single-row read matched more than one row")]
    MultipleRowsMatched(String) = 5006,
}

/// Extra context extracted from a database error.
struct DbErrorInfo {
    /// Column names involved in the violation.
    columns: Vec<String>,
    /// Table name reported by the database.
    table: Option<String>,
    /// Extra detail from the database (the `DETAIL` field).
    detail: Option<String>,
}

/// Extract column names, table, and detail from a database error.
///
/// Two strategies, in order:
///
/// 1. Downcast to `PgDatabaseError` for `column()` and `detail()`. The base
///    trait provides `table()`.
/// 2. Constraint-name convention: parse `<table>___<col(s)>___idx`.
fn parse_db_error_info(db_err: &dyn sqlx::error::DatabaseError) -> DbErrorInfo {
    // table() is on the base DatabaseError trait.
    let table = db_err.table().map(|s| s.to_string());
    let mut detail: Option<String> = None;

    if let Some(pg) = db_err.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
        detail = pg.detail().map(|s| s.to_string());
        if let Some(col) = pg.column() {
            return DbErrorInfo {
                columns: vec![col.to_string()],
                table,
                detail,
            };
        }
        // PostgreSQL may not set column but still have detail.
        // Fall through to constraint parsing below.
    }

    // Constraint name (e.g. "users___name___idx" → ["name"])
    if let Some(constraint) = db_err.constraint() {
        let parts: Vec<&str> = constraint.split("___").collect();
        if parts.len() >= 3 && parts.last() == Some(&"idx") {
            return DbErrorInfo {
                columns: parts[1..parts.len() - 1]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                table,
                detail,
            };
        }
    }

    DbErrorInfo {
        columns: Vec::new(),
        table,
        detail,
    }
}

/// Format a hint string from column names and optional value pairs.
///
/// - With values: `" for 'name' = 'admin'"` or `" for 'user_id' = 'u1', 'role_id' = 'r1'"`
/// - Without values: `" for 'name'"` or `" for 'user_id', 'role_id'"`
/// - No columns: `""`
pub fn format_hint(columns: &[String], values: &[(String, String)]) -> String {
    if columns.is_empty() {
        return String::new();
    }

    let parts: Vec<String> = columns
        .iter()
        .map(|col| {
            if let Some((_, val)) = values.iter().find(|(k, _)| k == col) {
                format!("'{col}' = {val}")
            } else {
                format!("'{col}'")
            }
        })
        .collect();

    format!(" for {}", parts.join(", "))
}

/// SQLSTATEs of a statement the database gave up on or aborted to let
/// another through: a retry may succeed.
const CONFLICTS: [&str; 2] = [
    "40001", // serialization_failure
    "40P01", // deadlock_detected
];

const TIMEOUTS: [&str; 2] = [
    "55P03", // lock_not_available (lock_timeout)
    "57014", // query_canceled (statement_timeout)
];

/// SQLSTATEs of a statement the database refused for lack of a shared
/// resource, such as a full predicate-lock table: the same request later may
/// succeed.
const SHORTAGES: [&str; 1] = [
    "53200", // out_of_memory
];

/// Classify a `sqlx::Error` into the appropriate `SqlError` variant,
/// enriching constraint-violation messages with column names and values,
/// and logging the error with structured context.
fn classify(entity: String, error: sqlx::Error, values: &[(String, String)]) -> SqlError {
    let code = match &error {
        sqlx::Error::Database(db_err) => db_err.code().map(|code| code.into_owned()),
        _ => None,
    };
    if code
        .as_deref()
        .is_some_and(|code| CONFLICTS.contains(&code))
    {
        debug!(entity = %entity, sql_error = %error, "Retryable database error");
        return SqlError::Busy(entity, error);
    }
    if matches!(error, sqlx::Error::PoolTimedOut)
        || code
            .as_deref()
            .is_some_and(|code| TIMEOUTS.contains(&code) || SHORTAGES.contains(&code))
    {
        warn!(entity = %entity, sql_error = %error, "Retryable database error");
        return SqlError::Busy(entity, error);
    }
    match &error {
        sqlx::Error::RowNotFound => {
            warn!(entity = %entity, "Row not found");
            SqlError::RowNotFound(entity, error)
        }
        sqlx::Error::Database(db_err) => {
            let info = parse_db_error_info(db_err.as_ref());
            let hint = format_hint(&info.columns, values);
            let db_table = info.table.as_deref().unwrap_or("-");
            let db_detail = info.detail.as_deref().unwrap_or("-");
            match db_err.kind() {
                sqlx::error::ErrorKind::UniqueViolation => {
                    warn!(entity = %entity, table = %db_table, detail = %db_detail, hint = %hint, sql_error = %db_err, "Unique violation");
                    SqlError::UniqueViolation(entity, hint, error)
                }
                sqlx::error::ErrorKind::ForeignKeyViolation => {
                    warn!(entity = %entity, table = %db_table, detail = %db_detail, hint = %hint, sql_error = %db_err, "Foreign key violation");
                    SqlError::ForeignKeyViolation(entity, hint, error)
                }
                sqlx::error::ErrorKind::NotNullViolation => {
                    warn!(entity = %entity, table = %db_table, detail = %db_detail, hint = %hint, sql_error = %db_err, "Not null violation");
                    SqlError::NotNullViolation(entity, hint, error)
                }
                sqlx::error::ErrorKind::CheckViolation => {
                    warn!(entity = %entity, table = %db_table, detail = %db_detail, hint = %hint, sql_error = %db_err, "Check violation");
                    SqlError::CheckViolation(entity, hint, error)
                }
                _ => {
                    error!(entity = %entity, table = %db_table, detail = %db_detail, sql_error = %db_err, "Unexpected database error");
                    SqlError::AnySqlxError(entity, error)
                }
            }
        }
        _ => {
            error!(entity = %entity, sql_error = %error, "Unexpected SQL error");
            SqlError::AnySqlxError(entity, error)
        }
    }
}

impl SqlError {
    /// A unique violation as the "already exists" its pre-check reports: a
    /// concurrent write can take the value after the check passed.
    pub fn already_exists(self) -> Self {
        match self {
            SqlError::UniqueViolation(entity, hint, _) => {
                SqlError::EntityAlreadyExists(entity, hint)
            }
            other => other,
        }
    }

    /// Classify a `sqlx::Error` for a write operation (insert/update),
    /// with DAO values available for hint enrichment.
    pub fn on_write<D: Dao>(error: sqlx::Error, values: &[(String, String)]) -> Self {
        classify(D::display_name().to_string(), error, values)
    }

    /// Classify a `sqlx::Error` for a read operation,
    /// with no DAO values available.
    pub fn on_read<D: Dao>(error: sqlx::Error) -> Self {
        classify(D::display_name().to_string(), error, &[])
    }

    /// The not-found a filtered read raises when it matches nothing — what
    /// `fetch_one` would have returned on an empty result.
    pub fn not_found<D: Dao>() -> Self {
        SqlError::RowNotFound(D::display_name().to_string(), sqlx::Error::RowNotFound)
    }
}

#[cfg(test)]
mod tests;
