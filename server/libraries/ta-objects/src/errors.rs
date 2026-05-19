//
// Copyright 2026 Tabs Data Inc.
//

use crate::DaoCore;
use sea_query::TableRef;
use td_error::td_error;
use tracing::{error, warn};

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
    #[error("Type with name '{0}' was not found in '{1:?}'")]
    TypeNotFound(String, Box<TableRef>) = 5001,
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
}

/// Extra context extracted from a database error across all backends.
struct DbErrorInfo {
    /// Column names involved in the violation.
    columns: Vec<String>,
    /// Table name reported by the database (PostgreSQL only via trait).
    table: Option<String>,
    /// Extra detail from the database (PostgreSQL `DETAIL` field).
    detail: Option<String>,
}

/// Extract column names, table, and detail from a database error.
///
/// Uses a multi-strategy approach to work across all backends:
///
/// 1. **PostgreSQL**: downcast to `PgDatabaseError` for `column()` and `detail()`.
///    The base trait provides `table()` (populated by PostgreSQL).
/// 2. **Constraint name convention**: parse `<table>___<col(s)>___idx` (all backends).
/// 3. **SQLite message parsing**: `"UNIQUE constraint failed: table.col"` etc.
/// 4. **MySQL message parsing**: `"Column 'x' cannot be null"`,
///    `"Duplicate entry 'v' for key 'k'"`.
fn parse_db_error_info(db_err: &dyn sqlx::error::DatabaseError) -> DbErrorInfo {
    // table() is on the base DatabaseError trait (populated by PostgreSQL).
    let table = db_err.table().map(|s| s.to_string());
    #[allow(unused_mut)] // mutated inside #[cfg(feature = "postgres")] block
    let mut detail: Option<String> = None;

    // Strategy 1: PostgreSQL — downcast for column() and detail().
    #[cfg(feature = "postgres")]
    {
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
    }

    // Strategy 2: parse constraint name (e.g. "users___name___idx" → ["name"])
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

    // Strategy 3: SQLite message parsing
    // e.g. "UNIQUE constraint failed: table.col1, table.col2"
    #[cfg(feature = "sqlite")]
    {
        let msg = db_err.message();
        if let Some(after) = msg
            .strip_prefix("UNIQUE constraint failed: ")
            .or_else(|| msg.strip_prefix("NOT NULL constraint failed: "))
            .or_else(|| msg.strip_prefix("FOREIGN KEY constraint failed: "))
            .or_else(|| msg.strip_prefix("CHECK constraint failed: "))
        {
            return DbErrorInfo {
                columns: after
                    .split(", ")
                    .filter_map(|part| part.split('.').next_back())
                    .map(|s| s.to_string())
                    .collect(),
                table,
                detail,
            };
        }
    }

    // Strategy 4: MySQL message parsing
    // NOT NULL: "Column 'name' cannot be null"
    #[cfg(feature = "mysql")]
    {
        let msg = db_err.message();
        if let Some(rest) = msg.strip_prefix("Column '")
            && let Some(col) = rest.split('\'').next()
        {
            return DbErrorInfo {
                columns: vec![col.to_string()],
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

/// Classify a `sqlx::Error` into the appropriate `SqlError` variant,
/// enriching constraint-violation messages with column names and values,
/// and logging the error with structured context.
fn classify(entity: String, error: sqlx::Error, values: &[(String, String)]) -> SqlError {
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
    /// Classify a `sqlx::Error` for a write operation (insert/update),
    /// with DAO values available for hint enrichment.
    pub fn on_write<D: DaoCore>(error: sqlx::Error, values: &[(String, String)]) -> Self {
        classify(D::display_name().to_string(), error, values)
    }

    /// Classify a `sqlx::Error` for a read operation,
    /// with no DAO values available.
    pub fn on_read<D: DaoCore>(error: sqlx::Error) -> Self {
        classify(D::display_name().to_string(), error, &[])
    }
}
