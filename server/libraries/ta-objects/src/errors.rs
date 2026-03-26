//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::TableRef;
use td_error::td_error;

#[td_error]
pub enum SqlError {
    #[error("Could not find entity with [{0}] [{1}] in '{2}'")]
    CouldNotFindEntity(String, String, String) = 0,
    #[error("Entity with [{0}] [{1}] already exists in '{2}'")]
    EntityAlreadyExists(String, String, String) = 1,

    #[error("Could not insert entity in '{0}': {1}")]
    InsertError(String, #[source] sqlx::Error) = 2,
    #[error("Could not get entity with [{0}] [{1}] in '{2}': {3}")]
    SelectError(String, String, String, #[source] sqlx::Error) = 3,
    #[error("Could not update entity with [{0}] [{1}] in '{2}': {3}")]
    UpdateError(String, String, String, #[source] sqlx::Error) = 4,
    #[error("Could not delete entity with [{0}] [{1}] in '{2}': {3}")]
    DeleteError(String, String, String, #[source] sqlx::Error) = 5,
    #[error("Could not find entity in '{0}': {1}")]
    FindError(String, #[source] sqlx::Error) = 6,
    #[error("Could not update entity in '{0}': {1}")]
    UpdateAllError(String, #[source] sqlx::Error) = 7,

    #[error("SQL error: {0}")]
    AnySqlxError(#[from] sqlx::Error) = 5000,
    #[error("Type with name '{0}' was not found in '{1:?}'")]
    TypeNotFound(String, TableRef) = 5001,
    #[error("Error building SQL Query: {0}")]
    QueryError(#[from] sea_query::error::Error) = 5002,
    #[error(
        "Cannot SELECT Versions::New directly, relative versions must be found beforehand and converted to another form of Versions"
    )]
    SelectVersionsNew = 5003,
    #[error("DTO conversion error: {0}")]
    DtoConversionError(#[source] td_error::TdError) = 5004,
}
