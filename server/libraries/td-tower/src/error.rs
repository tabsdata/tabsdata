//
// Copyright 2025 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum FromHandlerError {
    #[error("Fatal error: {0} not found in Service handler")]
    NotFound(String) = 5000,
    #[error("Fatal error: Multiple references to {0} in Service handler")]
    InternalError(String) = 5001,
}

#[td_error]
pub enum ConnectionError {
    #[error("Cannot get a connection to the database: {0}")]
    CannotGetConnection(#[source] sqlx::Error) = 5000,
    #[error("Cannot get begin a transaction to the database: {0}")]
    CannotBeginTransaction(#[source] sqlx::Error) = 5001,
    #[error("Cannot commit transaction to the database: {0}")]
    CannotCommitTransaction(#[source] sqlx::Error) = 5002,
    #[error("Broken connection while processing Service request")]
    ConnectionLost = 5003,
}
