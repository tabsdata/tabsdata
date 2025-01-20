//
//  Copyright 2024 Tabs Data Inc.
//

use crate::error::{ConnectionError, FromHandlerError};
use crate::handler::Handler;
use async_trait::async_trait;
use sqlx::pool::PoolConnection;
use sqlx::{Sqlite, SqliteConnection, Transaction};
use std::any::type_name;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

/// Trait for extracting an instance of a type from a `Handler`.
#[async_trait]
pub trait FromHandler: Sized {
    /// Extracts an instance of the type from the given `Handler`.
    async fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError>;
}

/// Wrapper for an input value. Input values can also be generated in inner services.
pub struct Input<T>(pub Arc<T>);

impl<T> Input<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> Clone for Input<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

#[async_trait]
impl<T> FromHandler for Input<T>
where
    T: Send + Sync + 'static,
{
    async fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError> {
        let value = match handler.get::<Input<T>>() {
            // Note that this just clones the Arc, not T itself
            Some(value) => Ok(value.clone()),
            None => Err(FromHandlerError::NotFound(String::from(type_name::<T>()))),
        }?;
        Ok(value)
    }
}

/// Wrapper for a context value.
pub struct Context<T>(pub Arc<T>);

impl<T> Context<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> Clone for Context<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

#[async_trait]
impl<T> FromHandler for Context<T>
where
    T: Send + Sync + 'static,
{
    async fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError> {
        let value = match handler.get::<Context<T>>() {
            // Note that this just clones the Arc, not T itself
            Some(value) => Ok(value.clone()),
            None => Err(FromHandlerError::NotFound(String::from(type_name::<T>()))),
        }?;
        Ok(value)
    }
}

#[derive(Debug)]
pub enum ConnectionType {
    /// A transaction connection.
    Transaction(Transaction<'static, Sqlite>),
    /// A pooled connection.
    PoolConnection(PoolConnection<Sqlite>),
}

impl From<ConnectionType> for Arc<Mutex<Option<ConnectionType>>> {
    fn from(connection: ConnectionType) -> Self {
        Arc::new(Mutex::new(Some(connection)))
    }
}

/// Trait for obtaining a mutable reference to a `SqliteConnection`.
pub trait IntoMutSqlConnection {
    /// Gets a mutable reference to the `SqliteConnection`. It is obtained through dereferencing
    /// the connection type.
    fn get_mut_connection(&mut self) -> Result<&mut SqliteConnection, ConnectionError>;
}

impl IntoMutSqlConnection for MutexGuard<'_, Option<ConnectionType>> {
    fn get_mut_connection(&mut self) -> Result<&mut SqliteConnection, ConnectionError> {
        let conn = if let Some(conn) = self.deref_mut() {
            Ok(conn.deref_mut())
        } else {
            Err(ConnectionError::ConnectionLost)
        };
        conn
    }
}

impl Deref for ConnectionType {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        match self {
            ConnectionType::Transaction(transaction) => transaction,
            ConnectionType::PoolConnection(pool_connection) => pool_connection,
        }
    }
}

impl DerefMut for ConnectionType {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ConnectionType::Transaction(transaction) => transaction.deref_mut(),
            ConnectionType::PoolConnection(pool_connection) => pool_connection.deref_mut(),
        }
    }
}

/// Wrapper for a connection. It can hold both PoolConnection and Transaction.
pub struct Connection(pub Arc<Mutex<Option<ConnectionType>>>);

impl Connection {
    pub fn new(transaction: Arc<Mutex<Option<ConnectionType>>>) -> Self {
        Self(transaction)
    }

    pub fn arc(&self) -> Arc<Mutex<Option<ConnectionType>>> {
        self.0.clone()
    }
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

#[async_trait]
impl FromHandler for Connection {
    async fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError> {
        if let Some(conn) = handler.get::<Connection>() {
            // Note that this just clones the Arc, not the connection itself
            Ok(conn.clone())
        } else {
            Err(FromHandlerError::NotFound(String::from("connection")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extractors;
    use sqlx::Connection;

    #[tokio::test]
    async fn test_input_extractor() {
        let input = Input::new(42);
        let mut handler = Handler::new();
        handler.insert(input.clone());

        let retrieved_input: Input<i32> = Input::from_handler(&handler).await.unwrap();
        assert_eq!(*retrieved_input.0, 42);
    }

    #[tokio::test]
    async fn test_context_extractor() {
        let context = Context::new(String::from("test"));
        let mut handler = Handler::new();
        handler.insert(context.clone());

        let retrieved_context: Context<String> = Context::from_handler(&handler).await.unwrap();
        assert_eq!(*retrieved_context.0, "test");
    }

    #[tokio::test]
    async fn test_connection_extractor_pool_connection() {
        let db = td_database::test_utils::db().await.unwrap();
        let connection = db.acquire().await.unwrap();
        let connection = ConnectionType::PoolConnection(connection).into();
        let connection = extractors::Connection::new(connection);

        let mut handler = Handler::new();
        handler.insert(connection.clone());

        let retrieved_connection = extractors::Connection::from_handler(&handler)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&retrieved_connection.0, &connection.0));

        let mut conn = retrieved_connection.0.lock().await;
        let conn = conn.get_mut_connection().unwrap();
        assert!(conn.ping().await.is_ok());
    }

    #[tokio::test]
    async fn test_connection_extractor_transaction() {
        let db = td_database::test_utils::db().await.unwrap();
        let transaction = db.begin().await.unwrap();
        let transaction = ConnectionType::Transaction(transaction).into();
        let connection = extractors::Connection::new(transaction);

        let mut handler = Handler::new();
        handler.insert(connection.clone());

        let retrieved_connection = extractors::Connection::from_handler(&handler)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&retrieved_connection.0, &connection.0));

        let mut conn = retrieved_connection.0.lock().await;
        let conn = conn.get_mut_connection().unwrap();
        assert!(conn.ping().await.is_ok());
    }
}
