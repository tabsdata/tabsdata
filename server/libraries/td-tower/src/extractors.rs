//
// Copyright 2024 Tabs Data Inc.
//

use crate::ctx_service::{CtxMap, InnerContext};
use crate::error::{ConnectionError, FromHandlerError};
use crate::handler::Handler;
use std::any::type_name;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use td_database::dynamic::{DynamicConnection, DynamicTransaction};
use tokio::sync::{Mutex, MutexGuard};

/// Trait for extracting an instance of a type from a `Handler`.
pub trait FromHandler: Sized {
    /// Extracts an instance of the type from the given `Handler`.
    fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError>;

    /// Returns the list of types this extractor actually consumes from the handler.
    ///
    /// The default returns `[type_of::<Self>()]`, which is correct for simple extractors
    /// like `Input<T>`, `Connection`, `SrvCtx<T>`, etc.
    ///
    /// Composite extractors that consume different/multiple types from the handler
    /// (e.g. `SqlConditionInput<All2<A, B>>` consumes `Input<A>` and `Input<B>`)
    /// should override this method to return the actual types consumed.
    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![crate::metadata::type_of::<Self>()]
    }
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

impl<T> FromHandler for Input<T>
where
    T: Send + Sync + 'static,
{
    fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError> {
        let value = match handler.get::<Input<T>>() {
            // Note that this just clones the Arc, not T itself
            Some(value) => Ok(value.clone()),
            None => Err(FromHandlerError::NotFound(String::from(type_name::<T>()))),
        }?;
        Ok(value)
    }
}

// () — trivial extractor for "no input" (e.g. By::<()>::list).
impl FromHandler for () {
    fn from_handler(_handler: &Handler) -> Result<Self, FromHandlerError> {
        Ok(())
    }
}

/// Wrapper for a context value.
pub struct SrvCtx<T>(pub Arc<T>);

impl<T> SrvCtx<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> Clone for SrvCtx<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> FromHandler for SrvCtx<T>
where
    T: Send + Sync + 'static,
{
    fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError> {
        let value = match handler.get::<SrvCtx<T>>() {
            // Note that this just clones the Arc, not T itself
            Some(value) => Ok(value.clone()),
            None => Err(FromHandlerError::NotFound(String::from(type_name::<T>()))),
        }?;
        Ok(value)
    }
}

/// The raw connection type — either a transaction or a pool connection.
#[derive(Debug)]
pub enum ConnectionType {
    Transaction(DynamicTransaction<'static>),
    PoolConnection(DynamicConnection<'static>),
}

impl From<DynamicTransaction<'static>> for ConnectionType {
    fn from(transaction: DynamicTransaction<'static>) -> Self {
        Self::Transaction(transaction)
    }
}

impl From<DynamicConnection<'static>> for ConnectionType {
    fn from(connection: DynamicConnection<'static>) -> Self {
        Self::PoolConnection(connection)
    }
}

/// The locked guard — the only way to access the underlying connection.
pub struct ConnectionGuard<'a> {
    guard: MutexGuard<'a, Option<ConnectionType>>,
}

impl ConnectionGuard<'_> {
    pub fn get(&mut self) -> Result<DynamicConnection<'_>, ConnectionError> {
        if let Some(conn) = self.guard.deref_mut() {
            let conn = match conn {
                ConnectionType::Transaction(tx) => tx.as_mut_conn(),
                ConnectionType::PoolConnection(conn) => conn.as_borrowed(),
            };
            Ok(conn)
        } else {
            Err(ConnectionError::ConnectionLost)
        }
    }
}

/// The actual connection handle — holds the shared state and exposes lock().
pub struct ConnectionInner {
    conn: Mutex<Option<ConnectionType>>,
}

impl ConnectionInner {
    fn new(conn: ConnectionType) -> Self {
        Self {
            conn: Mutex::new(Some(conn)),
        }
    }

    pub async fn lock(&self) -> ConnectionGuard<'_> {
        ConnectionGuard {
            guard: self.conn.lock().await,
        }
    }

    /// Reclaims ownership of the connection — used by layers for commit/rollback.
    pub(crate) async fn reclaim(&self) -> Result<ConnectionType, ConnectionError> {
        self.conn
            .lock()
            .await
            .take()
            .ok_or(ConnectionError::ConnectionLost)
    }
}

/// The extractor — a newtype around Arc<ConnectionInner> that implements FromHandler.
pub struct Connection(pub Arc<ConnectionInner>);

impl Connection {
    pub fn new(conn: impl Into<ConnectionType>) -> Self {
        Self(Arc::new(ConnectionInner::new(conn.into())))
    }

    pub(crate) fn arc(&self) -> Arc<ConnectionInner> {
        Arc::clone(&self.0)
    }
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl FromHandler for Connection {
    fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError> {
        handler
            .get::<Connection>()
            .map(|c| c.clone())
            .ok_or_else(|| FromHandlerError::NotFound(String::from("connection")))
    }
}

// We have an inner struct just so it looks like the other extractors in the layers.
#[derive(Debug, Default, Clone)]
pub struct ReqCtx(pub InnerContext);

impl ReqCtx {
    pub fn arc(&self) -> Arc<Mutex<Option<CtxMap>>> {
        self.0.deref().clone()
    }
}

impl FromHandler for ReqCtx {
    fn from_handler(handler: &Handler) -> Result<Self, FromHandlerError> {
        let value = match handler.get::<ReqCtx>() {
            Some(value) => Ok(value.clone()),
            None => Err(FromHandlerError::NotFound(String::from(
                type_name::<ReqCtx>(),
            ))),
        }?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_input_extractor() {
        let input = Input::new(42);
        let mut handler = Handler::new();
        handler.insert(input.clone());

        let retrieved_input: Input<i32> = Input::from_handler(&handler).unwrap();
        assert_eq!(*retrieved_input.0, 42);
    }

    #[tokio::test]
    async fn test_context_extractor() {
        let context = SrvCtx::new(String::from("test"));
        let mut handler = Handler::new();
        handler.insert(context.clone());

        let retrieved_context: SrvCtx<String> = SrvCtx::from_handler(&handler).unwrap();
        assert_eq!(*retrieved_context.0, "test");
    }

    #[tokio::test]
    async fn test_connection_extractor_pool_connection() {
        let db = td_database::test_utils::db().await.unwrap();
        let connection = db.acquire().await.unwrap();
        let connection = Connection::new(connection);

        let mut handler = Handler::new();
        handler.insert(connection.clone());

        let retrieved_connection = Connection::from_handler(&handler).unwrap();
        assert!(Arc::ptr_eq(&retrieved_connection.0, &connection.0));

        let mut conn = connection.0.lock().await;
        let mut conn = conn.get().unwrap();
        assert!(conn.ping().await.is_ok());
    }

    #[tokio::test]
    async fn test_connection_extractor_transaction() {
        let db = td_database::test_utils::db().await.unwrap();
        let transaction = db.begin().await.unwrap();
        let connection = Connection::new(transaction);

        let mut handler = Handler::new();
        handler.insert(connection.clone());

        let retrieved_connection = Connection::from_handler(&handler).unwrap();
        assert!(Arc::ptr_eq(&retrieved_connection.0, &connection.0));

        let mut conn = retrieved_connection.0.lock().await;
        let mut conn = conn.get().unwrap();
        assert!(conn.ping().await.is_ok());
    }

    #[tokio::test]
    async fn test_ctx_extractor() {
        let context = ReqCtx::default();
        let mut handler = Handler::new();
        handler.insert(context.clone());

        let retrieved_context: ReqCtx = ReqCtx::from_handler(&handler).unwrap();
        assert_eq!(retrieved_context.0.error_count().await, 0);
        assert_eq!(retrieved_context.0.warning_count().await, 0);
        assert_eq!(retrieved_context.0.notification_count().await, 0);
    }
}
