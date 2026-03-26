//
// Copyright 2024 Tabs Data Inc.
//

use crate::error::FromHandlerError;
use crate::handler::Handler;
use std::any::type_name;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Trait for extracting an instance of a type from a `Handler`.
pub trait FromHandler: Sized {
    /// Extracts an instance of the type from the given `Handler`.
    fn from_handler(handler: &mut Handler) -> Result<Self, FromHandlerError>;

    /// Called after the from_fn closure returns. Default: no-op.
    /// Used by MutRef to restore values back into the handler.
    fn post_extract(_handler: &mut Handler) {}

    /// Returns the list of types this extractor actually consumes from the handler.
    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![crate::metadata::type_of::<Self>()]
    }
}

/// Shared read-only reference. Clones the internal Arc.
pub struct Ref<T: ?Sized>(pub(crate) Arc<T>);

impl<T> Ref<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }

    pub fn from_arc(arc: Arc<T>) -> Self {
        Self(arc)
    }
}

impl<T> Deref for Ref<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // it will never expect due to rust borrow checker (it won't even compile)
        self.0.as_ref()
    }
}

impl<T> Clone for Ref<T> {
    fn clone(&self) -> Self {
        Ref(self.0.clone())
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Ref<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> AsRef<T> for Ref<T> {
    fn as_ref(&self) -> &T {
        // it will never expect due to rust borrow checker (it won't even compile)
        self.0.as_ref()
    }
}

impl<T: Send + 'static> FromHandler for Ref<T> {
    fn from_handler(handler: &mut Handler) -> Result<Self, FromHandlerError> {
        let value = handler
            .get::<T>()
            .ok_or_else(|| FromHandlerError::NotFound(type_name::<T>().into()))?;
        Ok(value)
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![crate::metadata::type_of::<T>()]
    }
}

/// Internal restoration slot, stored in handler by MutRef::from_handler.
/// post_extract receives the value from the oneshot channel and restores it.
struct RefSlot<T>(tokio::sync::oneshot::Receiver<T>);

/// Mutable owned value with auto-restore on drop.
///
/// Takes ownership from handler; when dropped, the value is automatically restored
/// to the handler via `post_extract`. Use `into_inner()` to opt out of auto-restore.
///
/// `MutRef::new()` creates a standalone value with no restore slot.
pub struct MutRef<T> {
    value: Option<T>,
    sender: Option<tokio::sync::oneshot::Sender<T>>,
}

impl<T> MutRef<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: Some(value),
            sender: None,
        }
    }
}

impl<T> Drop for MutRef<T> {
    fn drop(&mut self) {
        if let (Some(value), Some(sender)) = (self.value.take(), self.sender.take()) {
            let _ = sender.send(value);
        }
    }
}

impl<T> Deref for MutRef<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // it will never expect due to rust borrow checker (it won't even compile)
        self.value.as_ref().expect("MutRef value already consumed")
    }
}

impl<T> DerefMut for MutRef<T> {
    fn deref_mut(&mut self) -> &mut T {
        // it will never expect due to rust borrow checker (it won't even compile)
        self.value.as_mut().expect("MutRef value already consumed")
    }
}

impl<T: Send + 'static> FromHandler for MutRef<T> {
    fn from_handler(handler: &mut Handler) -> Result<Self, FromHandlerError> {
        let value = handler
            .take::<T>()
            .ok_or_else(|| FromHandlerError::NotFound(type_name::<T>().into()))?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        handler.insert(RefSlot::<T>(receiver));
        Ok(MutRef {
            value: Some(value),
            sender: Some(sender),
        })
    }

    fn post_extract(handler: &mut Handler) {
        // Drop already fired — value was sent through the channel. Move it back to handler.
        if let Some(mut slot) = handler.remove::<RefSlot<T>>() {
            if let Ok(value) = slot.0.try_recv() {
                handler.insert(value);
            }
        }
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![crate::metadata::type_of::<T>()]
    }
}

/// Owned value. Takes ownership via Arc::try_unwrap. Value is consumed.
/// Read-only access via Deref. Use `into_inner()` to consume.
pub struct Owned<T>(T);

impl<T> Owned<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Send + 'static> FromHandler for Owned<T> {
    fn from_handler(handler: &mut Handler) -> Result<Self, FromHandlerError> {
        handler
            .take::<T>()
            .map(Owned)
            .ok_or_else(|| FromHandlerError::NotFound(type_name::<T>().into()))
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![crate::metadata::type_of::<T>()]
    }
}

// () — trivial extractor for "no input" (e.g. By::<()>::list).
impl FromHandler for () {
    fn from_handler(_handler: &mut Handler) -> Result<Self, FromHandlerError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx_service::ReqCtx;

    #[tokio::test]
    async fn test_ref_extractor() {
        let mut handler = Handler::new();
        handler.set(42i32);

        let retrieved: Ref<i32> = Ref::from_handler(&mut handler).unwrap();
        assert_eq!(*retrieved, 42);
    }

    #[tokio::test]
    async fn test_mutref_extractor() {
        let mut handler = Handler::new();
        handler.set(42i32);

        let mut retrieved: MutRef<i32> = MutRef::from_handler(&mut handler).unwrap();
        *retrieved += 1;
        assert_eq!(*retrieved, 43);
    }

    #[tokio::test]
    async fn test_mutref_auto_restore() {
        let mut handler = Handler::new();
        handler.set(42i32);

        {
            let mut retrieved: MutRef<i32> = MutRef::from_handler(&mut handler).unwrap();
            *retrieved += 1;
            assert_eq!(*retrieved, 43);
            // MutRef drops here, sends 43 through the channel
        }

        // post_extract moves from channel back into handler
        MutRef::<i32>::post_extract(&mut handler);

        // Value should be restored
        let restored: MutRef<i32> = MutRef::from_handler(&mut handler).unwrap();
        assert_eq!(*restored, 43);
    }

    #[tokio::test]
    async fn test_mutref_into_inner_no_restore() {
        let mut handler = Handler::new();
        handler.set(42i32);

        let retrieved: MutRef<i32> = MutRef::from_handler(&mut handler).unwrap();
        let val = *retrieved;
        assert_eq!(val, 42);

        // post_extract should find nothing to restore
        MutRef::<i32>::post_extract(&mut handler);

        // Value should NOT be in the handler
        assert!(handler.take::<i32>().is_none());
    }

    #[tokio::test]
    async fn test_owned_extractor() {
        let mut handler = Handler::new();
        handler.set(42i32);

        let retrieved: Owned<i32> = Owned::from_handler(&mut handler).unwrap();
        assert_eq!(*retrieved, 42);
        // Value should be removed from handler
        assert!(handler.get::<i32>().is_none());
    }

    #[tokio::test]
    async fn test_pool_connection() {
        let db = td_database::test_utils::db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        assert!(conn.ping().await.is_ok());
    }

    #[tokio::test]
    async fn test_transaction_as_connection() {
        let db = td_database::test_utils::db().await.unwrap();
        let transaction = db.begin().await.unwrap();
        let mut conn = transaction.into_connection();
        assert!(conn.ping().await.is_ok());
    }

    #[tokio::test]
    async fn test_mutref_dynamic_connection() {
        use td_database::dynamic::DynamicConnection;

        let db = td_database::test_utils::db().await.unwrap();
        let conn = db.acquire().await.unwrap();

        let mut handler = Handler::new();
        handler.insert(conn);

        let mut conn: MutRef<DynamicConnection<'static>> =
            MutRef::from_handler(&mut handler).unwrap();
        assert!(conn.ping().await.is_ok());
    }

    #[test]
    fn test_ctx_extractor() {
        let context = ReqCtx::default();
        let mut handler = Handler::new();
        handler.set(context);

        let retrieved: MutRef<ReqCtx> = MutRef::from_handler(&mut handler).unwrap();
        assert_eq!(retrieved.error_count(), 0);
        assert_eq!(retrieved.warning_count(), 0);
        assert_eq!(retrieved.notification_count(), 0);
    }
}
