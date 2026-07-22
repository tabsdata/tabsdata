//
//  Copyright 2024 Tabsdata Inc.
//

use crate::extractors::Ref;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

/// A `Handler` struct that stores values of different types in a `HashMap`.
/// The values are stored as `Box<dyn Any + Send>`, allowing for type-safe storage and retrieval.
#[derive(Default)]
pub struct Handler {
    map: HashMap<TypeId, Box<dyn Any + Send>>,
}

pub trait IntoHandler: Sized {
    fn into_handler(self) -> Handler;
}

impl IntoHandler for Handler {
    fn into_handler(self) -> Handler {
        self
    }
}

impl Handler {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    // ===== Shared tier (Arc-wrapped, keyed by TypeId::of::<Ref<T>>()) =====

    /// Store a value wrapped in Arc for shared read access via `Ref<T>`.
    pub fn set<T: Send + Sync + 'static>(&mut self, val: T) {
        self.map
            .insert(TypeId::of::<Ref<T>>(), Box::new(Ref::new(val)));
    }

    /// Store an existing Arc without double-wrapping.
    pub fn set_arc<T: Send + Sync + 'static>(&mut self, arc: Arc<T>) {
        self.map.insert(TypeId::of::<Ref<T>>(), Box::new(Ref(arc)));
    }

    /// Clone the Ref for shared read access.
    pub fn get<T: 'static>(&self) -> Option<Ref<T>> {
        self.map
            .get(&TypeId::of::<Ref<T>>())
            .and_then(|boxed| boxed.downcast_ref::<Ref<T>>())
            .map(|r| Ref(r.0.clone()))
    }

    // ===== Exclusive tier (raw values, keyed by TypeId::of::<T>()) =====

    /// Store a raw value for exclusive access (no Arc wrapping).
    pub fn insert<T: Send + 'static>(&mut self, val: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(val));
    }

    /// Remove and return a raw value from the exclusive tier.
    pub fn remove<T: 'static>(&mut self) -> Option<T> {
        self.map
            .remove(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast::<T>().ok())
            .map(|boxed| *boxed)
    }

    /// Get a mutable reference to a raw value in the exclusive tier.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_mut::<T>())
    }

    // ===== Cross-tier =====

    /// Remove a value from either tier. Tries shared (unwrap Arc) first, then exclusive.
    pub fn take<T: 'static>(&mut self) -> Option<T> {
        self.map
            .remove(&TypeId::of::<Ref<T>>())
            .and_then(|boxed| boxed.downcast::<Ref<T>>().ok())
            .and_then(|r| Arc::try_unwrap(r.0).ok())
            .or_else(|| {
                self.map
                    .remove(&TypeId::of::<T>())
                    .and_then(|boxed| boxed.downcast::<T>().ok())
                    .map(|boxed| *boxed)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_handler() {
        let handler = Handler::new();
        assert!(handler.map.is_empty());
    }

    // --- Shared tier tests ---

    #[test]
    fn test_set_and_get() {
        let mut handler = Handler::new();
        handler.set(42u32);
        let value = handler.get::<u32>();
        assert_eq!(value.as_deref(), Some(&42));
    }

    #[test]
    fn test_get_non_existent() {
        let handler = Handler::new();
        let value = handler.get::<u32>();
        assert!(value.is_none());
    }

    #[test]
    fn test_set_and_take() {
        let mut handler = Handler::new();
        handler.set(42u32);
        let value = handler.take::<u32>();
        assert_eq!(value, Some(42));
    }

    #[test]
    fn test_set_arc() {
        let mut handler = Handler::new();
        handler.set_arc(Arc::new(42u32));
        let value = handler.get::<u32>();
        assert_eq!(value.as_deref(), Some(&42));
    }

    // --- Exclusive tier tests ---

    #[test]
    fn test_insert_and_remove() {
        let mut handler = Handler::new();
        handler.insert(42u32);
        let value = handler.remove::<u32>();
        assert_eq!(value, Some(42));
    }

    #[test]
    fn test_insert_and_get_mut() {
        let mut handler = Handler::new();
        handler.insert(42u32);
        let value = handler.get_mut::<u32>();
        assert_eq!(value, Some(&mut 42));
        *value.unwrap() = 99;
        assert_eq!(handler.remove::<u32>(), Some(99));
    }

    // --- Cross-tier: take checks both ---

    #[test]
    fn test_take_from_exclusive() {
        let mut handler = Handler::new();
        handler.insert(42u32);
        let value = handler.take::<u32>();
        assert_eq!(value, Some(42));
    }

    #[test]
    fn test_take_non_existent() {
        let mut handler = Handler::new();
        let value = handler.take::<u32>();
        assert!(value.is_none());
    }
}
