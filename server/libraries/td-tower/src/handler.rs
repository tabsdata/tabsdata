//
//  Copyright 2024 Tabs Data Inc.
//

use std::any::{Any, TypeId};
use std::collections::HashMap;

/// A `Handler` struct that stores values of different types in a `HashMap`.
/// The values are stored as `Box<dyn Any + Send + Sync>`, allowing for type-safe storage and retrieval.
/// Very similar to [`http::Extensions`], but usable outside of HTTP.
#[derive(Default)]
pub struct Handler {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
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

    pub fn insert<T: Send + Sync + 'static>(&mut self, val: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(val));
    }

    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|boxed| (**boxed).downcast_ref())
    }

    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<Box<T>> {
        self.map
            .remove(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast().ok())
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

    #[test]
    fn test_insert_and_get() {
        let mut handler = Handler::new();
        handler.insert(42u32);
        let value: Option<&u32> = handler.get();
        assert_eq!(value, Some(&42));
    }

    #[test]
    fn test_get_non_existent() {
        let handler = Handler::new();
        let value: Option<&u32> = handler.get();
        assert!(value.is_none());
    }

    #[test]
    fn test_remove() {
        let mut handler = Handler::new();
        handler.insert(42u32);
        let value: Option<Box<u32>> = handler.remove();
        assert_eq!(value, Some(Box::new(42)));
        let value: Option<&u32> = handler.get();
        assert!(value.is_none());
    }

    #[test]
    fn test_remove_non_existent() {
        let mut handler = Handler::new();
        let value: Option<Box<u32>> = handler.remove();
        assert!(value.is_none());
    }
}
