//
// Copyright 2024 Tabs Data Inc.
//

use bimap::BiHashMap;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::AddAssign;

/// The `Index` trait defines the requirements for types that can be used as indices in the `IndexMap`.
pub trait Index: Debug + Copy + Eq + Hash + From<usize> + AddAssign {}

/// Macro to define a new index type.
/// The generated type will implement the `Index` trait and required traits for use in `IndexMap`.
#[macro_export]
macro_rules! index {
    ($index_name:ident) => {
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
        pub struct $index_name(usize);

        impl From<usize> for $index_name {
            fn from(value: usize) -> Self {
                $index_name(value)
            }
        }

        impl std::ops::AddAssign for $index_name {
            fn add_assign(&mut self, rhs: Self) {
                self.0 += rhs.0;
            }
        }

        impl $crate::index_map::Index for $index_name {}
    };
}

/// A bidirectional map that associates indices of type `I` with values of type `T`. It allows
/// for fast index and item retrieval.
///
/// # Type Parameters
///
/// * `I`: The index type, which must implement the `Index` trait.
/// * `T`: The value type, which must implement `Eq` and `Hash`.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct IndexMap<I: Index, T: Eq + Hash> {
    map: BiHashMap<I, T>,
    next_index: I,
}

impl<I: Index, T: Debug + Eq + Hash> Debug for IndexMap<I, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<I: Index, T: Eq + Hash> FromIterator<(I, T)> for IndexMap<I, T> {
    fn from_iter<U: IntoIterator<Item = (I, T)>>(iter: U) -> Self {
        let map = BiHashMap::from_iter(iter);
        let len = map.len();
        IndexMap {
            map,
            next_index: I::from(len),
        }
    }
}

impl<I: Index, T: Eq + Hash> IndexMap<I, T> {
    /// Creates a new, empty `IndexMap`.
    pub fn new() -> Self {
        IndexMap {
            map: BiHashMap::default(),
            next_index: I::from(0),
        }
    }

    /// Returns an iterator over the entries of the `IndexMap`.
    pub fn iter(&self) -> impl Iterator<Item = (&I, &T)> {
        self.map.iter()
    }

    /// Returns if the `IndexMap` is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the number of elements in the `IndexMap`.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Inserts an item into the `IndexMap` if it is not already present.
    /// Returns the index of the item.
    pub fn insert_if_absent(&mut self, item: T) -> I {
        if let Some(&index) = self.index(&item) {
            index
        } else {
            let index = self.next_index;
            self.map.insert(index, item);
            self.next_index += I::from(1);
            index
        }
    }

    /// Returns a reference to the item associated with the given index, if it exists.
    pub fn get(&self, index: &I) -> Option<&T> {
        self.map.get_by_left(index)
    }

    /// Returns a reference to the index associated with the given item, if it exists.
    pub fn index(&self, item: &T) -> Option<&I> {
        self.map.get_by_right(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    index!(TestIndex);

    #[test]
    fn test_new_index_map() {
        let index_map: IndexMap<TestIndex, String> = IndexMap::new();
        assert_eq!(index_map.len(), 0);
    }

    #[test]
    fn test_insert_if_absent() {
        let mut index_map: IndexMap<TestIndex, String> = IndexMap::new();
        let index1 = index_map.insert_if_absent("item1".to_string());
        let index2 = index_map.insert_if_absent("item2".to_string());
        let index3 = index_map.insert_if_absent("item1".to_string()); // Duplicate item

        assert_eq!(index_map.len(), 2);
        assert_eq!(index1, index3); // Ensure the same index is returned for duplicate items
        assert_ne!(index1, index2);
    }

    #[test]
    fn test_get_by_index() {
        let mut index_map: IndexMap<TestIndex, String> = IndexMap::new();
        let index = index_map.insert_if_absent("item1".to_string());

        assert_eq!(index_map.get(&index), Some(&"item1".to_string()));
    }

    #[test]
    fn test_get_by_item() {
        let mut index_map: IndexMap<TestIndex, String> = IndexMap::new();
        let index = index_map.insert_if_absent("item1".to_string());

        assert_eq!(index_map.index(&"item1".to_string()), Some(&index));
    }

    #[test]
    fn test_len() {
        let mut index_map: IndexMap<TestIndex, String> = IndexMap::new();
        index_map.insert_if_absent("item1".to_string());
        index_map.insert_if_absent("item2".to_string());

        assert_eq!(index_map.len(), 2);
    }

    #[test]
    fn test_no_duplicate_items() {
        let mut index_map: IndexMap<TestIndex, String> = IndexMap::new();
        index_map.insert_if_absent("item1".to_string());
        index_map.insert_if_absent("item1".to_string()); // Duplicate item

        assert_eq!(index_map.len(), 1);
    }

    #[test]
    fn test_from_iter() {
        let items = vec![
            (TestIndex::from(0), "item1".to_string()),
            (TestIndex::from(1), "item2".to_string()),
        ];
        let index_map: IndexMap<TestIndex, String> = IndexMap::from_iter(items);

        assert_eq!(index_map.len(), 2);
        assert_eq!(
            index_map.get(&TestIndex::from(0)),
            Some(&"item1".to_string())
        );
        assert_eq!(
            index_map.get(&TestIndex::from(1)),
            Some(&"item2".to_string())
        );
    }
}
