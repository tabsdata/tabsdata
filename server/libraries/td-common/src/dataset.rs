//
// Copyright 2025 Tabs Data Inc.
//

use crate::id::Id;
use crate::uri::Versions;
use std::fmt::{Debug, Display};
use std::hash::Hash;

/// Trait for types that can be used as dataset references.
pub trait DatasetRef: Eq + Hash + Clone + Debug + Display {
    fn collection(&self) -> &str;
    fn dataset(&self) -> &str;
}

impl DatasetRef for String {
    fn collection(&self) -> &str {
        self.as_str()
    }

    fn dataset(&self) -> &str {
        self.as_str()
    }
}

/// A trait for types that can be used as table references.
pub trait TableRef: Eq + Hash + Clone + Debug + Display {}
impl TableRef for String {}
impl TableRef for Id {}

/// Trait for types that can be used as version references.
pub trait VersionRef: Eq + Hash + Clone + Debug + Sized {
    /// Returns the number of existing versions.
    fn existing_count(&self) -> usize;
}

impl VersionRef for String {
    fn existing_count(&self) -> usize {
        1
    }
}

impl VersionRef for Id {
    fn existing_count(&self) -> usize {
        1
    }
}

impl VersionRef for Option<Id> {
    fn existing_count(&self) -> usize {
        self.as_ref().map(|_| 1).unwrap_or(0)
    }
}

impl VersionRef for Versions {
    fn existing_count(&self) -> usize {
        self.fixed().len()
    }
}
