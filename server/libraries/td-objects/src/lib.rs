//
// Copyright 2025 Tabsdata Inc.
//

// Reexport API
pub use ta_objects::*;

// Allows the `Dao` macro's emitted code to use `td_objects::...` paths
// from within td-objects itself (the macro emits absolute paths so the
// same code works for both td-objects and external crates).
extern crate self as td_objects;

pub mod dxo;
pub mod graph;
pub mod list;
pub mod parse;
pub mod rest_urls;
pub mod system;
pub mod tower_service;
pub mod transaction;
pub mod types;

#[cfg(feature = "test-utils")]
pub mod test_utils;
