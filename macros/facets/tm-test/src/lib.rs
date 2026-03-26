//
// Copyright 2025 Tabs Data Inc.
//

//! SQLX:
//! Generate a test function that sets up a test database with fixtures.
//! Fixtures are currently passed as a String. The path constructed is relative to the current
//! test: ./fixtures/<fixture>.sql. If a fixture is passed, no schema is loaded.
//! The function must have a TdPool parameter.
//!
//! By default (no fixtures), the full schema is created via `TdPool::upgrade()`.
//!
//! # Examples
//! #[test(sqlx)]
//! #[test(sqlx(fixture = "table"))]

extern crate proc_macro;
mod scoped_test;

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    scoped_test::scoped_test(args, item)
}
