//
// Copyright 2025 Tabs Data Inc.
//

//! SQLX:
//! Generate a test function that sets up a test database with fixtures.
//! Fixtures are curently passed as a String. The path constructed is relative to the current
//! test: ./fixtures/<fixture>.sql. If a fixture is passed, no schema is loaded, unless a migrator
//! is passed.
//! Migrator should contain DB definitions, and fixtures can contain eitehr definitions or default data.
//! The function must have a DbPool parameter.
//!
//! By default, the schema is loaded from [`td_schema::schema()`].
//!
//! # Examples
//! #[test(sqlx)]
//! #[test(sqlx(fixture = "table"))]
//! #[test(sqlx(migrator = td_schema::schema(), fixture = "table"))]
//! #[test(sqlx(migrator = td_schema::schema(), fixture = "table"))]

extern crate proc_macro;
mod scoped_test;

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    scoped_test::scoped_test(args, item)
}
