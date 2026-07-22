//
// Copyright 2025 Tabsdata Inc.
//

//! `#[td_test::test(...)]` — exactly one flavor must be set:
//!
//! * `db` — runs the test against freshly provisioned component schema(s). Bare
//!   `db` provisions the apiserver component and the function must take
//!   `db: TdPool`; `db(Type, ...)` provisions each listed component on its own
//!   schema and injects one `TdPool` per type, in declaration order.
//! * `requires = SomeReqs` + `env_prefix = "ns"` — runs only when the
//!   provider's env vars are present; otherwise skipped (with
//!   `TD_TEST_SKIP_IF_NO_REQS=true`) or panics. The function must take an arg
//!   of type `SomeReqs`.
//!
//! Any attribute key not handled by this macro (everything except `db`,
//! `requires`, `env_prefix`) is forwarded verbatim to the
//! auto-emitted `#[::tokio::test(...)]`, so `flavor`, `worker_threads`,
//! `start_paused`, etc. work without changes here. Call sites never need to
//! write the `#[tokio::test]` attribute themselves.
//!
//! # Examples
//!
//! ```ignore
//! #[td_test::test(db)]
//! async fn smoke(db: TdPool) { /* ... */ }
//!
//! #[td_test::test(db(crate::fixtures::Foo))]
//! async fn with_fixture(db: TdPool) { /* ... */ }
//!
//! #[td_test::test(db, flavor = "multi_thread")]
//! async fn parallel(db: TdPool) { /* ... */ }
//!
//! #[td_test::test(requires = AwsS3Reqs, env_prefix = "s30")]
//! async fn s3_only(s3: AwsS3Reqs) { /* ... */ }
//! ```

extern crate proc_macro;
mod common;
mod db_arm;
mod requires_arm;
mod scoped_test;

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    scoped_test::scoped_test(args, item)
}
