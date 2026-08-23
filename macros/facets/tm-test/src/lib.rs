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
//! tdx-provisioned backends are not a flavor of this macro — see
//! [`macro@domain`] / [`macro@system`], which compose with any test attribute.
//!
//! Any attribute key not handled by this macro (everything except `db`,
//! `requires`, `env_prefix`) is forwarded verbatim to the auto-emitted
//! `#[::tokio::test(...)]`, so `flavor`, `worker_threads`, `start_paused`, etc.
//! work without changes here. Call sites never need to write the
//! `#[tokio::test]` attribute themselves.
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
mod backend_attr;
mod common;
mod db_arm;
mod requires_arm;
mod scoped_test;

use proc_macro::TokenStream;

use crate::backend_attr::Role;

#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    scoped_test::scoped_test(args, item)
}

/// Inject a tdx-provisioned **domain**, named by the parameter that receives it.
///
/// The argument is the backend's name in the store; the parameter it fills is
/// that name prefixed with `domain_`, so `"orders"` resolves the domain `orders`
/// into `domain_orders`. This mirrors the Python fixtures, where
/// `@s3_domain("orders")` injects `domain_orders`. The fixture type comes from
/// the parameter's own declared type, so it is never repeated in the attribute.
///
/// Place it **above** the test attribute: attributes expand top-down, and this
/// one must remove the parameter before `#[test]` / `#[tokio::test]` sees a
/// function it would reject for taking arguments. Sync and async tests are
/// treated identically — resolution does no I/O that needs a runtime.
///
/// Stack one per backend, including several of the same type. The test is
/// skipped, or fails, per `TD_TEST_ON_MISSING_DOMAIN` (see the `td-test`
/// `domain` module).
///
/// ```ignore
/// #[td_test::domain("s3_world_xs")]
/// #[td_test::system("s3")]
/// #[tokio::test]
/// async fn round_trip(domain_s3_world_xs: S3Domain, system_s3: S3System) { /* ... */ }
/// ```
#[proc_macro_attribute]
pub fn domain(args: TokenStream, item: TokenStream) -> TokenStream {
    backend_attr::backend_attr(Role::Domain, args, item)
}

/// Inject a tdx-provisioned **system**, named by the parameter that receives it.
///
/// The system sibling of [`macro@domain`]: `"s3"` reads the system `s3` from the
/// store and injects it as the parameter `system_s3`.
#[proc_macro_attribute]
pub fn system(args: TokenStream, item: TokenStream) -> TokenStream {
    backend_attr::backend_attr(Role::System, args, item)
}
