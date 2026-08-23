//
// Copyright 2025 Tabsdata Inc.
//

// So absolute paths emitted by the `tm-test` proc macro (`::td_test::...`)
// resolve correctly inside this crate's own self-tests. Without this, internal
// tests would need `use crate as td_test;` workarounds.
extern crate self as td_test;

pub use tm_test::*;

mod backend_cfg;
pub mod db;
pub mod domain;
pub mod reqs;

#[doc(hidden)]
pub mod __private {
    use std::future::Future;
    use std::pin::Pin;

    /// Per-test teardown the harness runs **after** the test body returns, without
    /// the test fn ever seeing it — e.g. dropping a postgres test's schemas on the
    /// shared server, or closing a sqlite temp-file connection. Async, so it can do
    /// real database work. Setups with nothing to release use [`no_teardown`].
    ///
    /// Best-effort: skipped if the test panics (the next `cargo make test` setup
    /// starts from a clean slate), so it must not be relied on for correctness.
    pub type Teardown = Pin<Box<dyn Future<Output = ()> + Send>>;

    /// Wrap a teardown future. The harness awaits it after the test body.
    pub fn teardown(f: impl Future<Output = ()> + Send + 'static) -> Teardown {
        Box::pin(f)
    }

    /// A teardown that does nothing — for setups with no per-test resources.
    pub fn no_teardown() -> Teardown {
        Box::pin(async {})
    }

    pub enum TestSetupExecution<T> {
        Skip,
        Run(T, Teardown),
    }

    pub trait TestSetup<T> {
        fn setup(&self) -> impl Future<Output = TestSetupExecution<T>> + Send;
    }
}
