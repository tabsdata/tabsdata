//
// Copyright 2025 Tabs Data Inc.
//

// So absolute paths emitted by the `tm-test` proc macro (`::td_test::...`)
// resolve correctly inside this crate's own self-tests. Without this, internal
// tests would need `use crate as td_test;` workarounds.
extern crate self as td_test;

pub use tm_test::*;

mod backend_cfg;
pub mod db;
pub mod reqs;

// Re-exported so the `#[td_test::test(db)]` macro can emit
// `#[::td_test::file_serial(...)]` without requiring every consumer crate to
// declare `serial_test` as a dev-dependency. The macro is invoked with
// `crate = ::td_test::serial_test` so its expansion resolves runtime symbols
// through this re-export instead of `::serial_test`.
pub use ::serial_test;
pub use serial_test::file_serial;

#[doc(hidden)]
pub mod __private {
    use std::future::Future;

    pub enum TestSetupExecution<T> {
        Skip,
        Run(T),
    }

    pub trait TestSetup<T> {
        fn setup(&self) -> impl Future<Output = TestSetupExecution<T>> + Send;
    }
}
