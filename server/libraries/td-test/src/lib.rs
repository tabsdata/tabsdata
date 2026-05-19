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

