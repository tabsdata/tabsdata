//
// Copyright 2025 Tabs Data Inc.
//

pub use tm_test::*;

use std::future::Future;

pub mod reqs;
pub mod sqlx;

#[cfg(feature = "test-utils")]
pub mod file;

pub enum TestSetupExecution<T> {
    Skip,
    Run(T),
}

pub trait TestSetup<T> {
    fn setup(&self) -> impl Future<Output = TestSetupExecution<T>> + Send;
}
