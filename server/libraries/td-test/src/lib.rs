//
// Copyright 2025 Tabs Data Inc.
//

pub use tm_test::*;

pub mod reqs;
pub mod sqlx;

use async_trait::async_trait;

pub enum TestSetupExecution<T> {
    Skip,
    Run(T),
}

#[async_trait]
pub trait TestSetup<T> {
    async fn setup(&self) -> TestSetupExecution<T>;
}
