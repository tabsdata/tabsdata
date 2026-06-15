//
// Copyright 2025 Tabs Data Inc.
//

use std::future::Future;
use td_error::TdError;

pub trait ContextExt<Base, Extended>: Sized {
    fn build(base: &Base, extended: &Extended) -> impl Future<Output = Result<Self, TdError>>;
}
