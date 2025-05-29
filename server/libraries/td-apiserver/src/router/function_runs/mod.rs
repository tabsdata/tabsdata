//
// Copyright 2025 Tabs Data Inc.
//

use crate::router::state::FunctionRuns;
use crate::routers;

pub mod read;

routers! {
    state => { FunctionRuns },
    router => {
        read => { state ( FunctionRuns ) },
    }
}
