//
// Copyright 2025 Tabs Data Inc.
//

use crate::router::state::FunctionRuns;
use crate::routers;

pub mod list;
pub mod read;

routers! {
    state => { FunctionRuns },
    router => {
        list => { state ( FunctionRuns ) },
        read => { state ( FunctionRuns ) },
    }
}
