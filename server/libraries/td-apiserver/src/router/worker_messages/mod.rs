//
// Copyright 2025 Tabs Data Inc.
//

pub mod list;
pub mod logs;

use crate::router::state::WorkerMessages;
use crate::routers;

routers! {
    state => { WorkerMessages },
    router => {
        list => { state ( WorkerMessages ) },
        logs => { state ( WorkerMessages ) },
    }
}
