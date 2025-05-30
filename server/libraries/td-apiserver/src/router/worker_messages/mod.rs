//
// Copyright 2025 Tabs Data Inc.
//

pub mod list;

use crate::router::state::WorkerMessages;
use crate::routers;

routers! {
    state => { WorkerMessages },
    router => {
        list => { state ( WorkerMessages ) },
    }
}
