//
// Copyright 2025 Tabs Data Inc.
//

pub mod list;
pub mod logs;

use crate::router::state::Workers;
use crate::routers;

routers! {
    state => { Workers },
    router => {
        list => { state ( Workers ) },
        logs => { state ( Workers ) },
    }
}
