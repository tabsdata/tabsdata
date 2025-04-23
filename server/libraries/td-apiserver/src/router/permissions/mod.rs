//
// Copyright 2025 Tabs Data Inc.
//

pub mod create;
pub mod delete;
pub mod list;

use crate::router::state::Permissions;
use crate::routers;

routers! {
    state => { Permissions },
    router => {
        create => { state ( Permissions ) },
        delete => { state ( Permissions ) },
        list => { state ( Permissions ) },
    }
}
