//
// Copyright 2025 Tabs Data Inc.
//

pub mod create;
pub mod delete;
pub mod list;
pub mod read;

use crate::router::state::UserRoles;
use crate::routers;

routers! {
    state => { UserRoles },
    router => {
        create => { state ( UserRoles ) },
        read => { state ( UserRoles ) },
        delete => { state ( UserRoles ) },
        list => { state ( UserRoles ) },
    }
}
