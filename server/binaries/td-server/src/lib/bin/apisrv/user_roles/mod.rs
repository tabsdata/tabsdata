//
// Copyright 2025 Tabs Data Inc.
//

pub mod create;
pub mod delete;
pub mod list;
pub mod read;

use crate::bin::apisrv::api_server::UserRolesState;
use crate::routers;

routers! {
    state => { UserRolesState },
    router => {
        create => { state ( UserRolesState ) },
        read => { state ( UserRolesState ) },
        delete => { state ( UserRolesState ) },
        list => { state ( UserRolesState ) },
    }
}
