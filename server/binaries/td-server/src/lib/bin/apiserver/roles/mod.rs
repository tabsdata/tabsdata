//
// Copyright 2025 Tabs Data Inc.
//

pub mod create;
pub mod delete;
pub mod list;
pub mod read;
pub mod update;

use crate::bin::apiserver::RolesState;
use crate::routers;
use td_apiforge::apiserver_tag;

apiserver_tag!(name = "Roles", description = "Roles API");

routers! {
    state => { RolesState },
    router => {
        create => { state ( RolesState ) },
        read => { state ( RolesState ) },
        update => { state ( RolesState ) },
        delete => { state ( RolesState ) },
        list => { state ( RolesState ) },
    }
}
