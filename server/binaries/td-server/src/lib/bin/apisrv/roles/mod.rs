//
// Copyright 2025 Tabs Data Inc.
//

pub mod create;
pub mod read;

use crate::bin::apisrv::api_server::RolesState;
use crate::routers;
use td_apiforge::api_server_tag;

api_server_tag!(name = "Roles", description = "Roles API");

routers! {
    state => { RolesState },
    router => {
        create => { state ( RolesState ) },
        read => { state ( RolesState ) },
    }
}
