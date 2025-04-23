//
// Copyright 2025 Tabs Data Inc.
//

pub mod create;
pub mod delete;
pub mod list;
pub mod read;
pub mod update;

use crate::router::state::Roles;
use crate::routers;
use td_apiforge::apiserver_tag;

apiserver_tag!(name = "Roles", description = "Roles API");

routers! {
    state => { Roles },
    router => {
        create => { state ( Roles ) },
        read => { state ( Roles ) },
        update => { state ( Roles ) },
        delete => { state ( Roles ) },
        list => { state ( Roles ) },
    }
}
