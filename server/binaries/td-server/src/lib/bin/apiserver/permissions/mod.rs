//
// Copyright 2025 Tabs Data Inc.
//

pub mod create;
pub mod delete;
pub mod list;

use crate::bin::apiserver::PermissionsState;
use crate::routers;

routers! {
    state => { PermissionsState },
    router => {
        create => { state ( PermissionsState ) },
        delete => { state ( PermissionsState ) },
        list => { state ( PermissionsState ) },
    }
}
