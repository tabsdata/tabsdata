//
// Copyright 2025. Tabs Data Inc.
//

use crate::router::state::InterCollectionPermissions;
use crate::routers;

pub mod create;
pub mod delete;
pub mod list;

routers! {
    state => { InterCollectionPermissions },
    router => {
        create => { state ( InterCollectionPermissions ) },
        delete => { state ( InterCollectionPermissions ) },
        list => { state ( InterCollectionPermissions ) },
    }
}
