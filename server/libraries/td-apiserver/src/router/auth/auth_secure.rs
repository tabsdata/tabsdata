//
// Copyright 2025. Tabs Data Inc.
//

use crate::router::auth::{logout, refresh_token, role_change, user_info};
use crate::router::state::Auth;
use crate::routers;

routers! {
    state => { Auth },
    router => {
        refresh_token => { state ( Auth ) },
        user_info => { state ( Auth ) },
        role_change => { state ( Auth ) },
        logout => { state ( Auth ) },
    }
}
